"""The TUI layout for the Myrient Search App."""
import contextlib
import logging
import queue
import threading
from datetime import UTC, date, datetime
from pathlib import Path
from time import time
from urllib.parse import unquote, urlparse

from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual.errors import TextualError
from textual.reactive import reactive
from textual.widgets import (
    Button,
    Checkbox,
    DataTable,
    Header,
    Input,
    Label,
    Link,
    ProgressBar,
    Select,
)

import crawler

# Local imports
from backend import MyrientBackend
from downloader import Downloader

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



class MyrientTUI(App):
    """Textual User Interface for the Myrient Search App."""

    platforms: reactive[list[str]] = reactive(list)
    regions: reactive[list[str]] = reactive(list)
    languages: reactive[list[str]] = reactive(list)
    versions: reactive[list[str]] = reactive(list)
    size_ranges: reactive[list[str]] = reactive(list)

    CSS = """
    Label { width: 12%; padding-left: 1; }
    Input { width: 25%; }
    Select { width: 12%; align: center middle; content-align: center middle; }
    DataTable { height: 1fr; width: 100%; }
    Button { width: 15%; margin: 0 0 0 1; }
    Placeholder { width: 5% }

    Vertical { height: 80%; align: center top; }
    Horizontal { padding-bottom: 1; height: auto; }

    #labels_row { padding-bottom: 0; }
    #regex_checkbox { width: 12%; }
    #results_per_page_label { width: 13%; padding-top: 1; content-align: right middle; }
    #results_per_page_input { width: 10%; }
    #platform_label { width: 25%; }

    #platform_select { width: 25%; height: 100%; }
    #region_select { height: 100%; }
    #language_select { height: 100%; }
    #version_select { height: 100%; }
    #size_select { height: 100%; }

    #title_label { width: 25%; }
    #status_label { width: 100%; padding-left: 1; }
    #last_sync_label { width: 100%; padding-left: 1; padding-bottom: 1; }
    #download_link { width: 100%; }

    #progress_container { height: 20%; }
    .progress_bar { width: 30%; }
    .progress_label { width: 70%; }
    """

    def __init__(
            self,
            base_url: str,
            db_file: str|Path,
            download_dir: str|Path,
            ) -> None:
        """Initialize the TUI with the given backend."""
        super().__init__()

        self.base_url = base_url
        self.db_file = db_file
        self.dbfile_time = None

        self.download_dir = download_dir

        self.backend = None
        self.downloader = None

        self.search_queue: queue.Queue[str] = queue.Queue()
        self.result_queue: queue.Queue[str] = queue.Queue()

        self.columns = ("title", "platform", "region", "language", "version", "size")

        self.result_urls: dict[str, str] = {}
        self.sort_column: str = "title"
        self.sort_reverse: bool = False


    def compose(self) -> ComposeResult:
        """Compose the TUI layout."""
        if not self.is_web:
            download_link = Link("", id="download_link")
        else:
            download_link = Label("", id="download_link")

        yield Vertical(
            Header("Myrient Search App", id="header"),
            Horizontal(
                Label("Title", id="title_label"),
                Label("Platform", id="platform_label"),
                Label("Region"),
                Label("Language"),
                Label("Version"),
                Label("Size"),
                id="labels_row",
            ),

            Horizontal(
                Input(placeholder="Enter search query...", id="search_input"),
                Select.from_values([], id="platform_select", prompt="Platform"),
                Select.from_values([], id="region_select", prompt="Region"),
                Select.from_values([], id="language_select", prompt="Language"),
                Select.from_values([], id="version_select", prompt="Version"),
                Select.from_values([], id="size_select", prompt="Size Range"),
                ),

            Horizontal(
                Checkbox("Regex", id="regex_checkbox"),
                Label("Results per page:", id="results_per_page_label"),
                Input(value="100", id="results_per_page_input"),
                Button("Load more", id="load_more_button", variant="primary"),
            ),

            Horizontal(
                Button("Search", id="search_button", variant="success"),
                Button("Reset Filters", id="reset_button", variant="warning"),
                Button("Update Database", id="update_button", variant="warning"),
                Button("Download Selected", id="download_button", variant="primary"),
                Button("Stop downloads", id="stop_button", variant="error"),
            ),
            Label("Database last sync date: N/A", id="last_sync_label"),
            DataTable(cursor_type="row"),
            Label("Status", id="status_label"),
            download_link,

        )

    def on_mount(self) -> None:
        """Set up the results table on mount."""
        self.progress_queue: queue.Queue = queue.Queue()
        self.progress_slots: list = []

        self.progress_container = Vertical(id="progress_container")
        self.mount(self.progress_container)

        self.set_interval(0.5, self.update_progress_from_queue)

        results_table = self.query_one(DataTable)
        results_table.add_column("Title", key="title", width=50)
        results_table.add_column("Platform", key="platform", width=40)
        results_table.add_column("Region", key="region", width=10)
        results_table.add_column("Language", key="language", width=10)
        results_table.add_column("Version", key="version", width=10)
        results_table.add_column("Size", key="size", width=10)
        results_table.add_rows([])
        if self.check_if_db_exists():
            self.do_search()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        """Handle input submission events."""
        if event.input.id == "search_input":
            results_table = self.query_one(DataTable)
            results_table.clear()
            self.do_search()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        """Handle button press events."""
        if event.button.id == "search_button":
            self.do_search()
        elif event.button.id == "load_more_button":
            results_table = self.query_one(DataTable)
            results_table.clear()
            self.load_more_results()
        elif event.button.id == "download_button":
            self.start_download()
        elif event.button.id == "stop_button":
            self.stop_downloads()
        elif event.button.id == "update_button":
            self.update_db(progress_callback=self.db_progress_handler)
        elif event.button.id == "reset_button":
            self.query_one("#search_input", Input).value = ""
            self.query_one("#results_per_page_input", Input).value = "100"
            self.query_one("#platform_select", Select).value = "all"
            self.query_one("#region_select", Select).value = "all"
            self.query_one("#language_select", Select).value = "all"
            self.query_one("#version_select", Select).value = "all"
            self.query_one("#size_select", Select).value = "all"
            self.do_search()




    def on_data_table_header_selected(
        self, event: DataTable.HeaderSelected,
    ) -> None:
        """Handle header click events for sorting."""
        column_key = str(event.label).strip()
        if column_key is None:
            return
        self.action_sort_results(column_key.lower())
        status_label = self.query_one("#status_label", Label)
        status_label.update(
            f"Sorting by {column_key} {'↓' if self.sort_reverse else '↑'}",
            )

    def on_data_table_row_selected(
        self, event: DataTable.RowSelected,
    ) -> None:
        """Handle row selection events."""
        table = self.query_one(DataTable)
        sel = event.cursor_row
        name = table.get_cell_at((sel,0))
        platform = table.get_cell_at((sel,1))
        size = table.get_cell_at((sel,5))
        url = self.result_urls[str(sel)]

        status_label = self.query_one("#status_label", Label)
        status_label.update(f'Selected: "{name}" ({platform}) - {size}')

        if not self.is_web:
            download_link = self.query_one("#download_link", Link)
            download_link.text = f"URL: {unquote(url)}"
            download_link.url = url
        else:
            download_link = self.query_one("#download_link", Label)
            download_link.update(f"URL: {unquote(url)}")

    # Database
    def check_if_db_exists(self) -> None:
        """Check if the database file is found and update widgets."""
        sync_label = self.query_one("#last_sync_label", Label)

        if not self.db_file.exists():
            self.notify("Database file not found.")
            sync_label.update("Database last sync date: N/A")
            return False

        self.backend = MyrientBackend(self.db_file)
        self.platforms = self.backend.list_platforms()
        self.regions = self.backend.list_regions()
        self.languages = self.backend.list_languages()
        self.versions = self.backend.list_versions()
        self.size_ranges = self.backend.list_size_ranges()

        self.dbfile_time = date.fromtimestamp(Path.stat(self.db_file).st_mtime)  # noqa: DTZ012
        sync_label.update(f"Database last sync date: {self.dbfile_time}")
        return True


    def db_progress_handler(self, msg: str) -> None:
        """Handle database progress messages."""
        status_label = self.query_one("#status_label", Label)
        self.call_from_thread(status_label.update, msg)


    def update_db(self, *,
        repair: bool = False,
        progress_callback: None = None,
    ) -> None:
        """Update the database with new items from the Myrient website.

        Args:
            repair (bool, optional): Whether to attempt a database repair
                before updating. Defaults to False.
            progress_callback (Callable[[str], None], optional): Callback
                function that receives progress messages. Defaults to None.

        """
        status_label = self.query_one("#status_label", Label)

        if not self.db_file.exists():
            self.notify("Database file not found.")
            self.dbfile_time = None

        if self.dbfile_time and datetime.now(tz=UTC).date() == self.dbfile_time:
                self.notify("Database has already been updated today.")
                return

        if self.backend is None:
            self.backend = MyrientBackend(self.db_file)

        def threaded_update() -> None:
            if not repair:
                self.call_from_thread(status_label.update, "Updating database...")

                crawler.crawl_and_index(
                    base_url=self.base_url,
                    db_path=self.db_file,
                    progress_callback=progress_callback,
                    )

                self.call_from_thread(status_label.update, "Database update complete")
            else:
                self.call_from_thread(status_label.update, "Repairing database...")

                crawler.rescan_database(
                    base_url=self.base_url,
                    db_path=self.db_file,
                    progress_callback=progress_callback,
                    )

                self.call_from_thread(status_label.update, "Database repair complete")

        threading.Thread(target=threaded_update, daemon=True).start()


    def repair_db(self) -> None:
        """Repair the database based on current crawler rules."""
        self.progress_update_delay = 10
        def progress_update(current:int, total:int) -> None:
            if current not in (
                "Rescan and update complete.",
                "Ignored platform deletion complete.",
                ):
                percent = round(int(current) / int(total) * 100, 2)
                self.progress_queue.put(
                    (0, f"Database repair in progress. "
                     f"{current} of {total} entries processed.",
                     current >= total, percent),
                     )
            else:
                self.progress_queue.put((0, current, True, ""))

        self.update_db(repair=True, progress_callback=progress_update)



    # Reactive watchers
    def watch_platforms(self, _old: list[str], new: list[str]) -> None:
        """Update platform select options when platforms change."""
        platform_select = self.query_one("#platform_select", Select)
        current_selection = platform_select.value
        platforms = (["all", *new] if new[0] != "all" else new) if new else ["all"]
        platform_select.set_options(
            [plat, plat] for plat in platforms
            )
        if current_selection != "all" and current_selection not in platforms:
            platform_select.value = "all"
        else:
            platform_select.value = current_selection


    def watch_regions(self, _old: list[str], new: list[str]) -> None:
        """Update region select options when regions change."""
        region_select = self.query_one("#region_select", Select)
        current_selection = region_select.value
        regions = (["all", *new] if new[0] != "all" else new) if new else ["all"]
        region_select.set_options(
            [reg, reg] for reg in regions
            )
        if current_selection != "all" and current_selection not in regions:
            region_select.value = "all"
        else:
            region_select.value = current_selection

    def watch_languages(self, _old: list[str], new: list[str]) -> None:
        """Update language select options when languages change."""
        languages_select = self.query_one("#language_select", Select)
        current_selection = languages_select.value
        languages = (["all", *new] if new[0] != "all" else new) if new else ["all"]
        languages_select.set_options(
            [lang, lang] for lang in languages
            )
        if current_selection != "all" and current_selection not in languages:
            languages_select.value = "all"
        else:
            languages_select.value = current_selection

    def watch_versions(self, _old: list[str], new: list[str]) -> None:
        """Update version select options when versions change."""
        version_select = self.query_one("#version_select", Select)
        current_selection = version_select.value
        versions = (["all", *new] if new[0] != "all" else new) if new else ["all"]
        version_select.set_options(
            [ver, ver] for ver in versions
            )
        if current_selection != "all" and current_selection not in versions:
            version_select.value = "all"
        else:
            version_select.value = current_selection

    def watch_size_ranges(self, _old: list[str], new: list[str]) -> None:
        """Update size select options when size ranges change."""
        sizes_select = self.query_one("#size_select", Select)
        current_selection = sizes_select.value
        sizes = (["all", *new] if new[0] != "all" else new) if new else ["all"]
        sizes_select.set_options(
            [size, size] for size in sizes
            )
        if current_selection != "all" and current_selection not in sizes:
            sizes_select.value = "all"
        else:
            sizes_select.value = current_selection



    def action_sort_results(self, column: str | None = None) -> None:
        """Sort the results by the selected column."""
        if column is None:
            return
        if self.sort_column == column:
            self.sort_reverse = not self.sort_reverse
        self.sort_column = column
        self.do_search()



    # Search functions
    def do_search(self, offset:int=0) -> None:
        """Search for items in the database matching the query."""
        self.current_offset = offset
        results_table = self.query_one(DataTable)
        results_table.clear()
        results_table.add_row("Searching...", "-", "-", "-", "-", "-", key="searching")

        try:
            result_input = self.query_one("#results_per_page_input", Input)
            limit = min(int(result_input.value.strip()), 5000)
            result_input.value = str(limit)
        except ValueError:
            limit = 100

        def search_thread() -> None:
            """Send the search query to the database backend."""
            try:
                # Normalize Select values (Textual may return a NoSelection object)
                def _get_select_value(sel_id: str) -> str | None:
                    val = self.query_one(sel_id, Select).value
                    if not isinstance(val, str):
                        return None
                    return None if val == "all" else val

                platform = _get_select_value("#platform_select")
                region = _get_select_value("#region_select")
                language = _get_select_value("#language_select")
                version = _get_select_value("#version_select")
                title_contains = self.query_one("#search_input", Input).value.strip()
                title_regex = self.query_one("#regex_checkbox", Checkbox).value
                size_range = _get_select_value("#size_select")

                search = {
                    "platform": platform,
                    "region": region,
                    "language": language,
                    "version": version,
                    "size_range": size_range,
                    "title_contains": title_contains,
                    "title_regex": title_regex,
                    }

                results, platforms, regions, languages, versions, size_ranges = (
                    self.backend.advanced_search(
                        search=search,
                        offset=self.current_offset,
                        limit=limit,
                        sort_by=self.sort_column,
                        sort_order="DESC" if self.sort_reverse else "ASC",
                    )
                )
                # update filter options based on search results
                self.platforms = ["all", *platforms]
                self.regions = ["all", *regions]
                self.languages = ["all", *languages]
                self.versions = ["all", *versions]
                self.size_ranges = ["all", *size_ranges]
            except TextualError as e:
                self.call_from_thread(self._display_error, str(e))
                return

            # send results to main thread for display
            self.call_from_thread(self._display_results, results)

        threading.Thread(target=search_thread, daemon=True).start()


    def load_more_results(self) -> None:
        """Load more results from the database."""
        offset = self.query_one("#results_per_page_input", Input).value.strip()
        if not offset:
            return
        self.current_offset += int(offset)
        self.do_search(offset=self.current_offset)


    def _display_error(self, msg: str) -> None:
        """Show an error row and remove searching placeholder (runs on main thread)."""
        results_table = self.query_one(DataTable)
        results_table.clear()

        results_table.add_row(f"Error: {msg}", "", "", "", "", "", key="error")
        logger.exception("Search failed: %s", msg)

    def _display_results(self, results: list) -> None:
        """Run on main thread: remove searching row and insert result rows."""
        results_table = self.query_one(DataTable)
        results_table.clear()

        def _cell(v: dict) -> str:
            if v is None:
                return "-"
            if isinstance(v, (list, tuple)):
                return ", ".join(str(x) for x in v) if v else "-"
            return str(v)



        # append rows with unique keys (use offset to avoid collisions)
        for idx, r in enumerate(results):
            values = (
                _cell(r["title"]),
                _cell(r["platform"]),
                _cell(r["region"]),
                _cell(r["language"]),
                _cell(r["version"]),
                _cell(r["size"]),
            )
            row_key = f"{idx}"
            results_table.add_row(*values, key=row_key)
            self.result_urls[row_key] = (
                r.get("url") if isinstance(r, dict) else r["url"]
                )



    # Download functions
    def get_selected_url(self) -> str | None:
        """Return URL for the currently selected DataTable row or None."""
        table = self.query_one(DataTable)
        sel = table.cursor_coordinate.row
        name = table.get_cell_at((sel,0))
        return self.result_urls.get(str(sel)), name


    def start_download(self) -> None:
        """Start downloading the selected files."""
        dw_url, _n = self.get_selected_url()
        if self.is_web:
            self.notify("Downloads currently not supported in web")
            return

        download_dir = self.download_dir or "downloads"
        if not self.downloader:
            self.downloader = Downloader(output_dir=download_dir)

        self.downloader.add_url(dw_url)
        queue_size = self.downloader.download_queue.qsize()

        status_label = self.query_one("#status_label", Label)

        if queue_size == 0:
            status_label.update("Select one or more items to download")
            return

        status_label.update(f"Downloading {queue_size} items to {download_dir}")

        # If the downloader is already running, just add the URL.
        # The existing thread will pick it up. Don't start a new thread.
        if self.downloader.download_running:
            return

        # This should only run once when the first download is initiated
        self.ensure_progress_slots(self.downloader.max_file_workers)
        self.download_start_time = time()

        def progress_callback(idx:int, url:str, completed:int, total:int) -> None:
            if self.downloader.cancel_flag.is_set():
                return
            parsed = urlparse(url)
            name = Path(unquote(parsed.path)).name
            pct = int(completed / total * 100) if total else 0
            size = f"{int((total or 0)/(1024*1024))}MB"
            text = f"{size:6} {name}"
            self.call_from_thread(
                self.progress_queue.put, (str(idx), text, completed >= total, pct),
                )

        def done_callback(completed:int, total:int) -> None:
            duration = time() - self.download_start_time
            self.call_from_thread(
                self.progress_queue.put, ("done", completed, total, duration),
            )

        def monitor_queue() -> None:
            """Wait for all items to be processed and then calls done_callback."""
            self.downloader.download_queue.join()
            if not self.downloader.cancel_flag.is_set():
                # This callback is now managed by the TUI, not the downloader
                done_callback(queue_size, queue_size)

        # Run downloader in a thread to avoid blocking GUI
        threading.Thread(
            target=self.downloader.start, kwargs={
                "progress_callback": progress_callback,
                }, daemon=True).start()

        threading.Thread(target=monitor_queue, daemon=True).start()

    def stop_downloads(self) -> None:
        """Stop all downloads."""
        if self.downloader:
            self.downloader.cancel_all()
            self.reset_progress_slots()



    # Progress update functions
    def ensure_progress_slots(self, count: int) -> None:
        """Ensure we have at least `count` slot widgets available."""
        while len(self.progress_slots) < count:
            # textual widgets: ProgressBar + Label (Static or Label)
            pbar = ProgressBar(total=100, id=f"pbar_{len(self.progress_slots)}",
                               classes="progress_bar")
            label = Label("", id=f"plabel_{len(self.progress_slots)}",
                          classes="progress_label")
            # layout one after another (you can customize)
            self.progress_container.mount(Horizontal(pbar, label))
            self.progress_slots.append([pbar, label, False, None])


    def reset_progress_slots(self) -> None:
        """Mark all slots unused and clear text."""
        for slot in self.progress_slots:
            slot[0].update(progress=0)  # pbar
            slot[1].update("")          # label
            slot[2] = False             # in_use
            slot[3] = None              # file_idx


    def get_or_assign_slot(self, file_idx: str) -> int:
        """Return an existing slot for file_idx or assign a free one."""
        # try find existing
        for slot in self.progress_slots:
            if slot[3] == file_idx:
                return slot
        # find free
        for slot in self.progress_slots:
            if not slot[2] and slot[3] is None:
                slot[2] = True
                slot[3] = file_idx
                return slot
        # fallback: ensure at least one slot exists
        if not self.progress_slots:
            self.ensure_progress_slots(1)
        return self.progress_slots[0]

    # --- queue poll / UI updater ----------------------------------------------
    def update_progress_from_queue(self) -> None:
        """Run on main thread: pull messages from queue and update slot widgets."""
        try:
            while True:
                msg = self.progress_queue.get_nowait()
                if not msg:
                    continue
                if msg[0] == "done":
                    _, completed, total, duration = msg
                    # optional: clear slots or show final message
                    for slot in self.progress_slots:
                        slot[0].update(progress=100)
                    # show summary in status area (adapt to your TUI)
                    self.query_one("#status_label", Label).update(
                        f"Downloaded {completed}/{total} in {duration:.1f}s",
                        )
                    # mark slots unused
                    for slot in self.progress_slots:
                        slot[2] = False
                        slot[3] = None
                else:
                    # expected (file_idx, text, finished:bool, percent:int)
                    file_idx, text, finished, percent = msg
                    slot = self.get_or_assign_slot(str(file_idx))
                    pbar, label = slot[0], slot[1]
                    label.update(text)
                    pbar.update(progress=percent)
                    if finished:
                        # free slot when done (optionally remove widget)
                        slot[2] = False
                        slot[3] = None
        except queue.Empty:
            pass






