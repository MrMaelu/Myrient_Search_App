import os
import queue
import threading
import time
from time import time
import tkinter.ttk as ttk
import customtkinter as ctk
import urllib.parse
from urllib.parse import unquote
import typing
from typing import List, Callable

# Local imports
import crawler
import backend
from backend import MyrientBackend
import downloader
from downloader import Downloader


class ScrollableOptionMenu(ctk.CTkFrame):
    def __init__(self, master, values: List[str], command: Callable = None, **kwargs):
        super().__init__(master, **kwargs)
        self.values = values
        self.command = command
        self.current_value = ctk.StringVar(value=values[0] if values else "")
        self._state = "normal"  # Track current state

        self.main_button = ctk.CTkButton(
            self, 
            text=self.current_value.get(),
            command=self.toggle_dropdown,
            anchor="w",
        )
        self.main_button.pack(fill="x", expand=True)

        self.dropdown_frame = None
        self.visible = False

    def configure(self, **kwargs):
        # Allow state to be set like normal widgets
        if "state" in kwargs:
            self.set_state(kwargs.pop("state"))
        super().configure(**kwargs)

    def set_state(self, state: str):
        """Enable or disable the option menu."""
        state = state.lower()
        if state not in ("normal", "disabled"):
            raise ValueError("state must be 'normal' or 'disabled'")
        self._state = state
        self.main_button.configure(state=state)
        # If disabling while open, close dropdown
        if state == "disabled" and self.visible:
            self.hide_dropdown()

    def toggle_dropdown(self):
        if self._state == "disabled":
            return
        if self.visible:
            self.hide_dropdown()
        else:
            self.show_dropdown()

    def show_dropdown(self):
        if self._state == "disabled":
            return
        if self.dropdown_frame is None:
            self.dropdown_frame = ctk.CTkToplevel(self)
            self.dropdown_frame.withdraw()
            self.dropdown_frame.overrideredirect(True)

            scroll_frame = ctk.CTkScrollableFrame(self.dropdown_frame, height=300)
            scroll_frame.pack(fill="both", expand=True)

            for value in self.values:
                btn = ctk.CTkButton(
                    scroll_frame,
                    text=value,
                    anchor="w",
                    height=15,
                    command=lambda v=value: self.select_option(v)
                )
                btn.pack(fill="x", pady=1)

        x = self.main_button.winfo_rootx()
        y = self.main_button.winfo_rooty() + self.main_button.winfo_height()
        self.dropdown_frame.geometry(f"{self.main_button.winfo_width()}x300+{x}+{y}")

        self.dropdown_frame.deiconify()
        self.visible = True
        self.dropdown_frame.bind("<FocusOut>", lambda e: self.hide_dropdown())
        self.dropdown_frame.focus_set()

    def hide_dropdown(self):
        if self.dropdown_frame:
            self.dropdown_frame.withdraw()
        self.visible = False

    def select_option(self, value):
        if self._state == "disabled":
            return
        self.current_value.set(value)
        self.main_button.configure(text=value)
        self.hide_dropdown()
        if self.command:
            self.command(value)

    def get(self):
        return self.current_value.get()

    def update_values(self, values: List[str], current_value="all"):
        self.values = ["all"] + sorted(values)
        self.current_value.set(current_value if current_value else values[0] or "")
        self.main_button.configure(text=self.current_value.get())

        # Rebuild dropdown buttons if dropdown_frame already exists
        if self.dropdown_frame:
            for widget in self.dropdown_frame.winfo_children():
                widget.destroy()  # remove old scrollable frame

            scroll_frame = ctk.CTkScrollableFrame(self.dropdown_frame, height=300)
            scroll_frame.pack(fill="both", expand=True)

            for value in self.values:
                btn = ctk.CTkButton(
                    scroll_frame,
                    text=value,
                    anchor="w",
                    height=15,
                    command=lambda v=value: self.select_option(v)
                )
                btn.pack(fill="x", pady=1)



class MyrientApp(ctk.CTk):
    def __init__(self, BASE_URL, DB_FILE, DOWNLOAD_DIR):
        super().__init__()
        self.style = ttk.Style(self)
        self.style.theme_use('clam')  # 'clam' supports more styling options

        # Treeview row colors
        self.style.configure(
            "Treeview",
            background="#2b2b2b",      # normal row background
            foreground="white",        # normal row text
            fieldbackground="#2b2b2b", # widget background
            highlightthickness=0,
            bordercolor="#2b2b2b",
            borderwidth=0
        )

        # Row state colors
        self.style.map(
            "Treeview",
            background=[
                ('selected', '#3874f2'),   # selected row
                ('active', '#333333')      # hovered row
            ],
            foreground=[
                ('selected', 'white'),
                ('active', 'white')
            ]
        )

        # Treeview heading base style
        self.style.configure(
            "Treeview.Heading",
            background="#3c3f41",     # normal heading
            foreground="white",       # heading text
            relief="flat"
        )

        # Heading hover & click colors
        self.style.map(
            "Treeview.Heading",
            background=[
                ('active', '#505354'),  # hover over heading
                ('pressed', '#606364')  # heading clicked
            ]
        )


        self.BASE_URL = BASE_URL
        self.DB_FILE = DB_FILE

        self.progress_queue = queue.Queue()
        self.progress_update_delay = 100
        self.after(self.progress_update_delay, self.update_progress_labels_from_queue)

        # Add offset tracking
        self.current_offset = 0

        self.title("Myrient Search and Download")

        app_size_x = 1024
        app_size_y = 768
        app_pos_x = (self.winfo_screenwidth() / 2) - (app_size_x / 2)
        app_pos_y = (self.winfo_screenheight() / 2) - (app_size_y / 2)
        self.geometry(f"{int(app_size_x)}x{int(app_size_y)}+{int(app_pos_x)}+{int(app_pos_y)}")

        self.backend = None
        self.downloader = None
        
        self.protocol("WM_DELETE_WINDOW", self.close_window)

        self.padx = 5
        self.pady = 5

        # Widgets
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")

        self.platform_var = ctk.StringVar(value="all")
        self.title_var = ctk.StringVar(value="")
        self.region_var = ctk.StringVar(value="all")
        self.language_var = ctk.StringVar(value="all")
        self.version_var = ctk.StringVar(value="all")
        self.regex_search_var = ctk.BooleanVar(value=False)
        self.number_of_results_var = ctk.StringVar(value="100")
        self.download_dir = ctk.StringVar(value=DOWNLOAD_DIR)


# Search controls frame
# ******************************************************************* #
        self.search_frame = ctk.CTkFrame(self)
        self.search_frame.pack(fill="both")

# ******** Row 0/1 ********
        row = 0

# Column 0-1: Title
        title_label = ctk.CTkLabel(self.search_frame, text="Title")
        title_label.grid(row=0, column=0, sticky="w", padx=self.padx, pady=0, columnspan=2)

        self.title_entry = ctk.CTkEntry(self.search_frame, placeholder_text="Title contains", textvariable=self.title_var)
        self.title_entry.grid(row=1, column=0, sticky="ew", padx=self.padx, pady=self.pady, columnspan=2)
        self.title_entry.bind("<Return>", lambda event: self.do_search())

# Column 2-3: Platform
        platform_label = ctk.CTkLabel(self.search_frame, text="Platform")
        platform_label.grid(row=row, column=2, sticky="w", padx=self.padx, pady=0, columnspan=2)

        self.platform_menu = ScrollableOptionMenu(self.search_frame, values=["all"], command=self.on_platform_select)
        self.platform_menu.grid(row=row+1, column=2, sticky="ew", padx=self.padx, pady=self.pady, columnspan=2)

# Column 4: Region
        region_label = ctk.CTkLabel(self.search_frame, text="Region")
        region_label.grid(row=row, column=4, sticky="w", padx=self.padx, pady=0)

        self.region_menu = ScrollableOptionMenu(self.search_frame, values=["all"], command=self.on_region_select)
        self.region_menu.grid(row=row+1, column=4, sticky="ew", padx=self.padx, pady=self.pady)

# Column 5: Language
        language_label = ctk.CTkLabel(self.search_frame, text="Language")
        language_label.grid(row=row, column=5, sticky="w", padx=self.padx, pady=0)

        self.language_menu = ScrollableOptionMenu(self.search_frame, values=["all"], command=self.on_language_select)
        self.language_menu.grid(row=row+1, column=5, sticky="ew", padx=self.padx, pady=self.pady)

# Column 6: Version
        version_label = ctk.CTkLabel(self.search_frame, text="Version")
        version_label.grid(row=row, column=6, sticky="w", padx=self.padx, pady=0)

        self.version_menu = ScrollableOptionMenu(self.search_frame, values=["all"], command=self.on_version_select)
        self.version_menu.grid(row=row+1, column=6, sticky="ew", padx=self.padx, pady=self.pady)

# ******** Row 2 ********
        row = 2

# Column 0: Regex search
        self.regex_search_checkbox = ctk.CTkCheckBox(self.search_frame, text="Regex search", variable=self.regex_search_var)
        self.regex_search_checkbox.grid(row=row, column=0, sticky="ew", padx=self.padx, pady=self.pady)

# Column 1: Reset filters
        self.reset_button = ctk.CTkButton(self.search_frame, text="Reset filters", command=self.reset_filters)
        self.reset_button.grid(row=row, column=1, sticky="ew", padx=self.padx, pady=self.pady)

# ******** Row 3 ********
        row = 3

# Column 0: Reset filters Search
        self.search_button = ctk.CTkButton(self.search_frame, text="Search", command=self.do_search)
        self.search_button.grid(row=row, column=0, sticky="ew", padx=self.padx, pady=self.pady)

# Column 1: Load more
        self.load_more_button = ctk.CTkButton(self.search_frame, text="Load More", command=self.load_more_results, state="disabled")
        self.load_more_button.grid(row=row, column=1, sticky="ew", padx=self.padx, pady=self.pady)

# Column 2: Results per page
        self.number_of_results_label = ctk.CTkLabel(self.search_frame, text="Results per page:")
        self.number_of_results_label.grid(row=2, column=2, sticky="w", padx=self.padx, pady=self.pady)
        
        self.number_of_results_input = ctk.CTkEntry(self.search_frame, placeholder_text="100", textvariable=self.number_of_results_var)
        self.number_of_results_input.grid(row=row, column=2, sticky="w", padx=self.padx, pady=self.pady)


# Configure columns
        self.search_frame.grid_columnconfigure(0, weight=1, minsize=120) # Title / regex / search
        self.search_frame.grid_columnconfigure(1, weight=1, minsize=120) # Title / regex / reset / load more

        self.search_frame.grid_columnconfigure(2, weight=1, minsize=140) # Platform / results label
        self.search_frame.grid_columnconfigure(3, weight=1, minsize=140) # Platform / results input

        self.search_frame.grid_columnconfigure(4, weight=1, minsize=100) # Region
        self.search_frame.grid_columnconfigure(5, weight=1, minsize=100) # Language
        self.search_frame.grid_columnconfigure(6, weight=1, minsize=100) # Version

# ******************************************************************* #

# Results frame with treeview
        self.results_frame = ctk.CTkFrame(self)
        self.results_frame.pack(fill="both", expand=True)

        # Use lowercase identifiers
        columns = ("title", "platform", "region", "language", "version", "size")
        self.results_tree = ttk.Treeview(
            self.results_frame, columns=columns, show="headings", selectmode="extended"
        )
        self.results_tree.bind("<<TreeviewSelect>>", self.show_url)

        # Set headings and column properties
        for col in columns:
            self.results_tree.heading(col, text=col.title())  # Capitalized display
            if col == "title":
                self.results_tree.column(col, width=250, anchor="w", stretch=True)
            elif col == "platform":
                self.results_tree.column(col, width=220, anchor="w", stretch=True)
            elif col in ("region"):
                self.results_tree.column(col, width=70, anchor="center", stretch=False)
            elif col in ("language"):
                self.results_tree.column(col, width=70, anchor="center", stretch=True)
            elif col == "version":
                self.results_tree.column(col, width=70, anchor="center", stretch=False)
            elif col == "size":
                self.results_tree.column(col, width=70, anchor="e", stretch=False)

        # Vertical scrollbar
        vsb = ttk.Scrollbar(self.results_frame, orient="vertical", command=self.results_tree.yview)
        self.results_tree.configure(yscrollcommand=vsb.set)

        # Pack Treeview and scrollbar
        self.results_tree.pack(side="left", fill="both", expand=True, padx=self.padx, pady=self.pady)
        vsb.pack(side="right", fill="y", padx=self.padx, pady=self.pady)

        # Bind sorting to headings
        for col in columns:
            self.results_tree.heading(col, text=col.title(), command=lambda c=col: self.sort_treeview_column(c, False))

# ******************************************************************* #



# Download controls frame
# ******************************************************************* #
        self.download_frame = ctk.CTkFrame(self)
        self.download_frame.pack(fill="both")

# ******** Row 0 ********
        row = 0

# Column 0: Download folder label
        self.download_dir_label = ctk.CTkLabel(self.download_frame, text="Download folder:")
        self.download_dir_label.grid(row=row, column=0, sticky="w", padx=self.padx, pady=0)

# Column 1-2: Download folder entry
        self.download_dir_input = ctk.CTkEntry(self.download_frame, placeholder_text="downloads", textvariable=self.download_dir)
        self.download_dir_input.grid(row=row, column=1, sticky="ew", padx=self.padx, pady=self.pady, columnspan=2)

# Column 3: Select folder button
        self.select_folder_button = ctk.CTkButton(self.download_frame, text="Select folder", command=self.select_folder)
        self.select_folder_button.grid(row=row, column=3, sticky="ew", padx=self.padx, pady=self.pady)

# Column 5: Update database button
        self.update_button = ctk.CTkButton(self.download_frame, text="Update DB", command=self.update_db)
        self.update_button.grid(row=row, column=5, sticky="ew", padx=self.padx, pady=self.pady)

# ******** Row 1 ********
        row = 1

# Column 0-2: Download Selected button
        self.download_button = ctk.CTkButton(self.download_frame, text="Download Selected", command=self.start_download)
        self.download_button.grid(row=row, column=0, sticky="ew", padx=self.padx, pady=self.pady, columnspan=3)

# Column 3: Cancel Download button
        self.cancel_button = ctk.CTkButton(self.download_frame, text="Cancel Download", command=self.stop_download)
        self.cancel_button.grid(row=row, column=3, sticky="ew", padx=self.padx, pady=self.pady)
        self.cancel_button.configure(state="disabled")

# Column 5: Repair database button
        self.repair_db_button = ctk.CTkButton(self.download_frame, text="Repair DB", command=self.repair_db)
        self.repair_db_button.grid(row=row, column=5, sticky="ew", padx=self.padx, pady=self.pady)

# Configure columns
        for i in range(6):
            self.download_frame.grid_columnconfigure(i, weight=1, minsize=120)

# ******************************************************************* #

        self.status_label = ctk.CTkLabel(self, text="")
        self.status_label.pack(padx=self.padx, pady=0)

        # Create a frame for progress labels
        self.progress_frame = ctk.CTkFrame(self, height=0)
        self.progress_frame.pack(fill="both")

        # Initialize empty list for progress labels
        self.progress_labels = []

        # Store URLs parallel to displayed rows
        self.result_urls = {}

        self.check_if_db_exists()



# Setup

    def sort_treeview_column(self, col, reverse):
        def size_to_bytes(size_str):
            try:
                num, unit = size_str.split()
                num = float(num)
                unit = unit.upper()
                if unit.startswith("GIB"):
                    return num * 1024
                elif unit.startswith("MIB"):
                    return num
                elif unit.startswith("KIB"):
                    return num / 1024
                return num
            except Exception:
                return 0

        # Build list of (value, item) for sorting
        data = [(self.results_tree.set(item, col), item) for item in self.results_tree.get_children("")]

        # Pick sorting key
        if col == "size":
            data.sort(key=lambda t: size_to_bytes(t[0]), reverse=reverse)
        elif col == "version":
            try:
                data.sort(key=lambda t: [int(x) for x in t[0].split('.')], reverse=reverse)
            except Exception:
                data.sort(reverse=reverse)
        else:
            data.sort(key=lambda t: t[0].lower(), reverse=reverse)

        # Reorder rows
        for index, (val, item) in enumerate(data):
            self.results_tree.move(item, "", index)

        # Toggle next sort
        self.results_tree.heading(col, command=lambda: self.sort_treeview_column(col, not reverse))

    def disable_all_except_one(self, button_to_keep):
        # Disable widgets in all frames
        for frame in (self.results_frame, self.download_frame, self.search_frame):
            for child in frame.winfo_children():
                if child != button_to_keep:
                    try:
                        child.configure(state="disabled")
                    except Exception:
                        pass

        # Grey-out Treeview
        self.style.configure("Disabled.Treeview",
            background="#444444",
            fieldbackground="#444444",
            foreground="#888888"
        )
        self.results_tree.configure(style="Disabled.Treeview")

    def enable_all_widgets(self):
        # Enable everything
        for frame in (self.search_frame, self.results_frame, self.download_frame):
            for child in frame.winfo_children():
                if child != self.cancel_button and child != self.load_more_button:
                    try:
                        child.configure(state="normal")
                    except Exception:
                        pass

        # Restore normal Treeview style
        self.results_tree.configure(style="Treeview")

    def load_platforms(self):
        try:
        # Load platforms
            self.platform_var.set(value="all")
            platforms = self.backend.list_platforms()
            self.platform_menu.update_values(platforms, "all")

        # Load regions
            self.region_var.set(value="all")
            regions = self.backend.list_regions()
            self.region_menu.update_values(regions, "all")

        # Load languages (flatten multi-language entries to single language codes)
            self.language_var.set(value="all")
            lang_rows = self.backend.list_languages()
            single_languages = sorted({lang.strip() for entry in lang_rows for lang in entry.split(',')})
            self.language_menu.update_values(single_languages, "all")

        # Load versions
            self.version_var.set(value="all")
            versions = self.backend.list_versions()
            self.version_menu.update_values(versions, "all")

        except Exception as e:
            print(f"Loading filter options failed: {e}")


# Button functions

    def get_urls_from_selection(self):
        urls = []
        selected_iids = self.results_tree.selection()
        for iid in selected_iids:
            url = self.result_urls[iid]
            urls.append(url)
        return urls

    def show_url(self, event):
        selected_iids = self.results_tree.selection()
        for iid in selected_iids:
            url = self.result_urls[iid]
            clean_url = unquote(url.replace(self.BASE_URL, ''))
            self.status_label.configure(text=clean_url)

    def select_folder(self):
        directory = ctk.filedialog.askdirectory()
        self.download_dir.set(value=directory)



# Dropdown select functions

    def on_platform_select(self, value):
        if self.platform_var.get() == value:
            return
        self.platform_var.set(value)
        self.schedule_search()

    def on_region_select(self, value):
        if self.region_var.get() == value:
            return
        self.region_var.set(value)
        self.schedule_search()

    def on_language_select(self, value):
        if self.language_var.get() == value:
            return
        self.language_var.set(value)
        self.schedule_search()

    def on_version_select(self, value):
        if self.version_var.get() == value:
            return
        self.version_var.set(value)
        self.schedule_search()



# Filter functions

    def reset_filters(self):
        self.load_platforms()
        self.do_search()

# Rebuild filter menus from search results
    def update_filters(self, platforms, regions, languages, versions):
        def update(menu, new_values, var):
            new_values = new_values or ["all"]

            # Only touch the menu if options changed
            if getattr(menu, "_last_values", None) != new_values:
                current = var.get()
                if current not in new_values:
                    current = "all"

                # Update the menu’s list + shown selection
                menu.update_values(new_values, current)

                # Keep the external StringVar in sync with what the menu now shows
                if var.get() != current:
                    var.set(current)

                menu._last_values = list(new_values)  # store copy to compare next time

            else:
                # Options same; ensure selection stays consistent
                current = var.get()
                if current not in new_values:
                    current = "all"
                    menu.update_values(new_values, current)
                    var.set(current)

        update(self.platform_menu, platforms, self.platform_var)
        update(self.region_menu, regions, self.region_var)
        update(self.language_menu, languages, self.language_var)
        update(self.version_menu, versions, self.version_var)



# Search functions
    def do_search(self, offset=0):
        self.current_offset = offset
        self.load_more_button.configure(state="disabled")

        try:
            limit = min(int(self.number_of_results_var.get()), 5000)
            self.number_of_results_var.set(limit)
        except ValueError:
            limit = 100

        def search_thread():
            results, platforms, regions, languages, versions = self.backend.advanced_search(
                platform=self.platform_var.get() if self.platform_var.get() != "all" else None,
                region=self.region_var.get() if self.region_var.get() != "all" else None,
                language=self.language_var.get() if self.language_var.get() != "all" else None,
                version=self.version_var.get() if self.version_var.get() != "all" else None,
                title_contains=self.title_entry.get(),
                title_regex=self.regex_search_var.get(),
                offset=self.current_offset,
                limit=limit
            )

            def update_ui():
                # Clear Treeview and URL list before inserting new results
                for i in self.results_tree.get_children():
                    self.results_tree.delete(i)
                self.result_urls.clear()

                self.update_filters(platforms, regions, languages, versions)

                for idx, r in enumerate(results):
                    values = (
                        r['title'],
                        r['platform'],
                        r['region'] or '-',
                        r['language'] or '-',
                        r['version'] or '-',
                        r['size']
                    )
                    iid = str(idx)
                    self.results_tree.insert("", "end", iid=iid, values=values)
                    self.result_urls[iid] = r['url']

                # Update status label
                total_results = len(self.result_urls)
                platform_string = f'{self.platform_var.get().upper()}' if self.platform_var.get() != "all" else "any platform"
                title_string = f'"{self.title_entry.get().upper()}"' if self.title_entry.get() else "all games"
                self.status_label.configure(
                    text=f'Showing results {self.current_offset + 1} to {total_results + self.current_offset} '
                        f'for {title_string} on {platform_string}.'
                )

                self.enable_all_widgets()

                # Enable/disable Load More button
                self.load_more_button.configure(state="normal" if len(results) == limit else "disabled")

            self.after(0, update_ui)

        threading.Thread(target=search_thread, daemon=True).start()

    def load_more_results(self):
            """Load next batch of search results"""
            self.current_offset += int(self.number_of_results_var.get())
            self.do_search(self.current_offset)

    def schedule_search(self, delay=200):
        if getattr(self, "_search_after_id", None):
            self.after_cancel(self._search_after_id)
        self._search_after_id = self.after(delay, self.do_search, 0)


# Download

    def start_download(self):
            self.disable_all_except_one(self.cancel_button)

            download_dir = self.download_dir.get() or "downloads"
            download_dir = download_dir.replace("\\", "/")
            self.downloader = Downloader(output_dir=download_dir)

            selected_items = self.results_tree.selection()
            if not selected_items:
                self.status_label.configure(text="Select one or more items to download")
                return
            else:
                plural = "s" if len(selected_items) > 1 else ""
                self.status_label.configure(text=f"Downloading {len(selected_items)} file" + plural)

            self.progress_update_delay = self.downloader.max_file_workers * 10

            urls_to_download = self.get_urls_from_selection()

            # Clear the downloader queue and add URLs
            self.downloader.download_queue.empty()
            for url in urls_to_download:
                self.downloader.add_url(url)

            self.ensure_progress_labels(self.downloader.max_file_workers)
            self.reset_progress_labels()

            self.download_button.configure(state="disabled")
            self.cancel_button.configure(state="normal")
            self.search_button.configure(state="disabled")
            self.update_button.configure(state="disabled")

            self.download_start_time = time()

            def progress_callback(file_idx, url, downloaded_bytes, total_bytes):
                filename = unquote(os.path.basename(url))
                percent = downloaded_bytes / total_bytes * 100 if total_bytes else 0
                progress_text = f"{filename} - {downloaded_bytes / (1024*1024):.2f} MB / {total_bytes / (1024*1024):.2f} MB ({percent:.1f}%)"
                self.progress_queue.put((file_idx, progress_text, downloaded_bytes >= total_bytes, percent))

            def done_callback(completed_files, total_files):
                elapsed = time() - self.download_start_time
                self.progress_queue.put(("done", completed_files, total_files, elapsed))
                self.enable_all_widgets()

            # Run downloader in a thread to avoid blocking GUI
            threading.Thread(
                target=self.downloader.start,
                kwargs={"progress_callback": progress_callback, "done_callback": done_callback},
                daemon=True
            ).start()

    def stop_download(self):
            if self.downloader: self.downloader.cancel_all()
            self.enable_all_widgets()



# Labels and progress

    def ensure_progress_labels(self, count):
            """Ensure there are at least `count` labels, creating more if needed."""
            while len(self.progress_labels) < count:
                progressbar = ctk.CTkProgressBar(self.progress_frame)
                progressbar.pack(fill="both", padx=self.padx, pady=0, expand=True)
                label = ctk.CTkLabel(self.progress_frame, text="")
                label.pack(fill="x", padx=self.padx, pady=0)
                # [progressbar, label, in_use_flag, file_index]
                self.progress_labels.append([progressbar, label, False, None])

    def reset_progress_labels(self):
            """Clear label text and mark them as unused without destroying."""
            for widgets in self.progress_labels:
                progressbar, label = widgets[0], widgets[1]
                progressbar.set(0.0)
                label.configure(text="")
                widgets[2] = False
                widgets[3] = None

    def update_progress_labels_from_queue(self):
            try:
                while True:
                    msg = self.progress_queue.get_nowait()
                    if msg[0] == "done":
                        completed_files, total_files, duration = msg[1], msg[2], msg[3]
                        self.reset_progress_labels()
                        if self.progress_labels:
                            self.status_label.configure(
                                text=f"Download complete: {completed_files}/{total_files} files in {round(duration, 2)}s."
                            )
                        self.enable_all_widgets()
                    elif msg[0] == "Rescan and update complete.":
                        self.status_label.configure(text=msg[0])
                    else:
                        file_idx, text, finished, percent = msg
                        widgets = self.get_or_assign_label(file_idx)
                        progressbar, label = widgets[0], widgets[1]
                        label.configure(text=text)
                        progressbar.set(1.0 if finished else percent/100)
                        if finished:
                            widgets[2] = False
                            widgets[3] = None
            except queue.Empty:
                pass
            finally:
                self.after(self.progress_update_delay, self.update_progress_labels_from_queue)

    def get_or_assign_label(self, file_idx):
            for widgets in self.progress_labels:
                if widgets[3] == file_idx:
                    return widgets
            for widgets in self.progress_labels:
                if not widgets[2] and widgets[3] is None:
                    widgets[2] = True
                    widgets[3] = file_idx
                    return widgets
            return self.progress_labels[0]  # fallback



# Database

    def check_if_db_exists(self):
        self.backend = MyrientBackend(self.DB_FILE)
        if not os.path.exists(self.DB_FILE):
            self.status_label.configure(text="No database file found. Click update DB or check path.")
            self.disable_all_except_one(self.update_button)
        else:
            self.enable_all_widgets()

            platforms = self.backend.list_platforms()
            regions = self.backend.list_regions()
            languages = self.backend.list_languages()
            versions = self.backend.list_versions()

            self.update_filters(platforms, regions, languages, versions)

    def update_db(self, repair=False, progress_callback=None):
        for i in self.results_tree.get_children():
            self.results_tree.delete(i)

        # Disable buttons during update
        self.disable_all_except_one(None)

        def threaded_update():
            if not repair:
                conn = self.backend.get_conn()
                crawler.crawl_and_index(self.BASE_URL, self.DB_FILE, self.update_db_progress, extract_version_from_parent=True)
                conn.close()
            else:
                crawler.rescan_database(base_url=self.BASE_URL, db_path=self.DB_FILE, progress_callback=progress_callback, extract_version_from_parent=True)

            def update_done_ui():
                self.status_label.configure(text="Database update complete")
                self.reset_filters()
                self.enable_all_widgets()

            self.after(0, update_done_ui)

        threading.Thread(target=threaded_update, daemon=True).start()

    def update_db_progress(self, value):
        value = f'"{value}"'
        self.results_tree.insert("", 0, values=value)

    def repair_db(self):
        self.ensure_progress_labels(1)
        self.progress_update_delay = 10
        def progress_update(current, total):
            if current not in ("Rescan and update complete.", "Ignored platform deletion complete."):
                percent = round(int(current) / int(total) * 100, 2)
                self.progress_queue.put((0, f"Database repair in progress. {current} of {total} entries processed.", current >= total, percent))
            else:
                self.progress_queue.put((0, current, True, ""))

        self.update_db(repair=True, progress_callback=progress_update)



# Closing application

    def close_window(self):
        if self.downloader:
            self.downloader.cancel_all()
        self.after(100, self.check_exit)

    def check_exit(self):
        if self.downloader: 
            if self.downloader.all_stopped():
                self.destroy()
            else:
                self.after(100, self.check_exit)
        else:
            self.destroy()
