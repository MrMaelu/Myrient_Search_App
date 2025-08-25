"""Web crawler for Myrient Search App."""
import concurrent.futures
import contextlib
import re
import shutil
import sqlite3
import threading
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from queue import Queue
from time import time
from urllib.parse import unquote

import requests
from lxml import html


@dataclass
class CrawlContext:
    """Context object holding shared state for the Myrient crawler.

    Attributes:
        base_url (str): The root URL to start crawling from.
        db_path (str): Path to the SQLite database file.
        folder_queue (Queue[str]): Queue of folders to process.
        visited (set[str]): Set of already visited folder paths.
        progress_callback (Callable[[str], None] | None): Optional callback
            for progress updates.
        thread_local (threading.local): Thread-local storage for SQLite
            connections.

    """

    base_url: str
    db_path: str
    folder_queue: Queue[str]
    visited: set[str]
    progress_callback: Callable[[str], None] | None
    thread_local: threading.local

    def get_connection(self) -> sqlite3.Connection:
        """Retrieve a thread-local SQLite connection for this crawl context.

        Returns:
            sqlite3.Connection: SQLite connection unique to the current thread.

        """
        if not hasattr(self.thread_local, "conn"):
            self.thread_local.conn = sqlite3.connect(self.db_path)
        return self.thread_local.conn


ignored_base_folders = [
    #'Internet Archive',
    "Miscellaneous",
    #'No-Intro',
    #'TOSEC-ISO',
    "TOSEC-PIX",
    #'TOSEC',
    #'Redump',
    "96x65pixels",
    "EBZero",
    "Unknown",
    "aberusugi",
    "aitus95",
    "archiver_2020",
    "bingbong294",
    "bluemaxima",
    "chadmaster",
    "cmpltromsets",
    "kodi_amp_spmc_canada",
    "lollo_220",
    "md_",
    "mdashk",
    "pixelspoil",
    "retro_game_champion",
    "romhacking_net",
    "rompacker",
    "rvt-r",
    "sketch_the_cow",
    "storage_manager",
    "superbio",
    "teamgt19",
    "the_last_collector",
    #'who_lee',
    "yahweasel",
]

ignored_base_folders = [folder.lower() for folder in ignored_base_folders]

ignored_folders = [
    "audio cd",
    "bd-video",
    "dvd-video",
    "video cd",
    "disc keys",
    "(themes)",
    "(updates)",
    "firmware",
    "demos",
    "docs",
    "gdi files",
    "applications",
    "operating systems",
    "various",
    "magaz",
    #'graphics',
    "educational",
    "samplers",
    "coverdiscs",
    "(music)",
    "diskmags",
    "books",
    "bios",
    "demo",
    "(movie only)",
    "documents",
    "video game osts",
    "video game scans",
    "source code",
    "playstation gameshark updates",
    "non-redump",
    "promo",
    "amiibo",
    "nintendo sdks",
    "ultimate codes",
    "action replay",
    "unlimited codes",
    "cheatcodes",
    "cheat code",
    "cheats",
    "cheat master",
    "cheat disc",
]

ignored_folders = [folder.lower() for folder in ignored_folders]

# Known aliases → canonical names
platform_aliases = {
    # Apple
    "apple 1": "Apple I",
    "apple i": "Apple I",
    "apple ii": "Apple II",
    "apple ii plus": "Apple II Plus",
    "apple iie": "Apple IIe",
    "apple iigs": "Apple IIGS",

    # VM Labs
    "vm labs nuon": "VM Labs NUON",

    # NEC
    "nec pc engine cd & turbografx": "NEC PC Engine CD & TurboGrafx-16",
    "nec pc engine cd + turbografx": "NEC PC Engine CD & TurboGrafx-16",
    "nec pc-engine & turbografx-16": "NEC PC Engine & TurboGrafx-16",
    "nec pc-engine cd & turbografx-16": "NEC PC Engine CD & TurboGrafx-16",

    # SNK
    "snk neo geo": "SNK Neo Geo",
    "snk neo-geo": "SNK Neo Geo",
    "snk neo-geo cd": "SNK Neo Geo CD",
    "snk neogeo pocket": "SNK Neo Geo Pocket",
    "snk neogeo pocket color": "SNK Neo Geo Pocket Color",

    # Nintendo
    "nintendo famicom & entertainment system": "Nintendo Entertainment System",
    "nintendo super famicom & entertainment system":
        "Super Nintendo Entertainment System",
    "nintendo super nintendo entertainment system":
        "Super Nintendo Entertainment System",
    "nintendo super entertainment system": "Super Nintendo Entertainment System",
    "nintendo famicom disk system": "Nintendo Famicom Disk System",
    "nintendo wii [zstd-19-128k]": "Nintendo Wii",
    "nintendo gamecube [zstd-19-128k]": "Nintendo Gamecube",

    # IBM
    "ibm pc compatible": "IBM PC Compatible",
    "ibm pc and compatibles": "IBM PC Compatible",
    "IBM PC Compatible SBI Subchannels": "IBM PC Compatible",
    "IBM PC Compatibles": "IBM PC Compatible",
}


# Main crawler
def crawl_and_index(
    base_url: str, db_path: str, progress_callback: Callable[[str], None] | None,
) -> None:
    """Crawl the Myrient site starting from a base URL and index files into SQLite.

    This function traverses folders on the site, parses metadata for each file,
    and stores it in a local SQLite database. Crawling is performed concurrently
    using a thread pool. Progress can be reported via an optional callback.

    Args:
        base_url (str): The root URL to start crawling from.
        db_path (str): Path to the SQLite database file for storing file metadata.
        progress_callback (Callable[[str], None] | None): Optional callback
            to receive progress updates as strings.

    Returns:
        None

    Notes:
        - Creates the `files` table if it does not exist.
        - Uses a thread-local SQLite connection per worker thread.
        - Skips folders listed in `ignored_base_folders` and `ignored_folders`.
        - Commits database changes after processing each folder batch.

    """
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS files (
            url TEXT PRIMARY KEY,
            title TEXT,
            platform TEXT,
            collection TEXT,
            region TEXT,
            language TEXT,
            version TEXT,
            size INTEGER,
            last_modified TEXT
        )
        """,
    )
    conn.commit()

    ctx = CrawlContext(
        base_url=base_url,
        db_path=db_path,
        folder_queue=Queue(),
        visited=set(),
        progress_callback=progress_callback,
        thread_local=threading.local(),
    )
    ctx.folder_queue.put("")
    processed = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        crawl_start = time()
        while not ctx.folder_queue.empty():
            batch_start = time()
            current_batch, processed = _get_batch(ctx, processed)
            if not current_batch:
                break
            _process_batch(executor, ctx, current_batch)

            if progress_callback:
                batch_time = time() - batch_start
                total_time = time() - crawl_start
                progress_callback("-" * 50)
                progress_callback(f"Batch time: {round(batch_time, 2)}")
                progress_callback(f"Total time: {round(total_time, 2)}")
                progress_callback(f"{processed} folders processed.")
                progress_callback("-" * 50)

    if hasattr(ctx.thread_local, "conn"):
        ctx.thread_local.conn.close()
    conn.close()

    if progress_callback:
        progress_callback("Indexing done.")


def _process_folder(folder: str, ctx: CrawlContext) -> list[str]:
    folder_url = ctx.base_url.rstrip("/") + "/" + folder.lstrip("/")
    if not folder_url.endswith("/"):
        folder_url += "/"

    folders: list[str] = []
    try:
        entries = fetch_folder_listing(folder_url)
    except (ConnectionError, TimeoutError):
        return []

    local_conn = ctx.get_connection()
    cursor = local_conn.cursor()

    for entry in entries:
        name = entry["name"]
        decoded = unquote(name.strip("/").lower())

        if decoded in ignored_base_folders:
            continue

        if name.endswith("/") and decoded not in ignored_base_folders:
            folders.append(folder + name)
        else:
            meta = parse_metadata_from_path(folder + name, ctx.base_url)
            if not meta:
                continue

            meta["platform"] = normalize_platform_name(meta["platform"])

            with contextlib.suppress(sqlite3.Error):
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO files
                    (url, title, platform, collection, region, language,
                        version, size, last_modified)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        meta["url"],
                        meta["title"],
                        meta["platform"],
                        meta["collection"],
                        meta["region"],
                        meta["language"],
                        meta["version"],
                        entry.get("size"),
                        entry.get("last_modified"),
                    ),
                )

    local_conn.commit()
    return folders

def _get_batch(ctx: CrawlContext, processed: int) -> tuple[list[str], int]:
    current_batch: list[str] = []
    for _ in range(8):
        if ctx.folder_queue.empty():
            break
        folder = ctx.folder_queue.get()
        if folder not in ctx.visited:
            ctx.visited.add(folder)
            if any(ig in unquote(folder).lower() for ig in ignored_folders):
                if ctx.progress_callback:
                    ctx.progress_callback(f" *** Ignoring {unquote(folder)}")
                continue
            current_batch.append(folder)
            processed += 1
            if ctx.progress_callback:
                ctx.progress_callback(f"Processing {unquote(folder)}")
    return current_batch, processed


def _process_batch(
    executor: concurrent.futures.ThreadPoolExecutor, ctx: CrawlContext,
    current_batch: list[str],
) -> None:
    future_to_folder = {
        executor.submit(_process_folder, folder, ctx): folder
        for folder in current_batch
    }
    for future in concurrent.futures.as_completed(future_to_folder):
        try:
            new_folders = future.result()
            for nf in new_folders:
                if nf not in ctx.visited:
                    ctx.folder_queue.put(nf)
        except sqlite3.Error:
            pass
        except (ConnectionError, TimeoutError):
            pass


# Rescan and rebuild existing database without crawling the website
def rescan_database(db_path:str, base_url:str, progress_callback:Callable|None) -> None:
    """Rescan and rebuild existing database without crawling the website."""
    backup_path = db_path.with_suffix(db_path.suffix + ".bak")

    if not Path.exists(backup_path):
        shutil.copy2(db_path, backup_path)
    else:
        pass

    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    c.execute("SELECT url FROM files")
    urls = [row[0] for row in c.fetchall()]

    total = len(urls)

    try:
        for i, url in enumerate(urls, 1):
            # Extract path relative to base_url
            url_path = url.removeprefix(base_url)

            meta = parse_metadata_from_path(url_path, base_url)
            if not meta:
                continue

            normalized_platform = normalize_platform_name(meta["platform"])
            meta["platform"] = normalized_platform

            # Update DB entry with new metadata
            c.execute("""
                UPDATE files SET
                    title = ?,
                    platform = ?,
                    collection = ?,
                    region = ?,
                    language = ?,
                    version = ?
                WHERE url = ?
            """, (
                meta["title"], meta["platform"], meta["collection"], meta["region"],
                meta["language"], meta["version"], meta["url"],
            ))

            if i % 100 == 0:
                conn.commit()
                progress_callback(i, total)

        delete_ignored_platforms(conn, progress_callback)

        conn.commit()
        progress_callback("Rescan and update complete.", "")

    except sqlite3.Error:
        conn.rollback()
    finally:
        conn.close()

# Helper functions
def delete_ignored_platforms(conn:sqlite3.Connection,
                             progress_callback:Callable|None,
                             batch_size:int=500) -> None:
    """Delete ignored platforms after DB repair."""
    c = conn.cursor()

    ignored = ignored_base_folders + ignored_folders
    patterns = [f"%{p}%" for p in ignored]

    url_conditions = " OR ".join(["LOWER(url) LIKE ?"] * len(patterns))
    platform_conditions = " OR ".join(["LOWER(platform) LIKE ?"] * len(patterns))
    title_conditions = " OR ".join(["LOWER(title) LIKE ?"] * len(patterns))

    where_clause = (
        f"({url_conditions}) OR ({platform_conditions}) OR ({title_conditions})"
        )
    params = patterns * 3

    # Get matching row IDs first
    c.execute(f"SELECT rowid FROM files WHERE {where_clause}", params)  # noqa: S608
    matching_ids = [row[0] for row in c.fetchall()]
    total = len(matching_ids)

    deleted = 0
    while matching_ids:
        batch_ids = matching_ids[:batch_size]
        matching_ids = matching_ids[batch_size:]

        # Delete current batch
        placeholders = ",".join("?" for _ in batch_ids)
        c.execute(f"DELETE FROM files WHERE rowid IN ({placeholders})", batch_ids)  # noqa: S608

        deleted += len(batch_ids)
        if progress_callback:
            progress_callback(deleted, total)

    if progress_callback:
        progress_callback("Ignored platform deletion complete.", "")


def normalize_platform_name(platform:str) -> str:
    """Replace aliases (case-insensitive)."""
    lower_platform = platform.lower()
    for alias, canonical in platform_aliases.items():
        if alias in lower_platform:
            # Simple case-insensitive replacement
            pattern = re.compile(re.escape(alias), re.IGNORECASE)
            platform = pattern.sub(canonical, platform)
            break

    # Remove text after '(' and split on ' - '
    parts = platform.split("(")[0].split(" - ")
    parts = [p.strip() for p in parts]

    # Remove consecutive duplicate words (case-insensitive)
    prev_word = None
    tokens = []
    for word in " ".join(parts).split():
        lw = word.lower()
        if lw != prev_word:
            tokens.append(word)
        prev_word = lw

    # Remove 'non-redump' from platform name
    cleaned_tokens = [token for token in tokens if token.lower() not in ignored_folders]

    normalized = " ".join(cleaned_tokens).strip()
    return normalized.title()

def fetch_folder_listing(url:str) -> list:
    """Fetch folder contents from URL."""
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    tree = html.fromstring(r.content)
    entries = []

    rows = tree.xpath('//table[@id="list"]//tr')[1:]  # skip header row
    for row in rows:
        href = row.xpath('.//td[@class="link"]/a/@href')
        size = row.xpath('.//td[@class="size"]/text()')
        date = row.xpath('.//td[@class="date"]/text()')
        if href:
            name = href[0]
            size_text = size[0].strip() if size else None
            date_text = date[0].strip() if date else None
            if name != "../":
                entries.append({
                    "name": name,
                    "size": size_text,
                    "last_modified": date_text,
                })

    return entries

def parse_metadata_from_path(url_path:str, base_url:str) -> dict|None:
    """Parse metadata from the full url."""
    if not url_path.endswith(".zip"):
        return None

    parts = url_path.strip("/").split("/")

    if "tosec" in url_path.lower() and "games" in url_path.lower():
        platform_raw = f"{parts[1]} - {parts[2]}"
    elif "who_lee" in unquote(url_path.lower()):
        platform_raw = parts[3]
    else:
        platform_raw = parts[1]

    collection = unquote(parts[0])
    platform_raw = unquote(platform_raw)
    filename = unquote(parts[-1])
    title = filename

    # Remove extension from title
    if "." in filename:
        title = filename.rsplit(".", 1)[0]

    # Extract metadata from title parentheses
    meta_parts = re.findall(r"\(([^)]+)\)", title)

    """region, languages, version = parse_region_languages_and_version(
        meta_parts,
        platform_raw,
        parts,
        )"""

    region = _extract_region(meta_parts)
    languages = _extract_languages(meta_parts)
    version = _extract_version(meta_parts, parts, platform_raw)
    platform = normalize_platform_name(platform_raw)

    # Remove duplicates from languages
    seen_langs = set()
    languages = [
        x.upper() for x in languages if not (x in seen_langs or seen_langs.add(x))
        ]

    title_clean = re.sub(r"\s*\([^)]*\)", "", title).strip()

    return {
        "title": title_clean,
        "platform": platform,
        "collection": collection,
        "region": region,
        "language": ",".join(languages) if languages else None,
        "version": version,
        "url": base_url + url_path.lstrip("/"),
    }


def _extract_region(meta_parts: list[str]) -> str | None:
    regions = {
        "us", "eu", "jp", "pal", "europe", "usa", "japan", "ntsc", "china", "korea",
        }
    for p in meta_parts:
        if p.lower() in regions:
            return p.upper()
    return None


def _extract_languages(meta_parts: list[str]) -> list[str]:
    lang_map = {
        "en": "EN", "english": "EN",
        "fr": "FR", "french": "FR",
        "de": "DE", "german": "DE",
        "es": "ES", "italian": "IT", "it": "IT",
        "jp": "JP", "japanese": "JP",
    }
    languages: list[str] = []
    for p in meta_parts:
        p_low = p.lower().replace(" ", "")
        if re.match(r"^[a-z]{2}(,[a-z]{2})*$", p_low):
            languages.extend([lang.strip().upper() for lang in p_low.split(",")])
        elif p_low in lang_map:
            languages.append(lang_map[p_low])
    return languages


def _extract_version(meta_parts: list[str],
                     parts: list[str],
                     platform_raw: str,
                     ) -> str | None:
    versions = ["decrypted", "encrypted"]
    for p in meta_parts:
        if p.lower() in versions:
            return p.title()

    # Check parent folders
    parent_folders = [p.lower() for p in parts[:-1]]
    for vf in versions:
        if any(vf in pf for pf in parent_folders):
            return vf.title()

    # Check platform string
    version_keywords = ["nkit rvz", "nkit", "rvz"]
    platform_lower = platform_raw.lower()
    for vk in version_keywords:
        if vk in platform_lower:
            return vk.title()

    return None
