import os
import re
import shutil
import sqlite3
import requests
import threading
import time
from time import time
import lxml
from lxml import html
import queue
from queue import Queue
import concurrent.futures
import urllib.parse
from urllib.parse import unquote


ignored_base_folders = [
    #'Internet Archive',
    'Miscellaneous',
    #'No-Intro',
    #'TOSEC-ISO',
    'TOSEC-PIX',
    #'TOSEC',
    #'Redump',
    '96x65pixels',
    'EBZero',
    'Unknown',
    'aberusugi',
    'aitus95',
    'archiver_2020',
    'bingbong294',
    'bluemaxima',
    'chadmaster',
    'cmpltromsets',
    'kodi_amp_spmc_canada',
    'lollo_220',
    'md_',
    'mdashk',
    'pixelspoil',
    'retro_game_champion',
    'romhacking_net',
    'rompacker',
    'rvt-r',
    'sketch_the_cow',
    'storage_manager',
    'superbio',
    'teamgt19',
    'the_last_collector',
    #'who_lee',
    'yahweasel',
]

lower_cased_base_folders = []
for folder in ignored_base_folders:
    lower_cased_base_folders.append(folder.lower())

ignored_base_folders = lower_cased_base_folders

ignored_folders = [
    'audio cd',
    'bd-video',
    'dvd-video',
    'video cd',
    'disc keys',
    '(themes)',
    '(updates)',
    'firmware',
    'demos',
    'docs',
    'gdi files',
    'applications',
    'operating systems',
    'various',
    'magaz',
    #'graphics',
    'educational',
    'samplers',
    'coverdiscs',
    '(music)',
    'diskmags',
    'books',
    'bios',
    'demo',
    '(movie only)',
    'documents',
    'video game osts',
    'video game scans',
    'source code',
    'playstation gameshark updates',
    'non-redump',
    'promo',
    'demos',
    'amiibo',
    'nintendo sdks',
    'ultimate codes',
    'action replay',
    'unlimited codes',
    'cheatcodes',
    'cheat code',
    'cheats',
    'cheat master',
    'cheat disc',
]

lower_cased_folders = []
for folder in ignored_folders:
    lower_cased_folders.append(folder.lower())

ignored_folders = lower_cased_folders

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
    "nintendo super famicom & entertainment system": "Super Nintendo Entertainment System",
    "nintendo super nintendo entertainment system": "Super Nintendo Entertainment System",
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
def crawl_and_index(base_url, db_path, progress_callback, extract_version_from_parent):
    #time_format = "%d-%b-%Y %H:%M"
    #db_timestamp_file = 'db_update_timestamp'
    #try:
    #    with open(db_timestamp_file, 'r') as f:
    #        db_update_date = datetime.datetime.strptime(f.read(), time_format)
    #except Exception as e:
    #    db_update_date = None

    """Recursively crawl remote folders and index files in SQLite using multiple threads."""
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("""
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
    """)
    conn.commit()

    # Thread-local storage for database connections
    thread_local = threading.local()

    def get_connection():
        if not hasattr(thread_local, "conn"):
            thread_local.conn = sqlite3.connect(db_path)
        return thread_local.conn

    def process_folder(url_path):
        folder_url = base_url.rstrip('/') + '/' + url_path.lstrip('/')
        if not folder_url.endswith('/'):
            folder_url += '/'

        try:
            entries = fetch_folder_listing(folder_url)
            folders = []
            local_conn = get_connection()
            cursor = local_conn.cursor()

            for entry in entries:
                name = entry["name"]
                if unquote(name.strip("/").lower()) in ignored_base_folders:
                    print(f"Ignored base folder: {unquote(name)}")
                if name.endswith("/") and unquote(name.strip("/").lower()) not in ignored_base_folders:
                    path = url_path + name
                    folders.append(path)
                else:
                    meta = parse_metadata_from_path(url_path + name, base_url, extract_version_from_parent=extract_version_from_parent)
                    if not meta:
                        continue

                    # Normalize platform name with duplicate filtering
                    normalized_platform = normalize_platform_name(meta["platform"])
                    meta["platform"] = normalized_platform

                    try:
                        cursor.execute("""
                            INSERT OR REPLACE INTO files 
                            (url, title, platform, collection, region, language, version, size, last_modified)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            meta["url"], meta["title"], meta["platform"], meta["collection"],
                            meta["region"], meta["language"], meta["version"],
                            entry.get("size"), entry.get("last_modified")
                        ))
                    except Exception as e:
                        print(f"DB error for {name}: {e}")

            local_conn.commit()
            return folders

        except Exception as e:
            print(f"Failed to fetch {folder_url}: {e}")
            return []


    visited = set()
    folder_queue = Queue()
    folder_queue.put("")  # Start with root
    folders_processed = 0

    # Process folders with thread pool
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        crawl_start_time = time()
        while not folder_queue.empty():
            batch_start_time = time()
            current_batch = []
            # Get up to 8 folders to process in parallel
            for _ in range(8):
                if folder_queue.empty():
                    break
                folder = folder_queue.get()
                if folder not in visited:
                    visited.add(folder)
                    should_ignore = False

                    for ignored_folder in ignored_folders:
                        if ignored_folder.lower() in unquote(folder).lower():
                            progress_callback(f" *** Ignoring {unquote(folder)}")
                            print(f" *** Ignoring {unquote(folder)}")
                            should_ignore = True
                            break

                    if not should_ignore:
                        current_batch.append(folder)
                        folders_processed += 1
                        progress_callback(f"Processing {unquote(folder)}")

            if not current_batch:
                break

            # Process current batch of folders in parallel
            future_to_folder = {executor.submit(process_folder, folder): folder 
                              for folder in current_batch}

            # Handle results and queue new folders
            for future in concurrent.futures.as_completed(future_to_folder):
                folder = future_to_folder[future]
                try:
                    new_folders = future.result()
                    for new_folder in new_folders:
                        if new_folder not in visited:
                            folder_queue.put(new_folder)
                except Exception as e:
                    print(f"Error processing folder {folder}: {e}")

            batch_time = time() - batch_start_time
            total_time = time() - crawl_start_time

            progress_callback("-" * 50)
            progress_callback(f"Batch process time: {round(batch_time, 2)}")
            progress_callback(f"Total time: {round(total_time, 2)}")
            progress_callback(f"{folders_processed} folders processed.")
            progress_callback("-" * 50)

    # Close all thread-local connections
    if hasattr(thread_local, "conn"):
        thread_local.conn.close()
    conn.close()

    progress_callback("Indexing done.")

#    with open(db_timestamp_file, "w") as f:
#        f.write(datetime.datetime.now().strftime(time_format))


# Rescan and rebuild existing database without crawling the website
def rescan_database(db_path, base_url, progress_callback, extract_version_from_parent=False):
    backup_path = db_path + ".bak"

    if not os.path.exists(backup_path):
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
            if url.startswith(base_url):
                url_path = url[len(base_url):]
            else:
                url_path = url  # fallback

            meta = parse_metadata_from_path(url_path, base_url, extract_version_from_parent)
            if not meta:
                continue

            normalized_platform = normalize_platform_name(meta["platform"])
            meta["platform"] = normalized_platform

            # Update DB entry with new metadata but keep size and last_modified untouched
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
                meta["language"], meta["version"], meta["url"]
            ))

            if i % 100 == 0:
                conn.commit()
                progress_callback(i, total)

        delete_ignored_platforms(conn, progress_callback)

        conn.commit()
        progress_callback("Rescan and update complete.", "")

    except Exception as e:
        conn.rollback()
        #progress_callback(f"Error during rescan: {e}", "")
        print(f"Error during rescan: {e}", "")
    finally:
        conn.close()

# Helper functions
def delete_ignored_platforms(conn, progress_callback=None, batch_size=500):
    c = conn.cursor()

    ignored = ignored_base_folders + ignored_folders
    patterns = [f"%{p}%" for p in ignored]

    url_conditions = " OR ".join(["LOWER(url) LIKE ?"] * len(patterns))
    platform_conditions = " OR ".join(["LOWER(platform) LIKE ?"] * len(patterns))
    title_conditions = " OR ".join(["LOWER(title) LIKE ?"] * len(patterns))

    where_clause = f"({url_conditions}) OR ({platform_conditions}) OR ({title_conditions})"
    params = patterns * 3

    # Get matching row IDs first
    c.execute(f"SELECT rowid FROM files WHERE {where_clause}", params)
    matching_ids = [row[0] for row in c.fetchall()]
    total = len(matching_ids)

    deleted = 0
    while matching_ids:
        batch_ids = matching_ids[:batch_size]
        matching_ids = matching_ids[batch_size:]

        # Delete current batch
        placeholders = ",".join("?" for _ in batch_ids)
        c.execute(f"DELETE FROM files WHERE rowid IN ({placeholders})", batch_ids)

        deleted += len(batch_ids)
        if progress_callback:
            progress_callback(deleted, total)

    if progress_callback:
        progress_callback("Ignored platform deletion complete.", "")


def normalize_platform_name(platform):
    # Replace aliases (case-insensitive)
    lower_platform = platform.lower()
    for alias, canonical in platform_aliases.items():
        if alias in lower_platform:
            # Simple case-insensitive replacement
            pattern = re.compile(re.escape(alias), re.IGNORECASE)
            platform = pattern.sub(canonical, platform)
            break

    # Remove text after '(' and split on ' - '
    parts = platform.split('(')[0].split(' - ')
    parts = [p.strip() for p in parts]

    # Remove consecutive duplicate words (case-insensitive)
    prev_word = None
    tokens = []
    for word in ' '.join(parts).split():
        lw = word.lower()
        if lw != prev_word:
            tokens.append(word)
        prev_word = lw

    # Remove 'non-redump' from platform name
    cleaned_tokens = []
    for token in tokens:
        if token.lower() not in ignored_folders:
            cleaned_tokens.append(token)

    normalized = ' '.join(cleaned_tokens).strip()
    return normalized.title()

def fetch_folder_listing(url):
    """Fetch folder contents from URL."""
    r = requests.get(url)
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

def parse_metadata_from_path(url_path, base_url, extract_version_from_parent=False):
    if not url_path.endswith(".zip"):
        return None


    parts = url_path.strip("/").split("/")
    if len(parts) < 3:
        return None

    if 'tosec' in url_path.lower():
        if not 'games' in url_path.lower():
            return None
        platform_raw = f"{parts[1]} - {parts[2]}"

    elif 'who_lee' in unquote(url_path.lower()):
        platform_raw = parts[3]

    else:
        platform_raw = parts[1]

    

    collection = unquote(parts[0])
    platform_raw = unquote(platform_raw)
    filename = unquote(parts[-1])
    title = filename
    region = None
    languages = []
    version = None

    # Remove extension from title
    if "." in filename:
        title = filename.rsplit(".", 1)[0]

    # Extract metadata from title parentheses
    meta_parts = re.findall(r"\(([^)]+)\)", title)
    for p in meta_parts:
        p_low = p.lower()

        # Region detection
        if p_low in ("us", "eu", "jp", "pal", "europe", "usa", "japan", "ntsc", "china", "korea"):
            region = p.upper()
        
        # Language detection (multiple comma-separated)
        elif re.match(r"^[a-z]{2}(,[a-z]{2})*$", p_low.replace(" ", "")):
            langs = [lang.strip() for lang in p_low.split(",")]
            languages.extend(langs)
        else:
            lang_map = {
                "en": "en",
                "english": "en",
                "fr": "fr",
                "french": "fr",
                "de": "de",
                "german": "de",
                "es": "es",
                "italian": "it",
                "it": "it",
                "jp": "jp",
                "japanese": "jp",
            }
            if p_low in lang_map:
                languages.append(lang_map[p_low].upper())

        # Version detection from filename parentheses
        versions = ["decrypted", "encrypted"]
        if p_low in versions:
            version = p.title()

    # Extract version from parent folder name if enabled and version not found yet
    if extract_version_from_parent and version is None:
        parent_folders = [p.lower() for p in parts[:-1]]  # exclude filename
        for vf in ["encrypted", "decrypted"]:
            if any(vf in pf for pf in parent_folders):
                version = vf.title()
                break

    # New logic: extract known version keywords from platform string (case-insensitive)
    # e.g. "nkit rvz", "nkit", "rvz"
    version_keywords = ['nkit rvz', 'nkit', 'rvz']
    platform_lower = platform_raw.lower()

    for vk in version_keywords:
        if vk in platform_lower:
            version = vk.title()
            # Remove version keyword from platform string
            pattern = re.compile(re.escape(vk), re.IGNORECASE)
            platform_raw = pattern.sub("", platform_raw).strip()
            break

    # Normalize platform name using your existing function
    platform = normalize_platform_name(platform_raw)

    # Remove duplicates from languages
    seen_langs = set()
    languages = [x.upper() for x in languages if not (x in seen_langs or seen_langs.add(x))]
    
    title_clean = re.sub(r"\s*\([^)]*\)", "", title).strip()

    output = {
        "title": title_clean,
        "platform": platform,
        "collection": collection,
        "region": region,
        "language": ",".join(languages) if languages else None,
        "version": version,
        "url": base_url + url_path.lstrip("/"),
    }

    return output

