import sqlite3
import typing
from typing import List, Optional, Dict

class MyrientBackend:
    def __init__(self, DB_FILE):
        self.DB_FILE = DB_FILE

    # -------------------------
    # Helper: open DB connection
    # -------------------------
    def get_conn(self):
        conn = sqlite3.connect(self.DB_FILE)
        conn.row_factory = sqlite3.Row
        return conn

    # -------------------------
    # Advanced search interface
    # -------------------------
    def advanced_search(self, platform=None, region=None, language=None, version=None,
                        title_contains=None, title_regex=None, ext=None,
                        min_size=None, max_size=None,
                        modified_after=None, modified_before=None,
                        limit=100, offset=0):
        conn = self.get_conn()
        cur = conn.cursor()

        base_query = "FROM files WHERE 1=1"

    # Exclude exact 16-character hex titles (e.g., 0005000010105A00)
        base_query += " AND title NOT GLOB '[0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f]" \
                    "[0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f]" \
                    "[0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f]" \
                    "[0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f]'"

        params = []

        # Exclude exact 16-character hex titles
        base_query += " AND title NOT GLOB '[0-9A-Fa-f]{16}'"

        if title_contains and not title_regex:
            base_query += " AND title LIKE ?"
            params.append(f"%{title_contains}%")
        if ext:
            ext = ext.lower().lstrip(".")
            base_query += " AND LOWER(url) LIKE ?"
            params.append(f"%.{ext}")
        if min_size is not None:
            base_query += " AND size >= ?"
            params.append(min_size)
        if max_size is not None:
            base_query += " AND size <= ?"
            params.append(max_size)
        if modified_after:
            base_query += " AND DATE(last_modified) >= DATE(?)"
            params.append(modified_after)
        if modified_before:
            base_query += " AND DATE(last_modified) <= DATE(?)"
            params.append(modified_before)

        # Apply main search filters
        main_query = "SELECT * " + base_query
        main_params = list(params)
        if platform:
            main_query += " AND platform = ?"
            main_params.append(platform)
        if region:
            main_query += " AND region = ?"
            main_params.append(region)
        if version:
            main_query += " AND version = ?"
            main_params.append(version)
        if language:
            main_query += " AND ',' || language || ',' LIKE ?"
            main_params.append(f"%,{language},%")
        main_query += " ORDER BY title LIMIT ? OFFSET ?"
        main_params.extend([limit, offset])

        cur.execute(main_query, main_params)
        results = cur.fetchall()

        # Helper to get context-aware distinct values
        def get_distinct(field, exclude_value=None):
            q = f"SELECT DISTINCT {field} " + base_query + f" AND {field} IS NOT NULL"
            p = list(params)
            # Apply all filters except the one being fetched
            if field != "platform" and platform:
                q += " AND platform = ?"
                p.append(platform)
            if field != "region" and region:
                q += " AND region = ?"
                p.append(region)
            if field != "version" and version:
                q += " AND version = ?"
                p.append(version)
            if field != "language" and language:
                q += " AND ',' || language || ',' LIKE ?"
                p.append(f"%,{language},%")
            cur.execute(q, p)
            values = [row[0] for row in cur.fetchall() if row[0]]
            if field == "language":
                # Split comma-separated languages
                values = sorted({lang.strip() for entry in values for lang in entry.split(',')})
            return values

        platforms = get_distinct("platform")
        regions = get_distinct("region")
        single_languages = get_distinct("language")
        versions = get_distinct("version")

        conn.close()

        return results, platforms, regions, single_languages, versions



    # -------------------------
    # Convenience search helpers
    # -------------------------
    def list_platforms(self) -> List[str]:
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT platform FROM files WHERE platform IS NOT NULL ORDER BY platform")
        platforms = [r[0] for r in cur.fetchall()]
        conn.close()
        return platforms

    def list_regions(self) -> List[str]:
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT region FROM files WHERE region IS NOT NULL ORDER BY region")
        regions = [r[0] for r in cur.fetchall()]
        conn.close()
        return regions

    def list_languages(self) -> List[str]:
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT language FROM files WHERE language IS NOT NULL ORDER BY language")
        langs = [r[0] for r in cur.fetchall()]
        conn.close()
        return langs
    
    def list_versions(self) -> List[str]:
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT version FROM files WHERE version IS NOT NULL ORDER BY version")
        vers = [r[0] for r in cur.fetchall()]
        conn.close()
        return vers

    # -------------------------
    # Utility: convert rows to urls for queue
    # -------------------------
    def urls_from_rows(self, rows: List[Dict]) -> List[str]:
        return [r["url"] for r in rows if r.get("url")]

    # -------------------------
    # Optional: export result rows to CSV for offline review
    # -------------------------
    def export_to_csv(self, rows: List[Dict], path: str):
        import csv
        keys = ["title","platform","region","language","version","size","url","last_modified"]
        with open(path, "w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=keys)
            w.writeheader()
            for r in rows:
                w.writerow({k: r.get(k, "") for k in keys})


