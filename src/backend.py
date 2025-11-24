"""Database backend for Myrient Search App."""
import re
import sqlite3
from pathlib import Path


class MyrientBackend:
    """Database backend for Myrient Search App."""

    def __init__(self, db_file:str|Path) -> None:
        """Initialize the backend with a SQLite database file."""
        self.db_file = Path(db_file)
        self.QUERY_MAP = {
            "platform":
                "SELECT DISTINCT platform FROM files WHERE platform IS NOT NULL",
            "region":
                "SELECT DISTINCT region FROM files WHERE region IS NOT NULL",
            "version":
                "SELECT DISTINCT version FROM files WHERE version IS NOT NULL",
            "language":
                "SELECT DISTINCT language FROM files WHERE language IS NOT NULL",
            "size":
                "SELECT DISTINCT size FROM files WHERE size IS NOT NULL",
            }


    # Helper function to open DB connection
    def get_conn(self) -> sqlite3.Connection:
        """Open a SQLite connection with REGEXP support."""
        conn = sqlite3.connect(str(self.db_file))
        conn.row_factory = sqlite3.Row

        def regexp(pattern:str, text:str) -> int:
            try:
                if not pattern or text is None:
                    return 0
                return 1 if re.search(pattern, str(text), re.IGNORECASE) else 0
            except re.error:
                return 0

        conn.create_function("REGEXP", 2, regexp)
        return conn


    def _fetch_distinct(self, field: str, filters: dict | None = None) -> list[str]:
        if field not in self.QUERY_MAP:
            error = f"Invalid field: {field}"
            raise ValueError(error)

        base_query = self.QUERY_MAP[field]
        params: list = []

        if filters:
            base_query, params = self._apply_text_filters(
                base_query, params, filters)
            if "platform" in filters and field != "platform" and filters["platform"]:
                base_query += " AND platform = ?"
                params.append(filters["platform"])
            if "region" in filters and field != "region" and filters["region"]:
                base_query += " AND region = ?"
                params.append(filters["region"])
            if "version" in filters and field != "version" and filters["version"]:
                base_query += " AND version = ?"
                params.append(filters["version"])
            if "language" in filters and field != "language" and filters["language"]:
                base_query += " AND ',' || language || ',' LIKE ?"
                params.append(f"%,{filters['language']},%")

        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute(base_query, params)
        values = [row[0] for row in cur.fetchall() if row[0]]
        conn.close()

        if field == "language":
            values = sorted(
                {lang.strip() for entry in values for lang in entry.split(",")},
                )
        else:
            values = sorted(values)

        return values

    def _fetch_distinct_size_ranges(self, filters: dict | None = None) -> list[str]:
        """Fetch and categorize distinct sizes into ranges."""
        sizes = self._fetch_distinct("size", filters)
        ranges = {
            "0-100MiB": 0, "100-500MiB": 0, "500MiB-1GiB": 0, "1-5GiB": 0, "5GiB+": 0,
        }
        for size_str in sizes:
            try:
                num_str, unit = size_str.split()
                num = float(num_str)
                if "G" in unit:
                    num *= 1024
                elif "K" in unit:
                    num /= 1024

                if num < 100:  # noqa: PLR2004
                    ranges["0-100MiB"] += 1
                elif num < 500:  # noqa: PLR2004
                    ranges["100-500MiB"] += 1
                elif num < 1024:  # noqa: PLR2004
                    ranges["500MiB-1GiB"] += 1
                elif num < 5120:  # noqa: PLR2004
                    ranges["1-5GiB"] += 1
                else:
                    ranges["5GiB+"] += 1
            except (ValueError, IndexError):
                continue

        return [r for r, count in ranges.items() if count > 0]

    def _parse_size_range(self, size_range: str) -> tuple[int, int]:
        """Parse size range string into min and max bytes."""
        range_map = {
            "0-100MiB": (0, 100 * 1024 * 1024),
            "100-500MiB": (100 * 1024 * 1024, 500 * 1024 * 1024),
            "500MiB-1GiB": (500 * 1024 * 1024, 1 * 1024 * 1024 * 1024),
            "1-5GiB": (1 * 1024 * 1024 * 1024, 5 * 1024 * 1024 * 1024),
            "5GiB+": (5 * 1024 * 1024 * 1024, float("inf")),
        }
        return range_map.get(size_range, (0, float("inf")))

    # Helper functions to build query
    def _apply_text_filters(
            self,
            base_query:str,
            params:list,
            search:dict,
            ) -> tuple[str, list]:

        if search["title_contains"] and not search["title_regex"]:
            base_query += " AND title LIKE ?"
            params.append(f"%{search["title_contains"]}%")

        if search["title_regex"]:
            base_query += " AND title REGEXP ?"
            params.append(search["title_contains"])

        return base_query, params

    def _apply_main_filters(
            self,
            query:str,
            params:list,
            search:dict,
            ) -> tuple[str, list]:

        if search["platform"]:
            query += " AND platform = ?"
            params.append(search["platform"])

        if search["region"]:
            query += " AND region = ?"
            params.append(search["region"])

        if search["version"]:
            query += " AND version = ?"
            params.append(search["version"])

        if search["language"]:
            query += " AND ',' || language || ',' LIKE ?"
            params.append(f"%,{search["language"]},%")

        return query, params

    def _apply_size_filter(self, query:str, params:list, search:dict)->tuple[str, list]:
        """Apply size range filter to the query."""
        if search.get("size_range"):
            min_size, max_size = self._parse_size_range(search["size_range"])
            # Placeholder for future implementation.
        return query, params


    # Main search function
    def advanced_search(
            self,
            search:dict,
            limit:int|None=100,
            offset:int|None=0,
            sort_by: str = "title",
            sort_order: str = "ASC",
            ) -> tuple[list[sqlite3.Row], list[str], list[str], list[str], list[str]]:
        """Perform advanced search with multiple filters."""
        conn = self.get_conn()
        cur = conn.cursor()

        base_query = "FROM files WHERE 1=1"
        base_query += " AND title NOT GLOB '[0-9A-Fa-f]{16}'"
        params:list[any] = []

        base_query, params = self._apply_text_filters(
            base_query, params, search)

        main_query = "SELECT * " + base_query
        main_params = list(params)
        main_query, main_params = self._apply_main_filters(
            main_query, main_params, search)

        # Add sorting
        if sort_by == "size":
            # Custom sorting for size stored as text e.g., "123.45 MB"
            order_clause = (
                "ORDER BY CAST(SUBSTR(size, 1, INSTR(size, ' ') - 1) AS REAL) * "
                "CASE SUBSTR(TRIM(size), INSTR(TRIM(size), ' ') + 1) "
                "WHEN 'GiB' THEN 1073741824 "  # 1024*1024*1024
                "WHEN 'MiB' THEN 1048576 "     # 1024*1024
                "WHEN 'KiB' THEN 1024 "
                f"ELSE 1 END {sort_order}"
            )
        else:
            # Basic alphanumeric sort for other columns
            order_clause = f"ORDER BY {sort_by} COLLATE NOCASE {sort_order}"

        main_query += f" {order_clause} LIMIT ? OFFSET ?"
        main_params.extend([limit, offset])

        cur.execute(main_query, main_params)
        results = cur.fetchall()

        # Fetch values for filters
        platforms = self._fetch_distinct("platform", search)
        regions = self._fetch_distinct("region", search)
        single_languages = self._fetch_distinct("language", search)
        versions = self._fetch_distinct("version", search)
        size_ranges = self._fetch_distinct_size_ranges(search)

        conn.close()

        return results, platforms, regions, single_languages, versions, size_ranges

    # Convenience search helpers

    def list_platforms(self) -> list[str]:
        """Return all platforms in database."""
        return self._fetch_distinct("platform")

    def list_regions(self) -> list[str]:
        """Return all regions in database."""
        return self._fetch_distinct("region")

    def list_languages(self) -> list[str]:
        """Return all languages in database."""
        return self._fetch_distinct("language")

    def list_versions(self) -> list[str]:
        """Return all versions in database."""
        return self._fetch_distinct("version")

    def list_size_ranges(self) -> list[str]:
        """Return all size ranges in database."""
        return self._fetch_distinct_size_ranges()



