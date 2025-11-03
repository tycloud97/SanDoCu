import csv
import os
import shutil
from datetime import datetime
from typing import Dict, Iterable, Optional, Set


# Unified CSV schema used across crawlers
# Note: Added "crawl_time" to help distinguish new vs old records
CSV_FIELDS: list[str] = [
    "id",
    "title",
    "description",
    "price",
    "location",
    "seller",
    "post_url",
    "image",
    "crawl_time",
]


def _repo_root_from_utils() -> str:
    # __file__ => backend/utils/csv_writer.py
    here = os.path.abspath(__file__)
    utils_dir = os.path.dirname(here)         # backend/utils
    backend_dir = os.path.dirname(utils_dir)  # backend
    repo_root = os.path.dirname(backend_dir)  # repo root
    return repo_root


def get_sources_dir() -> str:
    repo_root = _repo_root_from_utils()
    return os.path.join(repo_root, "frontend", "public", "data", "sources")


def ensure_sources_dir() -> str:
    d = get_sources_dir()
    os.makedirs(d, exist_ok=True)
    return d


def cleanup_old_csvs():
    """Remove legacy timestamped CSVs under backend/data/raw and frontend/public/data/sources.

    Keeps the non-timestamped destination files such as facebook_group.csv, etc.
    """
    repo_root = _repo_root_from_utils()

    # backend/data/raw/*.csv
    raw_dir = os.path.join(repo_root, "backend", "data", "raw")
    if os.path.isdir(raw_dir):
        try:
            for name in os.listdir(raw_dir):
                if name.endswith(".csv"):
                    try:
                        os.remove(os.path.join(raw_dir, name))
                    except Exception:
                        pass
        except Exception:
            pass

    # frontend/public/data/sources/*_YYYYMMDD_*.csv
    sources_dir = get_sources_dir()
    if os.path.isdir(sources_dir):
        try:
            for name in os.listdir(sources_dir):
                if name.endswith(".csv") and any(
                    part.isdigit() and len(part) == 8 for part in name.split("_")
                ):
                    # e.g. facebook_marketplace_20251103_230204.csv
                    path = os.path.join(sources_dir, name)
                    try:
                        os.remove(path)
                    except Exception:
                        pass
        except Exception:
            pass


class UnifiedCSVWriter:
    """CSV store with append + de-dup + auto crawl_time support.

    - Ensures directory exists
    - Appends to a single file; writes header on first creation
    - De-duplicates rows by `dedupe_key` (default: "id")
    - Adds `crawl_time` automatically if missing
    - Upgrades existing header to include new columns when necessary
    """

    def __init__(
        self,
        csv_path: str,
        fieldnames: Optional[Iterable[str]] = None,
        *,
        dedupe_key: Optional[str] = "id",
    ):
        self.csv_path = csv_path
        self.fieldnames = list(fieldnames) if fieldnames else CSV_FIELDS
        self.dedupe_key = dedupe_key
        self._existing_ids: Set[str] = set()

        # Ensure parent directory exists
        os.makedirs(os.path.dirname(self.csv_path), exist_ok=True)

        # If file exists, ensure header is up to date and collect existing ids
        is_new_file = not os.path.exists(self.csv_path)
        if not is_new_file:
            try:
                with open(self.csv_path, "r", newline="", encoding="utf-8-sig") as f:
                    reader = csv.DictReader(f)
                    existing_fields = [h.strip() for h in (reader.fieldnames or [])]
                    needs_header_upgrade = any(
                        fn not in existing_fields for fn in self.fieldnames
                    )
                    # Collect existing ids
                    if self.dedupe_key:
                        for row in reader:
                            val = (row.get(self.dedupe_key) or "").strip()
                            if val:
                                self._existing_ids.add(val)
                if needs_header_upgrade:
                    # Rewrite the file with new header, preserving rows
                    tmp_path = self.csv_path + ".tmp"
                    with open(self.csv_path, "r", newline="", encoding="utf-8-sig") as src, \
                         open(tmp_path, "w", newline="", encoding="utf-8-sig") as dst:
                        src_reader = csv.DictReader(src)
                        writer = csv.DictWriter(dst, fieldnames=self.fieldnames)
                        writer.writeheader()
                        for row in src_reader:
                            # Coerce to new schema; keep crawl_time if present
                            out: Dict[str, str] = {}
                            for f in self.fieldnames:
                                v = row.get(f, "")
                                out[f] = "" if v is None else str(v)
                            writer.writerow(out)
                    shutil.move(tmp_path, self.csv_path)
            except Exception:
                # If anything goes wrong, treat as new file to avoid blocking writes
                is_new_file = True

        # Open file for append and prepare writer
        mode = "w" if is_new_file else "a"
        self._fh = open(self.csv_path, mode, newline="", encoding="utf-8-sig")
        self._writer = csv.DictWriter(self._fh, fieldnames=self.fieldnames)
        if is_new_file:
            self._writer.writeheader()

    def _coerce_row(self, item: Dict) -> Dict[str, str]:
        row: Dict[str, str] = {}
        for f in self.fieldnames:
            v = item.get(f, "") if isinstance(item, dict) else ""
            # Normalize None to empty string and cast others to str only when needed
            row[f] = "" if v is None else (v if isinstance(v, str) else str(v))
        return row

    def write(self, item: Dict) -> None:
        # Ensure crawl_time exists
        if "crawl_time" not in item or not item["crawl_time"]:
            item = dict(item)
            item["crawl_time"] = datetime.now().isoformat(timespec="seconds")

        row = self._coerce_row(item)
        self._writer.writerow(row)
        self._fh.flush()

    def write_if_new(self, item: Dict) -> bool:
        """Write row if its `dedupe_key` hasn't been seen. Returns True if written."""
        if self.dedupe_key:
            key = (item.get(self.dedupe_key) or "").strip()
            if not key:
                return False
            if key in self._existing_ids:
                return False
            self._existing_ids.add(key)
        self.write(item)
        return True

    def close(self) -> None:
        try:
            self._fh.close()
        except Exception:
            pass
