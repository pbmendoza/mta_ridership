#!/usr/bin/env python3
"""Download MTA turnstile data by month via the SODA3 API.

This script downloads monthly turnstile records for all years defined in
references/turnstile_data_nyopendata.json, or for a specific year/month.

Usage:
    python scripts/local/data/update_turnstile_data.py                          # all years/months
    python scripts/local/data/update_turnstile_data.py --year 2019              # all months in 2019
    python scripts/local/data/update_turnstile_data.py --year 2019 --month 6    # June 2019 only
    python scripts/local/data/update_turnstile_data.py --force                  # overwrite existing files

Environment variables (or set in .env at repo root):
    SOCRATA_APP_TOKEN      App token for Socrata API
    SOCRATA_SECRET_TOKEN   Secret token for Socrata API (optional)
"""

from __future__ import annotations

import argparse
import calendar
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
from dataclasses import dataclass
import json
import os
import sqlite3
import sys
from threading import Lock
from datetime import date
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

# Ensure repo root is on sys.path so that ``scripts.utils`` is importable.
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import requests
from scripts.utils.socrata import (
    build_headers,
    load_socrata_token,
    load_socrata_secret_token,
    repo_root,
    request_json,
)
try:
    from rich.console import Console
    from rich.progress import (
        BarColumn,
        Progress,
        SpinnerColumn,
        TaskID,
        TaskProgressColumn,
        TextColumn,
        TimeElapsedColumn,
        TimeRemainingColumn,
    )
    RICH_AVAILABLE = True
except ImportError:
    Console = None
    Progress = None
    TaskID = int  # type: ignore[assignment]
    RICH_AVAILABLE = False

DEFAULT_PAGE_SIZE = 50000
DEFAULT_MAX_WORKERS = max(1, (os.cpu_count() or 2) - 1)
LINE_COUNT_CHUNK_SIZE = 1024 * 1024
DEFAULT_DUPLICATE_SAMPLE_LIMIT = 5
SODA_ORDER_CLAUSE = (
    "date ASC, "
    "c_a ASC, "
    "unit ASC, "
    "scp ASC, "
    "time ASC, "
    ":id ASC"
)

# Column order for CSV output.
COLUMN_ORDER = [
    "c_a",
    "unit",
    "scp",
    "station",
    "linename",
    "division",
    "date",
    "time",
    "desc",
    "entries",
    "exits",
]

PRINT_LOCK = Lock()


@dataclass(frozen=True)
class DownloadTask:
    """Represents one year/month download job."""

    year: int
    month: int
    endpoint: str


@dataclass(frozen=True)
class DownloadResult:
    """Represents the result of one year/month download job."""

    status: str
    message: str


def log(message: str) -> None:
    """Print a single log line safely across worker threads."""
    with PRINT_LOCK:
        print(message, flush=True)


class DownloadUI:
    """Thread-safe progress UI with rich and plain-text fallback."""

    def __init__(self, enable_rich: bool = True) -> None:
        self.use_rich = bool(enable_rich and RICH_AVAILABLE)
        self._lock = Lock()
        self._task_ids: Dict[Tuple[int, int], TaskID] = {}
        self.console = Console() if self.use_rich else None
        self.progress = (
            Progress(
                TextColumn("{task.description:<9}"),
                SpinnerColumn(style="cyan"),
                BarColumn(bar_width=24),
                TaskProgressColumn(),
                TextColumn("{task.completed:>12,.0f}/{task.total:>12,.0f}", justify="right"),
                TimeElapsedColumn(),
                TimeRemainingColumn(),
                TextColumn("{task.fields[status]}", justify="left"),
                console=self.console,
                transient=False,
            )
            if self.use_rich
            else None
        )

    def __enter__(self) -> "DownloadUI":
        if self.progress is not None:
            self.progress.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self.progress is not None:
            self.progress.stop()

    def _key(self, task: DownloadTask) -> Tuple[int, int]:
        return (task.year, task.month)

    def add_tasks(self, tasks: List[DownloadTask]) -> None:
        if self.progress is None:
            return
        with self._lock:
            for task in tasks:
                label = f"{task.year}/{task.month:02d}"
                task_id = self.progress.add_task(
                    description=label,
                    total=100,
                    completed=0,
                    status="queued",
                )
                self._task_ids[self._key(task)] = task_id

    def _update(self, task: DownloadTask, **kwargs) -> None:
        if self.progress is None:
            return
        task_id = self._task_ids.get(self._key(task))
        if task_id is None:
            return
        with self._lock:
            self.progress.update(task_id, **kwargs)

    def set_status(self, task: DownloadTask, status: str) -> None:
        self._update(task, status=status)

    def set_total(self, task: DownloadTask, total_rows: int) -> None:
        self._update(task, total=max(total_rows, 1), completed=0)

    def set_progress(self, task: DownloadTask, completed: int, total_rows: int) -> None:
        self._update(task, total=max(total_rows, 1), completed=completed)

    def mark_terminal(self, task: DownloadTask, status: str, total_rows: Optional[int] = None) -> None:
        updates: Dict[str, object] = {"status": status}
        if total_rows is not None:
            updates["total"] = max(total_rows, 1)
            updates["completed"] = max(total_rows, 1)
        self._update(task, **updates)

    def log(self, message: str) -> None:
        if self.progress is None:
            log(message)
            return
        with self._lock:
            self.progress.console.print(message)


def positive_int(value: str) -> int:
    """argparse type for positive integers."""
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("Value must be >= 1.")
    return parsed


def load_dataset_ids() -> Dict[str, str]:
    """Load year-to-dataset-id mapping from JSON config."""
    config_path = repo_root() / "references" / "turnstile_data_nyopendata.json"
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_soda_endpoint(dataset_id: str) -> str:
    """Build SODA3 endpoint URL for a dataset."""
    return f"https://data.ny.gov/resource/{dataset_id}.json"


def get_output_path(year: int, month: int) -> Path:
    """Return output path: data/raw/turnstile/{year}/{month}.csv"""
    return repo_root() / "data" / "raw" / "turnstile" / str(year) / f"{month}.csv"


def get_last_day_of_month(year: int, month: int) -> int:
    """Return the last day of the given month."""
    return calendar.monthrange(year, month)[1]


def is_future_month(year: int, month: int) -> bool:
    """Check if the given year/month is in the future."""
    today = date.today()
    return date(year, month, 1) > date(today.year, today.month, 1)


def compute_date_range(year: int, month: int) -> Tuple[str, str]:
    """Compute inclusive start and exclusive end ISO timestamps for a month."""
    start = date(year, month, 1)
    if month == 12:
        end = date(year + 1, 1, 1)
    else:
        end = date(year, month + 1, 1)
    return start.isoformat() + "T00:00:00", end.isoformat() + "T00:00:00"


def get_month_where_clause(year: int, month: int) -> str:
    """Build SoQL where clause for one month."""
    start_ts, end_ts = compute_date_range(year, month)
    return f"date >= '{start_ts}' AND date < '{end_ts}'"


def count_rows(
    session: requests.Session,
    endpoint: str,
    headers: Dict[str, str],
    where_clause: str,
) -> int:
    """Get total row count for a query."""
    params = {"$select": "count(1)", "$where": where_clause}
    rows = request_json(session, endpoint, params, headers)
    if not rows:
        return 0
    try:
        first = rows[0]
        count_value = next(iter(first.values())) if len(first) == 1 else first.get("count") or first.get("count_1")
        return int(count_value)
    except (KeyError, ValueError, TypeError):
        return 0


def check_first_day_has_data(
    session: requests.Session,
    endpoint: str,
    headers: Dict[str, str],
    year: int,
    month: int,
) -> bool:
    """Check if there is data for the first day of the month."""
    first_day_start = f"{year}-{month:02d}-01T00:00:00"
    first_day_end = f"{year}-{month:02d}-01T23:59:59"
    where_clause = f"date >= '{first_day_start}' AND date <= '{first_day_end}'"
    count = count_rows(session, endpoint, headers, where_clause)
    return count > 0


def check_last_day_has_data(
    session: requests.Session,
    endpoint: str,
    headers: Dict[str, str],
    year: int,
    month: int,
) -> bool:
    """Check if there is data for the last day of the month."""
    last_day = get_last_day_of_month(year, month)
    last_day_start = f"{year}-{month:02d}-{last_day:02d}T00:00:00"
    last_day_end = f"{year}-{month:02d}-{last_day:02d}T23:59:59"
    where_clause = f"date >= '{last_day_start}' AND date <= '{last_day_end}'"
    count = count_rows(session, endpoint, headers, where_clause)
    return count > 0


def count_unique_days(
    session: requests.Session,
    endpoint: str,
    headers: Dict[str, str],
    year: int,
    month: int,
) -> int:
    """Count unique days with data in the given month."""
    start_ts, end_ts = compute_date_range(year, month)
    where_clause = f"date >= '{start_ts}' AND date < '{end_ts}'"
    params = {
        "$select": "date_trunc_ymd(date) AS day",
        "$where": where_clause,
        "$group": "day",
    }
    rows = request_json(session, endpoint, params, headers)
    return len(rows)


def merge_header(seen_rows: List[Dict[str, str]]) -> List[str]:
    """Build CSV header from seen rows, respecting column order."""
    header: List[str] = []
    for column in COLUMN_ORDER:
        if any(column in row for row in seen_rows):
            header.append(column)
    for row in seen_rows:
        for key in row.keys():
            if key.startswith(":") or key in header:
                continue
            header.append(key)
    return header


def normalize_row(row: Dict[str, str], header: List[str]) -> Dict[str, str]:
    """Clean row data for CSV output."""
    cleaned: Dict[str, str] = {}
    for key in header:
        value = row.get(key, "")
        if isinstance(value, (dict, list)):
            cleaned[key] = json.dumps(value, separators=(",", ":"))
        else:
            cleaned[key] = value
    return cleaned


def count_csv_data_rows(csv_path: Path) -> int:
    """Count data rows in a CSV file without loading the file into memory."""
    newline_count = 0
    saw_any_bytes = False
    last_byte = b""

    with csv_path.open("rb") as handle:
        while True:
            chunk = handle.read(LINE_COUNT_CHUNK_SIZE)
            if not chunk:
                break
            saw_any_bytes = True
            newline_count += chunk.count(b"\n")
            last_byte = chunk[-1:]

    if not saw_any_bytes:
        return 0

    # If the file doesn't end with a newline, include the trailing line.
    if last_byte != b"\n":
        newline_count += 1

    # Subtract one line for CSV header.
    return max(0, newline_count - 1)


def inspect_duplicate_rows(
    csv_path: Path,
    sample_limit: int = DEFAULT_DUPLICATE_SAMPLE_LIMIT,
) -> Tuple[int, int, List[Dict[str, str]]]:
    """Inspect exact duplicate CSV rows using disk-backed SQLite.

    Returns:
        Tuple of (rows_scanned, duplicate_row_count, duplicate_row_samples).
    """
    sqlite_path = csv_path.with_suffix(csv_path.suffix + ".dupecheck.sqlite")
    safe_unlink(sqlite_path)

    total_rows = 0
    duplicate_rows = 0
    duplicate_samples: List[Dict[str, str]] = []

    try:
        with csv_path.open("r", encoding="utf-8", newline="") as source:
            reader = csv.reader(source)
            header = next(reader, None)
            if header is None:
                return 0, 0, []

            column_names = [f"c{i}" for i in range(len(header))]
            column_sql = ", ".join(column_names)
            placeholder_sql = ", ".join(["?"] * len(column_names))
            table_sql = (
                "CREATE TABLE rows ("
                "seq INTEGER PRIMARY KEY AUTOINCREMENT, "
                + ", ".join(f"{name} TEXT" for name in column_names)
                + ", seen_count INTEGER NOT NULL DEFAULT 1, "
                + f"UNIQUE ({column_sql})"
                + ")"
            )
            upsert_sql = (
                f"INSERT INTO rows ({column_sql}, seen_count) VALUES ({placeholder_sql}, 1) "
                f"ON CONFLICT ({column_sql}) DO UPDATE SET seen_count = seen_count + 1"
            )
            duplicate_count_sql = "SELECT COALESCE(SUM(seen_count - 1), 0) FROM rows"
            sample_sql = (
                f"SELECT {column_sql} FROM rows WHERE seen_count > 1 "
                "ORDER BY seq ASC LIMIT ?"
            )

            conn = sqlite3.connect(str(sqlite_path))
            try:
                conn.execute("PRAGMA journal_mode=OFF")
                conn.execute("PRAGMA synchronous=OFF")
                conn.execute("PRAGMA temp_store=FILE")
                conn.execute(table_sql)

                for row in reader:
                    total_rows += 1
                    if len(row) < len(column_names):
                        row = row + [""] * (len(column_names) - len(row))
                    elif len(row) > len(column_names):
                        row = row[: len(column_names)]
                    conn.execute(upsert_sql, row)

                conn.commit()
                duplicate_rows = int(conn.execute(duplicate_count_sql).fetchone()[0])
                if sample_limit > 0 and duplicate_rows > 0:
                    for db_row in conn.execute(sample_sql, (sample_limit,)):
                        duplicate_samples.append(
                            {
                                header[index]: (db_row[index] if db_row[index] is not None else "")
                                for index in range(len(header))
                            }
                        )
            finally:
                conn.close()

        return total_rows, duplicate_rows, duplicate_samples
    finally:
        safe_unlink(sqlite_path)


def download_month(
    session: requests.Session,
    endpoint: str,
    headers: Dict[str, str],
    where_clause: str,
    expected_rows: int,
    output_path: Path,
    page_size: int,
    progress_callback: Optional[Callable[[int, int], None]] = None,
) -> Tuple[bool, int]:
    """Download data for a single month. Returns (success, row_count)."""
    if expected_rows <= 0:
        return False, 0

    output_path.parent.mkdir(parents=True, exist_ok=True)

    offset = 0
    header: List[str] = []
    written = 0

    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer: Optional[csv.DictWriter] = None
        while True:
            params = {
                "$where": where_clause,
                "$order": SODA_ORDER_CLAUSE,
                "$limit": str(page_size),
                "$offset": str(offset),
            }
            rows = request_json(session, endpoint, params, headers)
            if not rows:
                break

            if not header:
                header = merge_header(rows)
                writer = csv.DictWriter(handle, fieldnames=header)
                writer.writeheader()

            for row in rows:
                if "date" not in row:
                    continue
                writer.writerow(normalize_row(row, header))
                written += 1

            offset += len(rows)
            if progress_callback is not None:
                progress_callback(written, expected_rows)
            if len(rows) < page_size:
                break

    if progress_callback is not None:
        progress_callback(written, expected_rows)
    return True, written


def safe_unlink(path: Path) -> None:
    """Remove file if present."""
    if path.exists():
        path.unlink()


def process_task(
    task: DownloadTask,
    headers: Dict[str, str],
    page_size: int,
    force: bool,
    verify_duplicates: bool,
    ui: DownloadUI,
) -> DownloadResult:
    """Process one monthly download task with robust completeness checks."""
    year, month = task.year, task.month
    month_label = f"{year}/{month:02d}"
    output_path = get_output_path(year, month)
    temp_path = output_path.with_suffix(output_path.suffix + ".tmp")
    where_clause = get_month_where_clause(year, month)

    session = requests.Session()
    try:
        ui.set_status(task, "counting rows")
        total_rows = count_rows(session, task.endpoint, headers, where_clause)
        if total_rows <= 0:
            safe_unlink(temp_path)
            safe_unlink(output_path)
            ui.mark_terminal(task, "no data", total_rows=1)
            return DownloadResult("incomplete", f"⚠️  {month_label}: No data found (removed local file if present).")

        ui.set_total(task, total_rows)

        # Completeness heuristics: these checks catch common gaps (e.g. missing
        # first/last day or absent days mid-month) but cannot guarantee the data
        # is fully correct — rows within a covered day may still be missing.
        ui.set_status(task, "checking completeness")
        has_first_day_data = check_first_day_has_data(session, task.endpoint, headers, year, month)
        has_last_day_data = check_last_day_has_data(session, task.endpoint, headers, year, month)
        expected_days = get_last_day_of_month(year, month)
        unique_days = count_unique_days(session, task.endpoint, headers, year, month)

        completeness_issues: List[str] = []
        if not has_first_day_data:
            completeness_issues.append("no data on first day")
        if not has_last_day_data:
            completeness_issues.append("no data on last day")
        if unique_days != expected_days:
            completeness_issues.append(f"{unique_days}/{expected_days} days covered")

        if completeness_issues:
            detail = "; ".join(completeness_issues)
            safe_unlink(temp_path)
            safe_unlink(output_path)
            ui.mark_terminal(task, "incomplete", total_rows=total_rows)
            return DownloadResult(
                "incomplete",
                f"⚠️  {month_label}: Incomplete ({detail}); removed local file if present.",
            )

        if output_path.exists() and not force:
            ui.set_status(task, "validating existing")
            local_rows = count_csv_data_rows(output_path)
            if local_rows == total_rows:
                ui.mark_terminal(task, "skipped", total_rows=total_rows)
                return DownloadResult(
                    "skipped",
                    f"⏭️  {month_label}: File complete and row-count matched ({local_rows:,}), skipping.",
                )
            ui.log(
                f"♻️  {month_label}: Existing row-count mismatch "
                f"(local {local_rows:,} vs API {total_rows:,}), re-downloading..."
            )
        elif force and output_path.exists():
            ui.log(f"♻️  {month_label}: --force enabled, re-downloading...")
        else:
            ui.set_status(task, "downloading")

        ui.set_status(task, "downloading")
        safe_unlink(temp_path)
        success, row_count = download_month(
            session=session,
            endpoint=task.endpoint,
            headers=headers,
            where_clause=where_clause,
            expected_rows=total_rows,
            output_path=temp_path,
            page_size=page_size,
            progress_callback=lambda written, expected: ui.set_progress(task, written, expected),
        )

        if not success or row_count == 0:
            safe_unlink(temp_path)
            safe_unlink(output_path)
            ui.mark_terminal(task, "empty", total_rows=total_rows)
            return DownloadResult("incomplete", f"⚠️  {month_label}: No rows written; removed local file.")

        if row_count != total_rows:
            safe_unlink(temp_path)
            ui.mark_terminal(task, "error", total_rows=total_rows)
            return DownloadResult(
                "errors",
                f"❌ {month_label}: Downloaded row mismatch (written {row_count:,} vs expected {total_rows:,}).",
            )

        if verify_duplicates:
            ui.set_status(task, "verifying duplicates")
            rows_scanned, duplicate_rows, duplicate_samples = inspect_duplicate_rows(temp_path)
            if rows_scanned != row_count:
                safe_unlink(temp_path)
                ui.mark_terminal(task, "error", total_rows=total_rows)
                return DownloadResult(
                    "errors",
                    (
                        f"❌ {month_label}: On-disk row mismatch during duplicate check "
                        f"(disk {rows_scanned:,} vs written {row_count:,})."
                    ),
                )

            if duplicate_rows > 0:
                ui.log(
                    f"🔎 {month_label}: Found {duplicate_rows:,} exact duplicate row(s) "
                    "(diagnostic only; file is unchanged)."
                )
                for index, sample in enumerate(duplicate_samples, start=1):
                    ui.log(
                        f"🔎 {month_label}: Duplicate sample {index}/{len(duplicate_samples)} "
                        f"{json.dumps(sample, ensure_ascii=True, separators=(',', ':'))}"
                    )
            else:
                ui.log(f"🔎 {month_label}: No exact duplicate rows found.")

        ui.set_status(task, "validating")

        output_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path.replace(output_path)
        ui.mark_terminal(task, "done", total_rows=total_rows)
        return DownloadResult(
            "downloaded",
            f"✅ {month_label}: Saved {row_count:,} rows to {output_path.relative_to(repo_root())}",
        )
    except Exception as exc:
        safe_unlink(temp_path)
        ui.mark_terminal(task, "error", total_rows=1)
        return DownloadResult("errors", f"❌ {month_label}: {exc}")
    finally:
        session.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download MTA turnstile data by month via SODA3 API.",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=None,
        help="Year to download (e.g., 2019). If omitted, downloads all years in config.",
    )
    parser.add_argument(
        "--month",
        type=int,
        default=None,
        help="Month to download (1-12). If omitted, downloads all months 1-12.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing files instead of skipping.",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=DEFAULT_PAGE_SIZE,
        help=f"Rows per page for SODA pagination (default: {DEFAULT_PAGE_SIZE}).",
    )
    parser.add_argument(
        "--max-workers",
        type=positive_int,
        default=DEFAULT_MAX_WORKERS,
        help=(
            "Maximum concurrent month downloads. "
            f"Default: cpu_count - 1 ({DEFAULT_MAX_WORKERS})."
        ),
    )
    parser.add_argument(
        "--verify-duplicates",
        action="store_true",
        help=(
            "Inspect downloaded CSVs for exact duplicate rows and print diagnostic "
            "samples without modifying files."
        ),
    )
    parser.add_argument(
        "--app-token",
        default=None,
        help="Socrata app token (default: from .env or SOCRATA_APP_TOKEN env var).",
    )
    parser.add_argument(
        "--secret-token",
        default=None,
        help="Socrata secret token (default: from .env or SOCRATA_SECRET_TOKEN env var).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    app_token = args.app_token if args.app_token is not None else load_socrata_token()
    secret_token = args.secret_token if args.secret_token is not None else load_socrata_secret_token()
    if not app_token:
        print(
            "⚠️  No Socrata app token found. Running without authentication "
            "(lower rate limits apply). Set SOCRATA_APP_TOKEN in .env, as an "
            "env var, or use --app-token. See references/docs/socrata_api_setup.md."
        )

    dataset_ids = load_dataset_ids()
    headers = build_headers(app_token, secret_token)

    # Determine years to process
    if args.year:
        years = [args.year]
    else:
        years = [int(y) for y in dataset_ids.keys()]

    # Determine months to process
    if args.month:
        months = [args.month]
    else:
        months = list(range(1, 13))

    stats = {"downloaded": 0, "skipped": 0, "incomplete": 0, "errors": 0}
    tasks: List[DownloadTask] = []
    ui = DownloadUI(enable_rich=True)

    if not ui.use_rich:
        log("⚠️  rich is not installed. Install with: pip install rich")

    for year in sorted(years):
        year_str = str(year)
        if year_str not in dataset_ids:
            ui.log(f"⚠️  Year {year} not found in dataset config, skipping.")
            continue

        dataset_id = dataset_ids[year_str]
        endpoint = get_soda_endpoint(dataset_id)

        for month in months:
            if is_future_month(year, month):
                ui.log(f"⏭️  {year}/{month:02d}: Future month, skipping.")
                stats["skipped"] += 1
                continue
            tasks.append(DownloadTask(year=year, month=month, endpoint=endpoint))

    if tasks:
        max_workers = min(args.max_workers, len(tasks))
        with ui:
            ui.add_tasks(tasks)
            ui.log(f"🧵 Running {len(tasks)} task(s) with max_workers={max_workers}")
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [
                    executor.submit(
                        process_task,
                        task,
                        headers,
                        args.page_size,
                        args.force,
                        args.verify_duplicates,
                        ui,
                    )
                    for task in tasks
                ]
                for future in as_completed(futures):
                    result = future.result()
                    if result.status not in stats:
                        stats["errors"] += 1
                    else:
                        stats[result.status] += 1
                    if not ui.use_rich or result.status in {"errors", "incomplete"}:
                        ui.log(result.message)

    # Print summary
    print()
    print("📊 Summary:")
    print(f"   Downloaded: {stats['downloaded']}")
    print(f"   Skipped:    {stats['skipped']}")
    print(f"   Incomplete: {stats['incomplete']}")
    print(f"   Errors:     {stats['errors']}")

    return 0 if stats["errors"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
