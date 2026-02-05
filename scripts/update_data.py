#!/usr/bin/env python3
"""Download MTA ridership data by month via the SODA3 API.

This script downloads monthly ridership records for all years defined in
references/dataset_id_on_nyopendata.json, or for a specific year/month.

Usage:
    python scripts/update_data.py                          # all years/months
    python scripts/update_data.py --year 2025              # all months in 2025
    python scripts/update_data.py --year 2025 --month 6    # June 2025 only
    python scripts/update_data.py --force                  # overwrite existing files

Environment variables:
    SOCRATA_APP_TOKEN      App token for Socrata API
    SOCRATA_SECRET_TOKEN   Secret token for Socrata API
"""

from __future__ import annotations

import argparse
import calendar
import csv
import json
import os
import sys
import time
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests

# Default credentials (can be overridden via env/CLI).
DEFAULT_APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN", "PSOinhnLdpy9yMfRdcYzUsJNy")
DEFAULT_SECRET_TOKEN = os.getenv("SOCRATA_SECRET_TOKEN", "4MWd4bOFT-e8fv7YrXh427YfHSrblNqUSX7m")

DEFAULT_PAGE_SIZE = 50000
REQUEST_TIMEOUT = 60
MAX_RETRIES = 5

# Column order for CSV output.
COLUMN_ORDER = [
    "transit_timestamp",
    "transit_mode",
    "station_complex_id",
    "station_complex",
    "borough",
    "payment_method",
    "fare_class_category",
    "ridership",
    "transfers",
    "latitude",
    "longitude",
    "georeference",
]


def repo_root() -> Path:
    """Return the repository root directory."""
    return Path(__file__).resolve().parents[1]


def load_dataset_ids() -> Dict[str, str]:
    """Load year-to-dataset-id mapping from JSON config."""
    config_path = repo_root() / "references" / "dataset_id_on_nyopendata.json"
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_soda_endpoint(dataset_id: str) -> str:
    """Build SODA3 endpoint URL for a dataset."""
    return f"https://data.ny.gov/resource/{dataset_id}.json"


def get_output_path(year: int, month: int) -> Path:
    """Return output path: data/raw/ridership/{year}/{month}.csv"""
    return repo_root() / "data" / "raw" / "ridership" / str(year) / f"{month}.csv"


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


def build_headers(app_token: str, secret_token: str) -> Dict[str, str]:
    """Build request headers with authentication."""
    headers = {
        "Accept": "application/json",
        "X-App-Token": app_token,
    }
    if secret_token:
        headers["X-App-Token-Secret"] = secret_token
    return headers


def request_json(
    session: requests.Session,
    endpoint: str,
    params: Dict[str, str],
    headers: Dict[str, str],
) -> List[Dict[str, str]]:
    """Make a request with retry logic."""
    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            response = session.get(endpoint, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
        except requests.RequestException as exc:
            attempt += 1
            time.sleep(min(2**attempt, 60))
            if attempt >= MAX_RETRIES:
                raise RuntimeError("Request failed after retries") from exc
            continue

        if response.status_code == 429:
            attempt += 1
            time.sleep(min(2**attempt, 60))
            continue

        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, dict) and payload.get("code") == "not_found":
            raise RuntimeError(f"SODA3 endpoint reported not_found: {payload}")
        if not isinstance(payload, list):
            raise RuntimeError(f"Unexpected payload shape: {payload!r}")
        return payload

    return []


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
    where_clause = f"transit_timestamp >= '{last_day_start}' AND transit_timestamp <= '{last_day_end}'"
    count = count_rows(session, endpoint, headers, where_clause)
    return count > 0


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


def download_month(
    session: requests.Session,
    endpoint: str,
    headers: Dict[str, str],
    year: int,
    month: int,
    output_path: Path,
    page_size: int,
) -> Tuple[bool, int]:
    """Download data for a single month. Returns (success, row_count)."""
    start_ts, end_ts = compute_date_range(year, month)
    where_clause = f"transit_timestamp >= '{start_ts}' AND transit_timestamp < '{end_ts}'"

    total_rows = count_rows(session, endpoint, headers, where_clause)
    if total_rows <= 0:
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
                "$order": "transit_timestamp ASC, station_complex_id ASC, payment_method ASC, fare_class_category ASC",
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
                if "transit_timestamp" not in row:
                    continue
                writer.writerow(normalize_row(row, header))
                written += 1
                progress = min(100.0, (written / total_rows) * 100)
                print(f"\r   Progress: {written}/{total_rows} ({progress:5.1f}%)", end="", flush=True)

            offset += len(rows)
            if len(rows) < page_size:
                break

    print()  # Finish progress line
    return True, written


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download MTA ridership data by month via SODA3 API.",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=None,
        help="Year to download (e.g., 2025). If omitted, downloads all years in config.",
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
        "--app-token",
        default=DEFAULT_APP_TOKEN,
        help="Socrata app token (overrides env).",
    )
    parser.add_argument(
        "--secret-token",
        default=DEFAULT_SECRET_TOKEN,
        help="Socrata secret token (overrides env).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if not args.app_token:
        print("❌ App token is required (set SOCRATA_APP_TOKEN or use --app-token)")
        return 1

    dataset_ids = load_dataset_ids()
    headers = build_headers(args.app_token, args.secret_token)

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

    session = requests.Session()
    stats = {"downloaded": 0, "skipped": 0, "incomplete": 0, "errors": 0}

    try:
        for year in sorted(years):
            year_str = str(year)
            if year_str not in dataset_ids:
                print(f"⚠️  Year {year} not found in dataset config, skipping.")
                continue

            dataset_id = dataset_ids[year_str]
            endpoint = get_soda_endpoint(dataset_id)

            for month in months:
                output_path = get_output_path(year, month)

                # Skip future months
                if is_future_month(year, month):
                    print(f"⏭️  {year}/{month:02d}: Future month, skipping.")
                    stats["skipped"] += 1
                    continue

                # Skip existing files unless --force
                if output_path.exists() and not args.force:
                    print(f"⏭️  {year}/{month:02d}: File exists, skipping.")
                    stats["skipped"] += 1
                    continue

                print(f"🔄 {year}/{month:02d}: Downloading...")

                try:
                    success, row_count = download_month(
                        session, endpoint, headers, year, month, output_path, args.page_size
                    )

                    if not success or row_count == 0:
                        print(f"   ⚠️  No data found for {year}/{month:02d}.")
                        if output_path.exists():
                            output_path.unlink()
                        stats["incomplete"] += 1
                        continue

                    # Check completeness: does the last day have data?
                    if not check_last_day_has_data(session, endpoint, headers, year, month):
                        print(f"   ⚠️  Incomplete data (no data for last day), removing file.")
                        if output_path.exists():
                            output_path.unlink()
                        stats["incomplete"] += 1
                        continue

                    print(f"   ✅ Saved {row_count} rows to {output_path.relative_to(repo_root())}")
                    stats["downloaded"] += 1

                except Exception as exc:
                    print(f"   ❌ Error: {exc}")
                    if output_path.exists():
                        output_path.unlink()
                    stats["errors"] += 1

    finally:
        session.close()

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
