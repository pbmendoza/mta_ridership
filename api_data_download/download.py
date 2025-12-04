#!/usr/bin/env python3
"""Download MTA ridership data via the SODA3 API.

This script uses the Socrata SODA (SoQL) API for dataset ``5wq4-mkjj`` to
download ridership records for a specified year, month, or day.

Usage:
    python api_data_download/download.py --year 2025                # full year
    python api_data_download/download.py --year 2025 --month 1      # full month
    python api_data_download/download.py --year 2025 --month 1 --day 15  # single day

App/secret tokens can be supplied via environment:
    SOCRATA_APP_TOKEN=... SOCRATA_SECRET_TOKEN=... python api_data_download/download.py
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import requests

DATASET_ID = "5wq4-mkjj"
SODA3_ENDPOINT = f"https://data.ny.gov/resource/{DATASET_ID}.json"

# Provided credentials (can be overridden via env/CLI).
DEFAULT_APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN", "PSOinhnLdpy9yMfRdcYzUsJNy")
DEFAULT_SECRET_TOKEN = os.getenv("SOCRATA_SECRET_TOKEN", "4MWd4bOFT-e8fv7YrXh427YfHSrblNqUSX7m")

DEFAULT_PAGE_SIZE = 50000  # SODA max is typically 50k
REQUEST_TIMEOUT = 60
MAX_RETRIES = 5

# Column order for CSV output (extras discovered at runtime get appended).
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download MTA ridership records via SODA3 for a given year/month/day.",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=2025,
        help="Year to download (4 digits, default: 2025).",
    )
    parser.add_argument(
        "--month",
        type=int,
        default=None,
        help="Month to download (1-12). If omitted, downloads full year.",
    )
    parser.add_argument(
        "--day",
        type=int,
        default=None,
        help="Day to download (1-31). Requires --month. If omitted, downloads full month.",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=DEFAULT_PAGE_SIZE,
        help="Rows per page for SODA pagination (max ~50k).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional output CSV path. Defaults to {year}{month}{day}.csv in script dir.",
    )
    parser.add_argument(
        "--app-token",
        default=DEFAULT_APP_TOKEN,
        help="Socrata app token (overrides env/constant).",
    )
    parser.add_argument(
        "--secret-token",
        default=DEFAULT_SECRET_TOKEN,
        help="Socrata secret token (overrides env/constant).",
    )
    args = parser.parse_args()
    if args.day is not None and args.month is None:
        parser.error("--day requires --month to be specified.")
    return args


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def compute_date_range(
    year: int, month: Optional[int], day: Optional[int]
) -> Tuple[str, str]:
    """Compute inclusive start and exclusive end ISO timestamps."""
    if month is None:
        # Full year
        start = date(year, 1, 1)
        end = date(year + 1, 1, 1)
    elif day is None:
        # Full month
        start = date(year, month, 1)
        if month == 12:
            end = date(year + 1, 1, 1)
        else:
            end = date(year, month + 1, 1)
    else:
        # Single day
        start = date(year, month, day)
        end = start + __import__("datetime").timedelta(days=1)
    return start.isoformat() + "T00:00:00", end.isoformat() + "T00:00:00"


def build_output_filename(
    year: int, month: Optional[int], day: Optional[int]
) -> str:
    """Build output filename based on provided date components."""
    if month is None:
        return f"{year}.csv"
    elif day is None:
        return f"{year}{month:02d}.csv"
    else:
        return f"{year}{month:02d}{day:02d}.csv"


def build_headers(app_token: str, secret_token: str) -> Dict[str, str]:
    headers = {
        "Accept": "application/json",
        "X-App-Token": app_token,
    }
    # Pass the secret as a secondary header to align with the new SODA3 auth scheme.
    # Socrata ignores unknown headers, so this is safe if the gateway does not require it.
    if secret_token:
        headers["X-App-Token-Secret"] = secret_token
    return headers


def request_json(
    session: requests.Session,
    params: Dict[str, str],
    headers: Dict[str, str],
) -> List[Dict[str, str]]:
    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            response = session.get(SODA3_ENDPOINT, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
        except requests.RequestException as exc:
            attempt += 1
            sleep_for = min(2**attempt, 60)
            time.sleep(sleep_for)
            if attempt >= MAX_RETRIES:
                raise RuntimeError("Request failed after retries") from exc
            continue

        if response.status_code == 429:
            attempt += 1
            sleep_for = min(2**attempt, 60)
            time.sleep(sleep_for)
            continue

        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, dict) and payload.get("code") == "not_found":
            raise RuntimeError(f"SODA3 endpoint reported not_found: {payload}")
        if not isinstance(payload, list):
            raise RuntimeError(f"Unexpected payload shape: {payload!r}")
        return payload

    return []


def count_rows(session: requests.Session, headers: Dict[str, str], where_clause: str) -> int:
    params = {"$select": "count(1)", "$where": where_clause}
    rows = request_json(session, params, headers)
    if not rows:
        raise RuntimeError("Unable to retrieve row count from API response")
    try:
        first = rows[0]
        if len(first) == 1:
            count_value = next(iter(first.values()))
        else:
            count_value = first.get("count") or first.get("count_1")
        return int(count_value)
    except (KeyError, ValueError, TypeError) as exc:
        raise RuntimeError(f"Malformed count response: {rows}") from exc


def merge_header(seen_rows: Sequence[Dict[str, str]]) -> List[str]:
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


def normalize_row(row: Dict[str, str], header: Sequence[str]) -> Dict[str, str]:
    cleaned: Dict[str, str] = {}
    for key in header:
        value = row.get(key, "")
        if isinstance(value, (dict, list)):
            cleaned[key] = json.dumps(value, separators=(",", ":"))
        else:
            cleaned[key] = value
    return cleaned


def default_output_path(year: int, month: Optional[int], day: Optional[int]) -> Path:
    """Return default output path in the data subdirectory."""
    filename = build_output_filename(year, month, day)
    return Path(__file__).resolve().parent / "data" / filename


def format_path(path: Path) -> str:
    for base in (repo_root(), Path(__file__).resolve().parent):
        try:
            return str(path.relative_to(base))
        except ValueError:
            continue
    return str(path)


def main() -> int:
    args = parse_args()

    app_token = args.app_token
    secret_token = args.secret_token
    if not app_token:
        raise SystemExit("App token is required (set SOCRATA_APP_TOKEN or use --app-token)")

    start_ts, end_ts = compute_date_range(args.year, args.month, args.day)
    headers = build_headers(app_token, secret_token)
    where_clause = f"transit_timestamp >= '{start_ts}' AND transit_timestamp < '{end_ts}'"
    output_path = args.output or default_output_path(args.year, args.month, args.day)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    try:
        total_rows = count_rows(session, headers, where_clause)
        print(f"Found {total_rows} rows for {where_clause}")
        if total_rows <= 0:
            print("No rows found for the requested window; exiting.")
            return 0

        offset = 0
        header: List[str] = []
        written = 0
        with output_path.open("w", encoding="utf-8", newline="") as handle:
            writer: csv.DictWriter | None = None
            while True:
                params = {
                    "$where": where_clause,
                    "$order": "transit_timestamp ASC, station_complex_id ASC, payment_method ASC, fare_class_category ASC",
                    "$limit": str(args.page_size),
                    "$offset": str(offset),
                }
                rows = request_json(session, params, headers)
                if not rows:
                    break

                if not header:
                    header = merge_header(rows)
                    writer = csv.DictWriter(handle, fieldnames=header)
                    writer.writeheader()
                assert writer is not None

                for row in rows:
                    if "transit_timestamp" not in row:
                        continue
                    writer.writerow(normalize_row(row, header))
                    written += 1
                    if total_rows:
                        progress = min(100.0, (written / total_rows) * 100)
                        # Lightweight progress indicator, overwrites the same line.
                        print(f"\rProgress: {written}/{total_rows} rows ({progress:5.1f}%)", end="", flush=True)

                offset += len(rows)
                if len(rows) < args.page_size:
                    break

        # Finish the progress line cleanly.
        if total_rows:
            print()
        print(f"Saved {written} rows to {format_path(output_path)}")
    finally:
        session.close()

    if total_rows != written:
        print(f"Warning: expected {total_rows} rows but wrote {written}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:  # pragma: no cover - defensive logging
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
