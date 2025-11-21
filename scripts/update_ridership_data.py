#!/usr/bin/env python3
"""Refresh yearly MTA ridership CSVs using Socrata SODA (SODA3) API.

Key behaviors:
- Reads the year→dataset_id mapping from ``references/dataset_id_on_nyopendata.json``.
- Maintains one CSV per year under ``data/raw/ridership/<year>.csv``.
- For the current year (the max key in the mapping), it appends only new rows
  based on the last timestamp fetched.
- For prior years, it compares API row counts to the local file; if counts
  match, it skips downloading. Otherwise, it refreshes the entire year.
- Creates any missing yearly CSVs automatically.
- Writes per-year state to ``data/raw/ridership/auto/<year>.yaml`` and updates
  ``data/raw/ridership/metadata.yaml`` with the current year and file details.
- Displays a progress bar during downloads.

Usage:
    SOCRATA_APP_TOKEN=... [SOCRATA_SECRET_TOKEN=...] python scripts/update_ridership_data.py
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import requests
from tqdm import tqdm  # type: ignore

API_TEMPLATE = "https://data.ny.gov/resource/{dataset_id}.json"
DEFAULT_TS_COLUMN = "transit_timestamp"
DEFAULT_PAGE_SIZE = 50000
DEFAULT_MAPPING_PATH = Path("references/dataset_id_on_nyopendata.json")

# Provided by user; can be overridden via environment variables.
DEFAULT_APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN", "PSOinhnLdpy9yMfRdcYzUsJNy")
DEFAULT_SECRET_TOKEN = os.getenv("SOCRATA_SECRET_TOKEN", "4MWd4bOFT-e8fv7YrXh427YfHSrblNqUSX7m")

REQUEST_TIMEOUT = 60  # seconds
MAX_RETRIES = 5


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Refresh yearly ridership files incrementally using SODA3.",
    )
    parser.add_argument(
        "--ts-column",
        default=DEFAULT_TS_COLUMN,
        help="Timestamp column name in the dataset (default: %(default)s)",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=DEFAULT_PAGE_SIZE,
        help="Rows per page to request (default: %(default)s)",
    )
    parser.add_argument(
        "--years",
        type=int,
        nargs="*",
        help="Optional list of years to process (defaults to all keys in mapping).",
    )
    parser.add_argument(
        "--mapping-path",
        type=Path,
        default=DEFAULT_MAPPING_PATH,
        help="Path to year→dataset_id JSON mapping (default: %(default)s)",
    )
    return parser.parse_args()


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def build_headers(app_token: str, secret_token: str) -> Dict[str, str]:
    headers = {"Accept": "application/json"}
    if app_token:
        headers["X-App-Token"] = app_token
    if secret_token:
        headers["X-App-Token-Secret"] = secret_token
    return headers


def load_dataset_mapping(path: Path) -> Dict[int, str]:
    if not path.exists():
        raise FileNotFoundError(f"Dataset mapping not found at {path}")
    raw = json.loads(path.read_text(encoding="utf-8"))
    mapping: Dict[int, str] = {}
    for key, value in raw.items():
        try:
            year = int(key)
        except (TypeError, ValueError):
            continue
        mapping[year] = str(value)
    if not mapping:
        raise RuntimeError("Dataset mapping is empty or invalid.")
    return mapping


def load_metadata(path: Path) -> Dict[str, str]:
    metadata: Dict[str, str] = {}
    if not path.exists():
        return metadata
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        metadata[key.strip()] = value.strip()
    return metadata


def save_metadata(path: Path, metadata: Dict[str, str]) -> None:
    ordered_keys = [
        "dataset_id",
        "year",
        "ts_column",
        "last_ts",
        "last_retrieved",
        "rows_added",
        "row_count",
        "mode",
    ]
    lines: List[str] = []
    for key in ordered_keys:
        value = metadata.get(key)
        if value:
            lines.append(f"{key}: {value}")
    for key, value in metadata.items():
        if key in ordered_keys or not value:
            continue
        lines.append(f"{key}: {value}")
    path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")


def read_csv_header(path: Path) -> List[str]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        try:
            header = next(reader)
        except StopIteration:
            return []
    return header


def read_last_nonempty_line(path: Path) -> Optional[str]:
    with path.open("rb") as handle:
        handle.seek(0, os.SEEK_END)
        end = handle.tell()
        if end == 0:
            return None
        buffer = b""
        chunk_size = 1024
        while end > 0:
            step = min(chunk_size, end)
            end -= step
            handle.seek(end)
            buffer = handle.read(step) + buffer
            lines = buffer.split(b"\n")
            for line in reversed(lines):
                if line.strip():
                    try:
                        return line.decode("utf-8")
                    except UnicodeDecodeError:
                        return line.decode("utf-8", errors="replace")
            buffer = lines[0]
        if buffer.strip():
            try:
                return buffer.strip().decode("utf-8")
            except UnicodeDecodeError:
                return buffer.strip().decode("utf-8", errors="replace")
    return None


def read_last_timestamp_from_csv(csv_path: Path, ts_column: str) -> Optional[str]:
    header = read_csv_header(csv_path)
    if not header:
        return None
    try:
        index = header.index(ts_column)
    except ValueError as exc:
        raise RuntimeError(
            f"Timestamp column '{ts_column}' not found in existing CSV header"
        ) from exc

    last_line = read_last_nonempty_line(csv_path)
    if not last_line:
        return None
    row = next(csv.reader([last_line]))
    if index >= len(row):
        return None
    return row[index].strip() or None


def count_local_rows(csv_path: Path) -> int:
    if not csv_path.exists() or csv_path.stat().st_size == 0:
        return 0
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        row_count = sum(1 for _ in handle)
    return max(0, row_count - 1)  # subtract header


def to_datetime(value: str) -> datetime:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"Unable to parse timestamp '{value}'") from exc


def request_page(
    session: requests.Session,
    url: str,
    headers: Dict[str, str],
    params: Dict[str, str],
) -> List[Dict[str, str]]:
    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            response = session.get(url, headers=headers, params=params, timeout=REQUEST_TIMEOUT)
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

        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            raise RuntimeError(
                f"Socrata request failed with status {response.status_code}: {response.text}"
            ) from exc

        payload = response.json()
        if isinstance(payload, dict) and payload.get("code") == "not_found":
            raise RuntimeError(f"Dataset not found or inaccessible: {payload}")
        if not isinstance(payload, list):
            raise RuntimeError(f"Unexpected payload structure: {payload!r}")
        return payload

    return []


def merge_columns(rows: Sequence[Dict[str, str]]) -> List[str]:
    columns: List[str] = []
    for row in rows:
        for key in row.keys():
            if key.startswith(":"):
                continue
            if key not in columns:
                columns.append(key)
    return columns


def build_where_clause(ts_column: str, lower: str, upper: str, inclusive_lower: bool) -> str:
    op = ">=" if inclusive_lower else ">"
    return f"{ts_column} {op} '{lower}' AND {ts_column} < '{upper}'"


def count_rows(
    session: requests.Session,
    dataset_id: str,
    headers: Dict[str, str],
    where_clause: str,
) -> int:
    url = API_TEMPLATE.format(dataset_id=dataset_id)
    params = {"$select": "count(1)", "$where": where_clause}
    rows = request_page(session, url, headers, params)
    if not rows:
        return 0
    first = rows[0]
    if len(first) == 1:
        count_value = next(iter(first.values()))
    else:
        count_value = first.get("count") or first.get("count_1")
    try:
        return int(count_value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"Malformed count response: {rows}") from exc


def download_rows(
    session: requests.Session,
    dataset_id: str,
    headers: Dict[str, str],
    where_clause: str,
    ts_column: str,
    csv_path: Path,
    page_size: int,
    header_seed: Sequence[str],
    csv_mode: str,
    csv_exists: bool,
    expected_rows: int,
    order_clause: str,
) -> Tuple[int, Optional[str], List[str]]:
    url = API_TEMPLATE.format(dataset_id=dataset_id)
    offset = 0
    header = list(header_seed)
    csv_handle = None
    writer: Optional[csv.DictWriter] = None
    write_header = csv_mode == "w" or not csv_exists
    latest_ts_raw: Optional[str] = None
    total_written = 0
    progress = tqdm(
        total=expected_rows,
        unit="rows",
        desc=csv_path.name,
        leave=False,
    )
    flush_every_pages = 5  # flush after this many pages for a balance of speed vs. safety
    page_counter = 0

    try:
        while True:
            params = {
                "$where": where_clause,
                "$order": order_clause,
                "$limit": str(page_size),
                "$offset": str(offset),
            }
            rows = request_page(session, url, headers, params)
            if not rows:
                break

            if not header:
                header = merge_columns(rows)

            if writer is None:
                mode = "a" if csv_mode == "append" else "w"
                csv_handle = csv_path.open(mode, encoding="utf-8", newline="")
                writer = csv.DictWriter(csv_handle, fieldnames=header, extrasaction="ignore")
                if write_header and header:
                    writer.writeheader()

            if writer is None:
                raise RuntimeError("Unable to initialize CSV writer (missing header).")

            batch: List[Dict[str, str]] = []
            for row in rows:
                cleaned = {key: value for key, value in row.items() if not key.startswith(":")}
                if ts_column not in cleaned or not cleaned[ts_column]:
                    continue
                batch.append({column: cleaned.get(column, "") for column in header})
                latest_ts_raw = cleaned[ts_column]

            written_this_page = len(batch)
            if written_this_page:
                writer.writerows(batch)
            total_written += written_this_page
            if written_this_page:
                progress.update(written_this_page)
                page_counter += 1
                if csv_handle is not None and page_counter % flush_every_pages == 0:
                    csv_handle.flush()

            offset += len(rows)
            if len(rows) < page_size:
                break
    finally:
        progress.close()
        if csv_handle is not None:
            csv_handle.flush()
            csv_handle.close()

    return total_written, latest_ts_raw, header


def _parse_minimal_yaml(text: str) -> Dict[str, dict]:
    data: Dict[str, dict] = {}
    current_key: Optional[str] = None
    for raw_line in text.splitlines():
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue
        if not raw_line.startswith(" "):
            key, _, rest = raw_line.partition(":")
            key = key.strip()
            rest = rest.strip()
            if rest:
                data[key] = rest.strip('"')
                current_key = None
            else:
                current_key = key
                data.setdefault(key, {})
        else:
            if current_key is None:
                continue
            sub_line = raw_line.strip()
            sub_key, _, sub_val = sub_line.partition(":")
            data[current_key][sub_key.strip()] = sub_val.strip().strip('"')
    return data


def load_user_metadata(path: Path) -> Dict[str, dict]:
    if not path.exists():
        return {}
    text = path.read_text(encoding="utf-8")
    try:
        import yaml  # type: ignore

        parsed = yaml.safe_load(text) or {}
        if isinstance(parsed, dict):
            return parsed  # type: ignore[return-value]
    except Exception:
        pass
    return _parse_minimal_yaml(text)


def render_metadata_yaml(data: Dict[str, dict]) -> str:
    lines = ["# MTA Ridership Data Metadata", ""]
    for key, value in data.items():
        if isinstance(value, dict):
            lines.append(f"{key}:")
            for sub_key, sub_val in value.items():
                lines.append(f"  {sub_key}: {sub_val}")
            lines.append("")
        else:
            lines.append(f"{key}: {value}")
            lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def save_user_metadata(path: Path, data: Dict[str, dict]) -> None:
    try:
        import yaml  # type: ignore

        dumped = yaml.safe_dump(data, sort_keys=False)  # type: ignore[arg-type]
        header = "# MTA Ridership Data Metadata\n"
        path.write_text(header + dumped, encoding="utf-8")
    except Exception:
        path.write_text(render_metadata_yaml(data), encoding="utf-8")


def dataset_url(dataset_id: str) -> str:
    return f"https://data.ny.gov/d/{dataset_id}"


def process_year(
    year: int,
    dataset_id: str,
    ts_column: str,
    page_size: int,
    current_year: int,
    data_dir: Path,
    auto_dir: Path,
    session: requests.Session,
    headers: Dict[str, str],
) -> Dict[str, object]:
    start_iso = f"{year:04d}-01-01T00:00:00"
    end_iso = f"{year + 1:04d}-01-01T00:00:00"
    csv_path = data_dir / f"{year}.csv"
    csv_exists = csv_path.exists() and csv_path.stat().st_size > 0
    local_count = count_local_rows(csv_path)
    existing_header = read_csv_header(csv_path) if csv_exists else []
    if csv_exists and existing_header and ts_column not in existing_header:
        raise RuntimeError(
            f"Timestamp column '{ts_column}' not present in existing {csv_path.name}. "
            "Recreate the file or specify the correct --ts-column."
        )

    meta_path = auto_dir / f"{year}.yaml"
    meta = load_metadata(meta_path)
    last_ts = (
        meta.get("last_ts")
        if meta.get("dataset_id") == dataset_id
        and meta.get("ts_column") == ts_column
        and meta.get("year") == str(year)
        else None
    )
    if not last_ts and csv_exists and ts_column in existing_header:
        last_ts = read_last_timestamp_from_csv(csv_path, ts_column)

    order_clause = f"{ts_column} ASC, :id ASC"

    # Decide strategy based on year.
    if year == current_year:
        lower_bound = last_ts or start_iso
        inclusive_lower = last_ts is None
        where_clause = build_where_clause(ts_column, lower_bound, end_iso, inclusive_lower)
        expected_rows = count_rows(session, dataset_id, headers, where_clause)
        if expected_rows == 0:
            print(f"{year}: no new rows (last_ts={last_ts or 'start'})")
            final_last_ts = last_ts
            final_count = local_count
            mode = "no_new_rows"
            rows_written = 0
        else:
            print(f"{year}: downloading {expected_rows} new rows (starting at {lower_bound})")
            rows_written, latest_ts_raw, header = download_rows(
                session,
                dataset_id,
                headers,
                where_clause,
                ts_column,
                csv_path,
                page_size,
                existing_header,
                "append",
                csv_exists,
                expected_rows,
                order_clause,
            )
            final_last_ts = latest_ts_raw or last_ts
            final_count = local_count + rows_written
            mode = "incremental"
    else:
        where_clause = build_where_clause(ts_column, start_iso, end_iso, True)
        remote_count = count_rows(session, dataset_id, headers, where_clause)
        if remote_count == local_count and remote_count > 0:
            print(f"{year}: local rows match remote ({local_count}); skipping download.")
            final_last_ts = (
                read_last_timestamp_from_csv(csv_path, ts_column)
                if (csv_exists and ts_column in existing_header)
                else None
            )
            rows_written = 0
            final_count = local_count
            mode = "match"
        elif remote_count == 0:
            print(f"{year}: API returned zero rows; creating/keeping empty file.")
            csv_path.write_text("", encoding="utf-8")
            rows_written = 0
            final_last_ts = None
            final_count = 0
            mode = "empty"
        else:
            print(f"{year}: refreshing full year ({remote_count} rows).")
            rows_written, latest_ts_raw, header = download_rows(
                session,
                dataset_id,
                headers,
                where_clause,
                ts_column,
                csv_path,
                page_size,
                [],
                "w",
                False,
                remote_count,
                order_clause,
            )
            final_last_ts = latest_ts_raw
            final_count = rows_written
            mode = "full_refresh"

    last_retrieved = datetime.now(timezone.utc).isoformat()
    meta_record: Dict[str, str] = {
        "dataset_id": dataset_id,
        "year": str(year),
        "ts_column": ts_column,
        "last_retrieved": last_retrieved,
        "rows_added": str(rows_written),
        "row_count": str(final_count),
        "mode": mode,
    }
    if final_last_ts:
        meta_record["last_ts"] = final_last_ts
    save_metadata(meta_path, meta_record)

    return {
        "year": year,
        "dataset_id": dataset_id,
        "csv_path": csv_path,
        "row_count": final_count,
        "last_ts": final_last_ts,
        "mode": mode,
    }


def main() -> int:
    args = parse_args()
    ts_column = args.ts_column
    page_size = args.page_size

    mapping = load_dataset_mapping(repo_root() / args.mapping_path)
    years = sorted(mapping.keys())
    if args.years:
        requested = set(args.years)
        missing = requested - set(years)
        if missing:
            raise SystemExit(f"Requested years not in mapping: {sorted(missing)}")
        years = sorted(requested)

    current_year = max(years)

    root = repo_root()
    data_dir = root / "data" / "raw" / "ridership"
    auto_dir = data_dir / "auto"
    ensure_dir(data_dir)
    ensure_dir(auto_dir)

    token = DEFAULT_APP_TOKEN
    secret = DEFAULT_SECRET_TOKEN
    headers = build_headers(token, secret)
    session = requests.Session()

    results: List[Dict[str, object]] = []
    try:
        for year in years:
            dataset_id = mapping[year]
            result = process_year(
                year=year,
                dataset_id=dataset_id,
                ts_column=ts_column,
                page_size=page_size,
                current_year=current_year,
                data_dir=data_dir,
                auto_dir=auto_dir,
                session=session,
                headers=headers,
            )
            results.append(result)
    finally:
        session.close()

    # Update human-readable metadata file.
    metadata_path = data_dir / "metadata.yaml"
    user_metadata = load_user_metadata(metadata_path)
    user_metadata["current_year"] = {"year": current_year}
    user_metadata["year"] = current_year
    today = datetime.now(timezone.utc).date().isoformat()
    for result in results:
        year = result["year"]
        dataset_id = result["dataset_id"]
        row_count = result["row_count"]
        key = f"{year}.csv"
        block = user_metadata.get(key, {}) or {}
        block["data_url"] = dataset_url(dataset_id)  # type: ignore[index]
        block["retrieval_date"] = today
        block["dataset_id"] = dataset_id  # type: ignore[index]
        block["rows"] = row_count  # type: ignore[index]
        user_metadata[key] = block  # type: ignore[index]
    save_user_metadata(metadata_path, user_metadata)

    print("Done.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:  # pragma: no cover - defensive logging
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
