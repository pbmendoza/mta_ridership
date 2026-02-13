#!/usr/bin/env python3
"""Build station-level monthly ridership metrics directly from NY Open Data API.

This script recreates the schema of:
    results/ridership/monthly_ridership_station.csv

Output columns:
    complex_id, year, month, period, day_group, ridership, omny_pct

Key behaviors mirrored from the existing local pipeline:
1. Subway-only filter
2. Exclude station_complex_id='502'
3. Exclude incomplete months (every calendar day must have data)
4. Produce weekday/weekend plus derived total rows
5. Fill missing station-month-day_group combinations with zeros (excluding SIR)
6. Compute OMNY percentage per row

Operating modes:
- **Incremental** (default): Reads the existing output CSV, skips months
  already present, and only fetches new months. The fastest option for
  routine monthly updates.
- **Targeted refresh** (``--year`` / ``--month``): Always re-fetches the
  specified months, replacing them in the existing output while keeping
  all other months intact.
- **Full refresh** (``--full-refresh``): Ignores the existing output and
  reprocesses every month from scratch. Use when the upstream raw data has
  been corrected or the station reference file has changed.
"""

from __future__ import annotations

import argparse
import calendar
from dataclasses import dataclass
from datetime import date
import json
from pathlib import Path
import sys
from typing import Dict, Iterable, List, Optional, Set, Tuple

# Ensure repo root is on sys.path so that ``scripts.utils`` is importable.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pandas as pd
import requests

from scripts.utils.socrata import (
    build_headers,
    get_soda_endpoint,
    load_socrata_token,
    load_socrata_secret_token,
    repo_root,
    request_json,
)

DEFAULT_PAGE_SIZE = 50_000
DAY_GROUP_ORDER = ["total", "weekday", "weekend"]
OUTPUT_REL_PATH = Path("results/ridership/monthly_ridership_station.csv")


@dataclass(frozen=True)
class MonthTask:
    year: int
    month: int
    dataset_id: str


def load_dataset_ids() -> Dict[int, str]:
    config_path = repo_root() / "references" / "dataset_id_on_nyopendata.json"
    with config_path.open("r", encoding="utf-8") as handle:
        raw = json.load(handle)
    return {int(year): dataset_id for year, dataset_id in raw.items()}


def month_date_range(year: int, month: int) -> Tuple[str, str]:
    start = date(year, month, 1)
    if month == 12:
        end = date(year + 1, 1, 1)
    else:
        end = date(year, month + 1, 1)
    return f"{start.isoformat()}T00:00:00", f"{end.isoformat()}T00:00:00"


def is_future_month(year: int, month: int) -> bool:
    today = date.today()
    return date(year, month, 1) > date(today.year, today.month, 1)


def month_where_clause(year: int, month: int, station_id: Optional[str] = None) -> str:
    start_ts, end_ts = month_date_range(year, month)
    where_clause = (
        "transit_mode = 'subway' "
        "AND station_complex_id != '502' "
        f"AND transit_timestamp >= '{start_ts}' "
        f"AND transit_timestamp < '{end_ts}'"
    )
    if station_id:
        where_clause += f" AND station_complex_id = '{station_id}'"
    return where_clause


def month_has_complete_days(
    session: requests.Session,
    endpoint: str,
    headers: Dict[str, str],
    year: int,
    month: int,
) -> bool:
    # Completeness is evaluated at month level across the full filtered dataset,
    # not per-station, to mirror the existing pipeline behavior.
    where_clause = month_where_clause(year, month, station_id=None)
    params = {
        "$select": "date_trunc_ymd(transit_timestamp) as service_day",
        "$where": where_clause,
        "$group": "service_day",
        "$order": "service_day ASC",
        "$limit": "50000",
    }
    rows = request_json(session, endpoint, params, headers)
    expected_days = calendar.monthrange(year, month)[1]
    return len(rows) == expected_days


def fetch_grouped_rows(
    session: requests.Session,
    endpoint: str,
    headers: Dict[str, str],
    where_clause: str,
    page_size: int,
) -> List[Dict[str, str]]:
    select_sql = "station_complex_id, payment_method, sum(ridership) as ridership"
    group_sql = "station_complex_id, payment_method"
    order_sql = "station_complex_id ASC, payment_method ASC"

    all_rows: List[Dict[str, str]] = []
    offset = 0

    while True:
        params = {
            "$select": select_sql,
            "$where": where_clause,
            "$group": group_sql,
            "$order": order_sql,
            "$limit": str(page_size),
            "$offset": str(offset),
        }
        rows = request_json(session, endpoint, params, headers)
        if not rows:
            break
        all_rows.extend(rows)
        if len(rows) < page_size:
            break
        offset += len(rows)

    return all_rows


def fetch_month_station_payment_day_group(
    session: requests.Session,
    endpoint: str,
    headers: Dict[str, str],
    year: int,
    month: int,
    page_size: int,
    station_id: Optional[str],
) -> pd.DataFrame:
    base_where = month_where_clause(year, month, station_id=station_id)
    weekday_where = base_where + " AND date_extract_dow(transit_timestamp) NOT IN (0, 6)"
    weekend_where = base_where + " AND date_extract_dow(transit_timestamp) IN (0, 6)"

    weekday_rows = fetch_grouped_rows(session, endpoint, headers, weekday_where, page_size)
    weekend_rows = fetch_grouped_rows(session, endpoint, headers, weekend_where, page_size)

    tagged_rows: List[Dict[str, str]] = []
    for row in weekday_rows:
        row_copy = dict(row)
        row_copy["day_group"] = "weekday"
        tagged_rows.append(row_copy)
    for row in weekend_rows:
        row_copy = dict(row)
        row_copy["day_group"] = "weekend"
        tagged_rows.append(row_copy)

    if not tagged_rows:
        return pd.DataFrame(columns=["station_complex_id", "payment_method", "day_group", "ridership"])

    df = pd.DataFrame(tagged_rows)
    df["station_complex_id"] = df["station_complex_id"].astype(str)
    df["payment_method"] = df["payment_method"].astype("string").str.upper().fillna("")
    df["ridership"] = pd.to_numeric(df["ridership"], errors="coerce").fillna(0.0)
    df["day_group"] = df["day_group"].astype("string")
    return df


def to_station_month_metrics(month_df: pd.DataFrame, year: int, month: int) -> pd.DataFrame:
    if month_df.empty:
        return pd.DataFrame(
            columns=[
                "station_complex_id",
                "year",
                "month",
                "period",
                "day_group",
                "ridership",
                "omny_pct",
            ]
        )

    by_method = (
        month_df.groupby(["station_complex_id", "payment_method", "day_group"], as_index=False)["ridership"]
        .sum()
    )

    by_method_total = (
        by_method.groupby(["station_complex_id", "payment_method"], as_index=False)["ridership"]
        .sum()
        .assign(day_group="total")
    )
    by_method = pd.concat([by_method, by_method_total], ignore_index=True)

    totals = by_method.groupby(["station_complex_id", "day_group"], as_index=False)["ridership"].sum()
    omny = (
        by_method[by_method["payment_method"] == "OMNY"]
        .groupby(["station_complex_id", "day_group"], as_index=False)["ridership"]
        .sum()
        .rename(columns={"ridership": "omny_ridership"})
    )
    station = totals.merge(omny, on=["station_complex_id", "day_group"], how="left")
    station["omny_ridership"] = station["omny_ridership"].fillna(0.0)

    station["omny_pct"] = 0.0
    nonzero = station["ridership"] > 0
    station.loc[nonzero, "omny_pct"] = (
        station.loc[nonzero, "omny_ridership"] / station.loc[nonzero, "ridership"] * 100
    ).round(2)

    station["year"] = year
    station["month"] = month
    station["period"] = f"{year:04d}-{month:02d}-01"
    return station[
        [
            "station_complex_id",
            "year",
            "month",
            "period",
            "day_group",
            "ridership",
            "omny_pct",
        ]
    ].copy()


def create_complete_station_month_grid(
    monthly_data: pd.DataFrame,
    station_id: Optional[str] = None,
) -> pd.DataFrame:
    if station_id:
        all_station_ids = [str(station_id)]
    else:
        stations_file = repo_root() / "references" / "stations" / "stations_complexes_official.csv"
        stations_df = pd.read_csv(stations_file)
        stations_df = stations_df[stations_df["Daytime Routes"] != "SIR"]
        all_station_ids = stations_df["Complex ID"].astype(str).unique()

    periods = monthly_data[["year", "month", "period"]].drop_duplicates()
    if periods.empty:
        return monthly_data.copy()

    grid_parts: List[pd.DataFrame] = []
    for _, period in periods.iterrows():
        for day_group in DAY_GROUP_ORDER:
            grid_parts.append(
                pd.DataFrame(
                    {
                        "station_complex_id": all_station_ids,
                        "year": int(period["year"]),
                        "month": int(period["month"]),
                        "period": period["period"],
                        "day_group": day_group,
                    }
                )
            )

    grid = pd.concat(grid_parts, ignore_index=True)
    merged = grid.merge(
        monthly_data,
        on=["station_complex_id", "year", "month", "period", "day_group"],
        how="left",
    )
    merged["ridership"] = merged["ridership"].fillna(0.0)
    merged["omny_pct"] = merged["omny_pct"].fillna(0.0)
    return merged


def format_output(df: pd.DataFrame) -> pd.DataFrame:
    out = df.rename(columns={"station_complex_id": "complex_id"}).copy()
    out["complex_id"] = out["complex_id"].astype(str)
    out["_complex_id_sort"] = pd.to_numeric(out["complex_id"], errors="coerce")
    out["day_group"] = pd.Categorical(out["day_group"], categories=DAY_GROUP_ORDER, ordered=True)
    out = out.sort_values(["year", "month", "_complex_id_sort", "day_group"], ignore_index=True)
    out = out.drop(columns=["_complex_id_sort"])
    out["day_group"] = out["day_group"].astype(str)
    return out[
        ["complex_id", "year", "month", "period", "day_group", "ridership", "omny_pct"]
    ].copy()


def build_tasks(dataset_ids: Dict[int, str], year: Optional[int], month: Optional[int]) -> Iterable[MonthTask]:
    if year is not None:
        years = [year]
    else:
        years = sorted(dataset_ids.keys())

    months = [month] if month is not None else list(range(1, 13))

    for y in years:
        dataset_id = dataset_ids.get(y)
        if dataset_id is None:
            print(f"⚠️  Year {y} is not present in references/dataset_id_on_nyopendata.json, skipping.")
            continue
        for m in months:
            if is_future_month(y, m):
                print(f"⏭️  {y}/{m:02d}: future month, skipping.")
                continue
            yield MonthTask(year=y, month=m, dataset_id=dataset_id)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Recreate station-level monthly ridership from NY Open Data API."
    )
    parser.add_argument("--year", type=int, default=None, help="Optional year filter (e.g., 2025).")
    parser.add_argument("--month", type=int, default=None, help="Optional month filter (1-12). Requires --year.")
    parser.add_argument(
        "--station-id",
        type=str,
        default=None,
        help="Optional station_complex_id filter for a quick single-station run (e.g., 399).",
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        default=False,
        help="Ignore existing output and reprocess all months from scratch.",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=DEFAULT_PAGE_SIZE,
        help=f"API page size for grouped queries (default: {DEFAULT_PAGE_SIZE}).",
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
    args = parser.parse_args()

    if args.month is not None and args.year is None:
        parser.error("--month requires --year.")
    if args.month is not None and (args.month < 1 or args.month > 12):
        parser.error("--month must be in 1..12.")
    if args.page_size < 1:
        parser.error("--page-size must be >= 1.")
    if args.full_refresh and (args.year is not None or args.month is not None):
        print(
            "⚠️  --full-refresh processes all months; "
            "--year/--month filters will be ignored."
        )
        args.year = None
        args.month = None

    return args


def load_existing_output(output_path: Path) -> Optional[pd.DataFrame]:
    """Load the existing output CSV if it exists and is non-empty."""
    if not output_path.exists():
        return None
    try:
        df = pd.read_csv(output_path)
        if df.empty:
            return None
        df["complex_id"] = df["complex_id"].astype(str)
        df["year"] = df["year"].astype(int)
        df["month"] = df["month"].astype(int)
        df["ridership"] = pd.to_numeric(df["ridership"], errors="coerce").fillna(0.0)
        df["omny_pct"] = pd.to_numeric(df["omny_pct"], errors="coerce").fillna(0.0)
        return df
    except Exception as exc:
        print(f"⚠️  Could not read existing output ({exc}); will do a full build.")
        return None


def get_existing_months(existing_df: Optional[pd.DataFrame]) -> Set[Tuple[int, int]]:
    """Return a set of (year, month) tuples present in the existing output."""
    if existing_df is None:
        return set()
    return set(
        existing_df[["year", "month"]].drop_duplicates().itertuples(index=False, name=None)
    )


def main() -> int:
    args = parse_args()
    dataset_ids = load_dataset_ids()

    app_token = args.app_token if args.app_token is not None else load_socrata_token()
    secret_token = args.secret_token if args.secret_token is not None else load_socrata_secret_token()
    if not app_token:
        print(
            "⚠️  No Socrata app token found. Running without authentication "
            "(lower rate limits apply). Set SOCRATA_APP_TOKEN in .env, as an "
            "env var, or use --app-token. See references/docs/socrata_api_setup.md."
        )
    headers = build_headers(app_token, secret_token)

    output_path = repo_root() / OUTPUT_REL_PATH

    # -- Build candidate tasks from the JSON config --------------------------
    all_tasks = list(build_tasks(dataset_ids, args.year, args.month))
    if not all_tasks:
        print("No month tasks to process.")
        return 0

    # -- Determine operating mode and filter tasks ---------------------------
    #   full_refresh  year/month  → mode
    #   True          (ignored)   → Full: process all, overwrite file
    #   False         given       → Targeted: always re-fetch those months
    #   False         None        → Incremental: skip months already on disk
    existing_df: Optional[pd.DataFrame] = None
    is_targeted = not args.full_refresh and args.year is not None

    # When --station-id is used, skip incremental/targeted merge to avoid
    # mixing single-station results into the full-station output file.
    if args.station_id:
        print(f"📌 Single-station mode (station {args.station_id}) — no incremental merge.")
        tasks = all_tasks
    elif args.full_refresh:
        print("🔄 Full refresh — will reprocess all months.")
        tasks = all_tasks
    elif is_targeted:
        # Targeted: always re-fetch the specified year/month(s).
        # We still load the existing file so we can merge later.
        existing_df = load_existing_output(output_path)
        tasks = all_tasks
        target_label = f"{args.year}"
        if args.month is not None:
            target_label += f"/{args.month:02d}"
        print(f"🎯 Targeted refresh for {target_label}.")
    else:
        # Incremental: skip months already present in the output file.
        existing_df = load_existing_output(output_path)
        existing_months = get_existing_months(existing_df)
        if existing_months:
            tasks = [
                t for t in all_tasks
                if (t.year, t.month) not in existing_months
            ]
            skipped = len(all_tasks) - len(tasks)
            latest = max(existing_months)
            print(
                f"📂 Existing output has {len(existing_months)} month(s) "
                f"(latest: {latest[0]}/{latest[1]:02d}). "
                f"Skipping {skipped} month(s) already on disk."
            )
        else:
            print("📂 No existing output found — running full build.")
            tasks = all_tasks

    if not tasks:
        print("✅ Output is already up-to-date. Nothing to do.")
        return 0

    # -- Fetch new / refreshed months ----------------------------------------
    session = requests.Session()
    monthly_frames: List[pd.DataFrame] = []
    complete_count = 0
    incomplete_count = 0
    refreshed_months: Set[Tuple[int, int]] = set()

    try:
        for task in tasks:
            endpoint = get_soda_endpoint(task.dataset_id)
            month_label = f"{task.year}/{task.month:02d}"
            print(f"🔍 {month_label}: checking completeness...")

            if not month_has_complete_days(session, endpoint, headers, task.year, task.month):
                incomplete_count += 1
                print(f"⚠️  {month_label}: incomplete month, skipping.")
                continue

            print(f"📥 {month_label}: fetching grouped API rows...")
            raw_month = fetch_month_station_payment_day_group(
                session=session,
                endpoint=endpoint,
                headers=headers,
                year=task.year,
                month=task.month,
                page_size=args.page_size,
                station_id=args.station_id,
            )
            if raw_month.empty:
                incomplete_count += 1
                print(f"⚠️  {month_label}: no rows returned after filters, skipping.")
                continue

            month_metrics = to_station_month_metrics(raw_month, task.year, task.month)
            monthly_frames.append(month_metrics)
            refreshed_months.add((task.year, task.month))
            complete_count += 1
            print(f"✅ {month_label}: collected {len(month_metrics):,} station/day_group rows.")
    finally:
        session.close()

    # -- Merge with existing data --------------------------------------------
    if existing_df is not None and refreshed_months:
        # Remove months we just re-fetched (they're being replaced).
        existing_keys = set(zip(existing_df["year"], existing_df["month"]))
        keep_mask = ~pd.Series(
            [(y, m) in refreshed_months for y, m in zip(existing_df["year"], existing_df["month"])]
        )
        kept_existing = existing_df[keep_mask.values].copy()
        # Rename to internal column name for grid-fill compatibility.
        kept_existing = kept_existing.rename(columns={"complex_id": "station_complex_id"})
    else:
        kept_existing = None

    # Grid-fill the newly-fetched data.
    if monthly_frames:
        new_data = pd.concat(monthly_frames, ignore_index=True)
        new_data = create_complete_station_month_grid(new_data, station_id=args.station_id)
    else:
        new_data = None

    # Combine.
    parts = [p for p in [kept_existing, new_data] if p is not None and not p.empty]
    if not parts:
        print("No complete months were produced. Nothing written.")
        return 1

    combined = pd.concat(parts, ignore_index=True)
    output_df = format_output(combined)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_df.to_csv(output_path, index=False)

    kept_count = (
        kept_existing[["year", "month"]].drop_duplicates().shape[0]
        if kept_existing is not None and not kept_existing.empty
        else 0
    )
    total_months = output_df[["year", "month"]].drop_duplicates().shape[0]
    print()
    print("📊 Summary")
    print(f"   Months kept from disk:        {kept_count}")
    print(f"   Months fetched (new/refresh): {complete_count}")
    print(f"   Months incomplete (skipped):  {incomplete_count}")
    print(f"   Total months in output:       {total_months}")
    print(f"   Output rows:                  {len(output_df):,}")
    print(f"   Saved to:                     {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
