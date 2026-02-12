#!/usr/bin/env python3
"""Calculate monthly baseline ridership from the 2017-2019 hourly ridership API.

Uses dataset t69i-h2me (MTA Subway Hourly Ridership: 2017-2019) via SODA API.
Produces station, PUMA, and NYC-level baselines with weekday/weekend/total breakdown.

January 2017 is excluded (first 12 days missing); January uses 2-year average.
Special cases from references/baseline_special_cases.csv are respected.

Output:
    results/baseline/monthly_baseline_station.csv
    results/baseline/monthly_baseline_puma.csv
    results/baseline/monthly_baseline_nyc.csv
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict, List, Set

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

DATASET_ID = "t69i-h2me"
AVAILABLE_YEARS = {2017, 2018, 2019}
DEFAULT_PAGE_SIZE = 50_000
DAY_GROUP_ORDER = ["total", "weekday", "weekend"]

SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parents[1]


# ---------------------------------------------------------------------------
# Special cases
# ---------------------------------------------------------------------------

def load_special_cases() -> Dict[int, Set[int]]:
    """Load special-case station baseline years from CSV.

    Returns:
        Mapping of complex_id -> set of allowed baseline years (intersected
        with AVAILABLE_YEARS).
    """
    path = PROJECT_ROOT / "references" / "baseline_special_cases.csv"
    df = pd.read_csv(path)
    cases: Dict[int, Set[int]] = {}
    for _, row in df.iterrows():
        cid = int(row["complex_id"])
        years = {int(y.strip()) for y in str(row["baseline_years"]).split(",")}
        cases[cid] = years & AVAILABLE_YEARS
    return cases


# ---------------------------------------------------------------------------
# API fetch
# ---------------------------------------------------------------------------

def fetch_day_group_for_year(
    session: requests.Session,
    endpoint: str,
    headers: Dict[str, str],
    year: int,
    is_weekday: bool,
) -> List[Dict[str, str]]:
    """Fetch station/month ridership totals for one year and day_group.

    Args:
        session: Requests session.
        endpoint: SODA3 endpoint URL.
        headers: HTTP headers with auth tokens.
        year: Calendar year to query.
        is_weekday: True for weekday, False for weekend.

    Returns:
        List of raw JSON rows from the API.
    """
    dow_filter = (
        "date_extract_dow(transit_timestamp) NOT IN (0, 6)"
        if is_weekday
        else "date_extract_dow(transit_timestamp) IN (0, 6)"
    )
    start_ts = f"{year}-01-01T00:00:00"
    end_ts = f"{year + 1}-01-01T00:00:00"

    all_rows: List[Dict[str, str]] = []
    offset = 0

    while True:
        params = {
            "$select": (
                "station_complex_id, "
                "date_extract_m(transit_timestamp) as month, "
                "sum(ridership) as ridership"
            ),
            "$where": (
                "transit_mode = 'subway' "
                "AND station_complex_id != '502' "
                f"AND transit_timestamp >= '{start_ts}' "
                f"AND transit_timestamp < '{end_ts}' "
                f"AND {dow_filter}"
            ),
            "$group": "station_complex_id, month",
            "$order": "station_complex_id ASC, month ASC",
            "$limit": str(DEFAULT_PAGE_SIZE),
            "$offset": str(offset),
        }
        rows = request_json(
            session, endpoint, params, headers, timeout=180
        )
        if not rows:
            break
        all_rows.extend(rows)
        if len(rows) < DEFAULT_PAGE_SIZE:
            break
        offset += len(rows)

    return all_rows


def fetch_all(session: requests.Session, endpoint: str, headers: Dict[str, str]) -> pd.DataFrame:
    """Fetch weekday + weekend ridership for all years.

    Returns:
        DataFrame with columns: station_complex_id, year, month, ridership, day_group.
    """
    all_rows: List[Dict[str, str]] = []

    for year in sorted(AVAILABLE_YEARS):
        for is_weekday in [True, False]:
            dg = "weekday" if is_weekday else "weekend"
            print(f"   📥 {year} {dg}...")
            rows = fetch_day_group_for_year(
                session, endpoint, headers, year, is_weekday
            )
            for r in rows:
                r["year"] = str(year)
                r["day_group"] = dg
            all_rows.extend(rows)
            print(f"      ✅ {len(rows):,} station/month rows")

    if not all_rows:
        return pd.DataFrame(
            columns=["station_complex_id", "year", "month", "ridership", "day_group"]
        )

    df = pd.DataFrame(all_rows)
    df["station_complex_id"] = df["station_complex_id"].astype(str)
    df["year"] = df["year"].astype(int)
    df["month"] = df["month"].astype(int)
    df["ridership"] = pd.to_numeric(df["ridership"], errors="coerce").fillna(0.0)
    return df


# ---------------------------------------------------------------------------
# Averaging logic
# ---------------------------------------------------------------------------

def compute_baseline(
    raw: pd.DataFrame, special_cases: Dict[int, Set[int]]
) -> pd.DataFrame:
    """Average yearly ridership per station/month/day_group into a baseline.

    Handles:
    - January 2017 exclusion (incomplete data).
    - Special-case stations with restricted year sets.
    - Default stations use all 3 years (2 for January).

    Args:
        raw: Combined weekday+weekend DataFrame with year-level rows.
        special_cases: complex_id -> allowed years within 2017-2019.

    Returns:
        Baseline DataFrame with columns: complex_id, month, day_group, ridership.
    """
    # Drop January 2017 globally
    raw = raw[~((raw["year"] == 2017) & (raw["month"] == 1))].copy()

    special_ids = set(special_cases.keys())
    raw["_cid_int"] = pd.to_numeric(raw["station_complex_id"], errors="coerce")
    df_regular = raw[~raw["_cid_int"].isin(special_ids)].copy()
    df_special = raw[raw["_cid_int"].isin(special_ids)].copy()

    results: List[pd.DataFrame] = []

    # --- Regular stations ---
    if not df_regular.empty:
        totals = df_regular.groupby(
            ["station_complex_id", "month", "day_group"], as_index=False
        )["ridership"].sum()

        # Year count: 2 for January (2018-2019), 3 for other months
        totals["n_years"] = totals["month"].apply(lambda m: 2 if m == 1 else 3)
        totals["ridership"] = totals["ridership"] / totals["n_years"]
        results.append(
            totals[["station_complex_id", "month", "day_group", "ridership"]]
        )

    # --- Special-case stations ---
    for cid, allowed_years in special_cases.items():
        cid_str = str(cid)
        station_rows = df_special[df_special["station_complex_id"] == cid_str]
        if station_rows.empty:
            continue

        for month in range(1, 13):
            valid_years = allowed_years - ({2017} if month == 1 else set())
            month_rows = station_rows[
                (station_rows["month"] == month)
                & (station_rows["year"].isin(valid_years))
            ]
            n_years = len(valid_years)
            if n_years == 0:
                continue

            for dg in ["weekday", "weekend"]:
                total = month_rows.loc[
                    month_rows["day_group"] == dg, "ridership"
                ].sum()
                results.append(
                    pd.DataFrame(
                        [
                            {
                                "station_complex_id": cid_str,
                                "month": month,
                                "day_group": dg,
                                "ridership": total / n_years,
                            }
                        ]
                    )
                )

    if not results:
        return pd.DataFrame(
            columns=["complex_id", "month", "day_group", "ridership"]
        )

    baseline = pd.concat(results, ignore_index=True)

    # Derive total = weekday + weekend
    totals = (
        baseline.groupby(["station_complex_id", "month"], as_index=False)[
            "ridership"
        ]
        .sum()
        .assign(day_group="total")
    )
    baseline = pd.concat([baseline, totals], ignore_index=True)

    baseline = baseline.rename(columns={"station_complex_id": "complex_id"})
    return baseline[["complex_id", "month", "day_group", "ridership"]]


# ---------------------------------------------------------------------------
# Grid-fill
# ---------------------------------------------------------------------------

def grid_fill(baseline: pd.DataFrame) -> pd.DataFrame:
    """Ensure every non-SIR station x month x day_group exists (fill 0)."""
    stations_file = (
        PROJECT_ROOT / "references" / "stations" / "stations_complexes_official.csv"
    )
    stations_df = pd.read_csv(stations_file)
    stations_df = stations_df[stations_df["Daytime Routes"] != "SIR"]
    all_ids = stations_df["Complex ID"].astype(str).unique()

    grid_parts: List[pd.DataFrame] = []
    for month in range(1, 13):
        for dg in DAY_GROUP_ORDER:
            grid_parts.append(
                pd.DataFrame(
                    {
                        "complex_id": all_ids,
                        "month": month,
                        "day_group": dg,
                    }
                )
            )
    grid = pd.concat(grid_parts, ignore_index=True)

    merged = grid.merge(
        baseline, on=["complex_id", "month", "day_group"], how="left"
    )
    merged["ridership"] = merged["ridership"].fillna(0.0)
    return merged


# ---------------------------------------------------------------------------
# Aggregation (PUMA / NYC)
# ---------------------------------------------------------------------------

def aggregate_to_puma(station_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate station baseline to PUMA level."""
    mapping_path = PROJECT_ROOT / "references" / "stations" / "station_to_puma.csv"
    mapping = pd.read_csv(mapping_path)
    mapping["Complex ID"] = mapping["Complex ID"].astype(str)

    merged = station_df.merge(
        mapping, left_on="complex_id", right_on="Complex ID", how="inner"
    )
    grouped = merged.groupby(
        ["PUMA", "month", "day_group"], as_index=False
    ).agg(ridership=("ridership", "sum"))
    grouped = grouped.rename(columns={"PUMA": "puma"})
    grouped["day_group"] = pd.Categorical(
        grouped["day_group"], categories=DAY_GROUP_ORDER, ordered=True
    )
    grouped = grouped.sort_values(
        ["month", "puma", "day_group"], ignore_index=True
    )
    grouped["day_group"] = grouped["day_group"].astype(str)
    return grouped[["puma", "month", "day_group", "ridership"]]


def aggregate_to_nyc(station_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate station baseline to NYC-wide level."""
    grouped = station_df.groupby(
        ["month", "day_group"], as_index=False
    ).agg(ridership=("ridership", "sum"))
    grouped["day_group"] = pd.Categorical(
        grouped["day_group"], categories=DAY_GROUP_ORDER, ordered=True
    )
    grouped = grouped.sort_values(["month", "day_group"], ignore_index=True)
    grouped["day_group"] = grouped["day_group"].astype(str)
    return grouped[["month", "day_group", "ridership"]]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    """Fetch 2017-2019 ridership, compute baseline, and save."""
    app_token = load_socrata_token()
    secret_token = load_socrata_secret_token()
    if not app_token:
        print(
            "⚠️  No Socrata app token found. Running without authentication "
            "(lower rate limits)."
        )
    headers = build_headers(app_token, secret_token)
    endpoint = get_soda_endpoint(DATASET_ID)

    session = requests.Session()
    try:
        print("📥 Fetching ridership from API (6 queries: 3 years × 2 day_groups)...")
        raw = fetch_all(session, endpoint, headers)
        print(f"   📊 Total: {len(raw):,} rows")
    finally:
        session.close()

    print("🔢 Computing baseline averages (with special cases)...")
    special_cases = load_special_cases()
    baseline = compute_baseline(raw, special_cases)

    print("📊 Grid-filling missing station/month/day_group combinations...")
    baseline = grid_fill(baseline)

    # Sort output
    baseline["_cid_sort"] = pd.to_numeric(baseline["complex_id"], errors="coerce")
    baseline["day_group"] = pd.Categorical(
        baseline["day_group"], categories=DAY_GROUP_ORDER, ordered=True
    )
    baseline = baseline.sort_values(
        ["month", "_cid_sort", "day_group"], ignore_index=True
    )
    baseline["day_group"] = baseline["day_group"].astype(str)
    baseline = baseline.drop(columns=["_cid_sort"])

    # Round ridership
    baseline["ridership"] = baseline["ridership"].round(2)

    # Save
    output_dir = PROJECT_ROOT / "results" / "baseline"
    output_dir.mkdir(parents=True, exist_ok=True)

    station_path = output_dir / "monthly_baseline_station.csv"
    baseline.to_csv(station_path, index=False)
    print(
        f"✅ Station baseline ({len(baseline):,} rows) "
        f"→ {station_path.relative_to(PROJECT_ROOT)}"
    )

    print("🏘️  Aggregating to PUMA level...")
    puma_df = aggregate_to_puma(baseline)
    puma_df["ridership"] = puma_df["ridership"].round(2)
    puma_path = output_dir / "monthly_baseline_puma.csv"
    puma_df.to_csv(puma_path, index=False)
    print(
        f"✅ PUMA baseline ({len(puma_df):,} rows) "
        f"→ {puma_path.relative_to(PROJECT_ROOT)}"
    )

    print("🏙️  Aggregating to NYC level...")
    nyc_df = aggregate_to_nyc(baseline)
    nyc_df["ridership"] = nyc_df["ridership"].round(2)
    nyc_path = output_dir / "monthly_baseline_nyc.csv"
    nyc_df.to_csv(nyc_path, index=False)
    print(
        f"✅ NYC baseline ({len(nyc_df):,} rows) "
        f"→ {nyc_path.relative_to(PROJECT_ROOT)}"
    )


if __name__ == "__main__":
    main()
