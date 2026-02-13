#!/usr/bin/env python3
"""Aggregate API station-level ridership to PUMA and NYC levels.

Reads results/ridership/monthly_ridership_station.csv and produces:
    - results/ridership/monthly_ridership_puma.csv
    - results/ridership/monthly_ridership_nyc.csv
"""

from pathlib import Path

import pandas as pd

SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parents[1]

DAY_GROUP_ORDER = ["total", "weekday", "weekend"]


def load_station_data() -> pd.DataFrame:
    """Load station-level ridership from the API output."""
    path = PROJECT_ROOT / "results" / "ridership" / "monthly_ridership_station.csv"
    df = pd.read_csv(path)
    # Back-calculate OMNY ridership for proper re-aggregation
    df["omny_ridership"] = (df["ridership"] * df["omny_pct"] / 100).round(2)
    return df


def load_station_puma_mapping() -> pd.DataFrame:
    """Load station-to-PUMA mapping."""
    path = PROJECT_ROOT / "references" / "stations" / "station_to_puma.csv"
    return pd.read_csv(path)


def aggregate_by_puma(station_df: pd.DataFrame, mapping_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate station ridership to PUMA level.

    Args:
        station_df: Station-level monthly ridership with omny_ridership column.
        mapping_df: Station-to-PUMA mapping (Complex ID -> PUMA).

    Returns:
        PUMA-level monthly ridership DataFrame.
    """
    merged = station_df.merge(
        mapping_df,
        left_on="complex_id",
        right_on="Complex ID",
        how="inner",
    )

    grouped = merged.groupby(
        ["PUMA", "year", "month", "period", "day_group"], as_index=False
    ).agg(ridership=("ridership", "sum"), omny_ridership=("omny_ridership", "sum"))

    grouped["omny_pct"] = 0.0
    nonzero = grouped["ridership"] > 0
    grouped.loc[nonzero, "omny_pct"] = (
        grouped.loc[nonzero, "omny_ridership"] / grouped.loc[nonzero, "ridership"] * 100
    ).round(2)

    grouped = grouped.rename(columns={"PUMA": "puma"})
    grouped["day_group"] = pd.Categorical(grouped["day_group"], categories=DAY_GROUP_ORDER, ordered=True)
    grouped = grouped.sort_values(["year", "month", "puma", "day_group"], ignore_index=True)
    grouped["day_group"] = grouped["day_group"].astype(str)

    return grouped[["puma", "year", "month", "period", "day_group", "ridership", "omny_pct"]]


def aggregate_to_nyc(station_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate station ridership to NYC-wide level.

    Args:
        station_df: Station-level monthly ridership with omny_ridership column.

    Returns:
        NYC-level monthly ridership DataFrame.
    """
    grouped = station_df.groupby(
        ["year", "month", "period", "day_group"], as_index=False
    ).agg(ridership=("ridership", "sum"), omny_ridership=("omny_ridership", "sum"))

    grouped["omny_pct"] = 0.0
    nonzero = grouped["ridership"] > 0
    grouped.loc[nonzero, "omny_pct"] = (
        grouped.loc[nonzero, "omny_ridership"] / grouped.loc[nonzero, "ridership"] * 100
    ).round(2)

    grouped["day_group"] = pd.Categorical(grouped["day_group"], categories=DAY_GROUP_ORDER, ordered=True)
    grouped = grouped.sort_values(["year", "month", "day_group"], ignore_index=True)
    grouped["day_group"] = grouped["day_group"].astype(str)

    return grouped[["year", "month", "period", "day_group", "ridership", "omny_pct"]]


def main() -> None:
    """Load station data, aggregate to PUMA and NYC, and save."""
    print("📥 Loading station-level ridership from API output...")
    station_df = load_station_data()
    mapping_df = load_station_puma_mapping()

    print("🏘️  Aggregating to PUMA level...")
    puma_df = aggregate_by_puma(station_df, mapping_df)

    print("🏙️  Aggregating to NYC level...")
    nyc_df = aggregate_to_nyc(station_df)

    output_dir = PROJECT_ROOT / "results" / "ridership"
    output_dir.mkdir(parents=True, exist_ok=True)

    puma_path = output_dir / "monthly_ridership_puma.csv"
    nyc_path = output_dir / "monthly_ridership_nyc.csv"

    puma_df.to_csv(puma_path, index=False)
    print(f"✅ Saved PUMA data ({len(puma_df):,} rows) → {puma_path.relative_to(PROJECT_ROOT)}")

    nyc_df.to_csv(nyc_path, index=False)
    print(f"✅ Saved NYC data ({len(nyc_df):,} rows) → {nyc_path.relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
    main()
