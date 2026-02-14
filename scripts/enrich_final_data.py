#!/usr/bin/env python3
"""
    Enrich and Sort Final Data

This script enriches the final ridership analysis files by adding human-readable names
for PUMAs and subway station complexes, then sorts the data for consistent output.
"""

from pathlib import Path
import logging
import sys
from typing import Tuple

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.utils.runtime import find_project_root, setup_script_logging


def output_status(path: Path, new_count: int) -> str:
    """Describe output state in operator-friendly language."""
    if not path.exists():
        return "Created new output file."

    try:
        previous_count = len(pd.read_csv(path))
    except Exception:
        return "Updated (could not compare to previous file)."

    if previous_count == new_count:
        return "Up to date (row count unchanged)."
    return f"Updated ({previous_count:,} → {new_count:,} rows)."


def load_puma_crosswalk(base_dir: Path) -> pd.DataFrame:
    """Load PUMA crosswalk data."""
    puma_file = base_dir / "data" / "external" / "puma" / "nyc_puma_crosswalk_2020.csv"
    df = pd.read_csv(puma_file)
    df["puma_code"] = df["puma_code"].astype(str)
    return df


def load_station_reference(base_dir: Path) -> pd.DataFrame:
    """Load station complex reference data."""
    station_file = base_dir / "references" / "stations" / "stations_complexes_official.csv"
    df = pd.read_csv(station_file)
    df = df[["Complex ID", "Stop Name"]].copy()
    df.columns = ["complex_id", "station_name"]
    df["complex_id"] = df["complex_id"].astype(str)
    return df.drop_duplicates(subset=["complex_id"])


def enrich_puma_data(
    base_dir: Path,
    logger: logging.Logger,
    input_dir: Path,
    output_dir: Path,
) -> Tuple[int, int, str]:
    """Enrich PUMA ridership data with PUMA names."""
    puma_file = input_dir / "monthly_ridership_puma.csv"
    logger.info(f"PUMA data: {puma_file.name}")

    df = pd.read_csv(puma_file)
    original_count = len(df)
    df["puma"] = df["puma"].astype(str)

    puma_crosswalk = load_puma_crosswalk(base_dir)

    if "puma_name" in df.columns:
        df = df.drop("puma_name", axis=1)

    df_enriched = df.merge(
        puma_crosswalk[["puma_code", "puma_name"]],
        left_on="puma",
        right_on="puma_code",
        how="left",
    ).drop("puma_code", axis=1)

    cols = list(df_enriched.columns)
    cols.remove("puma_name")
    puma_idx = cols.index("puma")
    cols.insert(puma_idx + 1, "puma_name")
    df_enriched = df_enriched[cols]

    missing_mask = df_enriched["puma_name"].isna()
    missing_count = int(missing_mask.sum())
    if missing_count:
        logger.warning(
            f"   {missing_count:,} PUMA rows are still missing a readable station area name."
        )

    matched_count = original_count - missing_count
    df_enriched = df_enriched.sort_values(["year", "month", "puma"], ignore_index=True)

    output_path = output_dir / "monthly_ridership_puma.csv"
    status = output_status(output_path, len(df_enriched))
    df_enriched.to_csv(output_path, index=False)
    logger.info(f"   Wrote {output_path.name}: {status}")

    return matched_count, missing_count, status


def enrich_station_data(
    base_dir: Path,
    logger: logging.Logger,
    input_dir: Path,
    output_dir: Path,
) -> Tuple[int, int, str]:
    """Enrich station ridership data with station names."""
    station_file = input_dir / "monthly_ridership_station.csv"
    logger.info(f"Station data: {station_file.name}")

    df = pd.read_csv(station_file)
    original_count = len(df)
    df["complex_id"] = df["complex_id"].astype(str)

    station_ref = load_station_reference(base_dir)

    if "station_name" in df.columns:
        df = df.drop("station_name", axis=1)

    df_enriched = df.merge(station_ref, on="complex_id", how="left")

    cols = list(df_enriched.columns)
    cols.remove("station_name")
    complex_idx = cols.index("complex_id")
    cols.insert(complex_idx + 1, "station_name")
    df_enriched = df_enriched[cols]

    missing_mask = df_enriched["station_name"].isna()
    missing_count = int(missing_mask.sum())
    if missing_count:
        logger.warning(
            f"   {missing_count:,} station rows are still missing a readable station name."
        )

    matched_count = original_count - missing_count
    df_enriched = df_enriched.sort_values(["year", "month", "station_name"], ignore_index=True)

    output_path = output_dir / "monthly_ridership_station.csv"
    status = output_status(output_path, len(df_enriched))
    df_enriched.to_csv(output_path, index=False)
    logger.info(f"   Wrote {output_path.name}: {status}")

    return matched_count, missing_count, status


def sort_nyc_data(
    base_dir: Path,
    logger: logging.Logger,
    input_dir: Path,
    output_dir: Path,
) -> Tuple[int, str]:
    """Sort NYC-wide ridership data by year and month."""
    nyc_file = input_dir / "monthly_ridership_nyc.csv"
    logger.info(f"NYC data: {nyc_file.name}")

    df = pd.read_csv(nyc_file)
    record_count = len(df)
    df = df.sort_values(["year", "month"], ignore_index=True)

    output_path = output_dir / "monthly_ridership_nyc.csv"
    status = output_status(output_path, len(df))
    df.to_csv(output_path, index=False)
    logger.info(f"   Wrote {output_path.name}: {status}")

    return record_count, status


def main() -> None:
    """Main execution function."""
    base_dir = find_project_root()
    logger, _ = setup_script_logging(
        base_dir=base_dir,
        logger_name=__name__,
        timestamped_prefix="enrich_final_data",
        fmt="%(message)s",
    )

    logger.info("Updating final data files with readable names")

    try:
        input_dir = base_dir / "data" / "api" / "processed"
        output_dir = base_dir / "data" / "production"
        output_dir.mkdir(parents=True, exist_ok=True)

        puma_matched, puma_missing, puma_status = enrich_puma_data(
            base_dir, logger, input_dir, output_dir
        )
        station_matched, station_missing, station_status = enrich_station_data(
            base_dir, logger, input_dir, output_dir
        )
        nyc_records, nyc_status = sort_nyc_data(base_dir, logger, input_dir, output_dir)

        puma_total = puma_matched + puma_missing
        station_total = station_matched + station_missing
        all_up_to_date = all(
            "Up to date" in status or "up to date" in status
            for status in [puma_status, station_status, nyc_status]
        )

        logger.info("")
        logger.info("Step 4 summary")
        logger.info(f"Status: {'Up to date' if all_up_to_date else 'Updated'}")
        logger.info(f"PUMA file: {puma_status}")
        logger.info(f"Station file: {station_status}")
        logger.info(f"NYC file: {nyc_status}")
        logger.info(
            f"PUMA names matched: {puma_matched:,} of {puma_total:,} rows "
            f"({(puma_matched / puma_total * 100) if puma_total else 0:.1f}%)"
        )
        logger.info(
            f"Station names matched: {station_matched:,} of {station_total:,} rows "
            f"({(station_matched / station_total * 100) if station_total else 0:.1f}%)"
        )
        logger.info(f"NYC rows sorted: {nyc_records:,}")
    except Exception as e:
        logger.error(f"Error during processing: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
