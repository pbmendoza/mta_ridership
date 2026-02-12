#!/usr/bin/env python3
"""Download MTA ridership data by month via the SODA3 API.

This script downloads monthly ridership records for all years defined in
references/dataset_id_on_nyopendata.json, or for a specific year/month.

Usage:
    python scripts/local/data/update_ridership_data.py                          # all years/months
    python scripts/local/data/update_ridership_data.py --year 2025              # all months in 2025
    python scripts/local/data/update_ridership_data.py --year 2025 --month 6    # June 2025 only
    python scripts/local/data/update_ridership_data.py --force                  # overwrite existing files

Environment variables (or set in .env at repo root):
    SOCRATA_APP_TOKEN      App token for Socrata API
    SOCRATA_SECRET_TOKEN   Secret token for Socrata API (optional)
"""

from __future__ import annotations

import sys
from pathlib import Path

# Ensure repo root is on sys.path so that ``scripts.utils`` is importable.
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from scripts.utils.download import DatasetConfig, run_download_pipeline

RIDERSHIP_CONFIG = DatasetConfig(
    description="Download MTA ridership data by month via SODA3 API.",
    config_filename="dataset_id_on_nyopendata.json",
    output_subdir="ridership",
    timestamp_field="transit_timestamp",
    soda_order_clause=(
        "transit_timestamp ASC, "
        "station_complex_id ASC, "
        "payment_method ASC, "
        "fare_class_category ASC, "
        ":id ASC"
    ),
    column_order=[
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
    ],
    example_year="2025",
)


if __name__ == "__main__":
    sys.exit(run_download_pipeline(RIDERSHIP_CONFIG))
