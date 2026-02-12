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

import sys
from pathlib import Path

# Ensure repo root is on sys.path so that ``scripts.utils`` is importable.
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from scripts.utils.download import DatasetConfig, run_download_pipeline

TURNSTILE_CONFIG = DatasetConfig(
    description="Download MTA turnstile data by month via SODA3 API.",
    config_filename="turnstile_data_nyopendata.json",
    output_subdir="turnstile",
    timestamp_field="date",
    soda_order_clause=(
        "date ASC, "
        "c_a ASC, "
        "unit ASC, "
        "scp ASC, "
        "time ASC, "
        ":id ASC"
    ),
    column_order=[
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
    ],
    example_year="2019",
)


if __name__ == "__main__":
    sys.exit(run_download_pipeline(TURNSTILE_CONFIG))
