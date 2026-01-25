"""Processes raw MTA ridership data and prepares it for staging.

This script loads a specified CSV file containing MTA ridership data,
filters it to include only subway-related entries, selects relevant columns,
and saves the processed data to the 'data/staging/ridership' directory.

Usage:
    python scripts/stage_ridership_data.py
    python scripts/stage_ridership_data.py --filename ridership_2020_2024.csv
    python scripts/stage_ridership_data.py --filename YOUR_FILE.csv

If no filename is provided, it defaults to '2025.csv'. Other available: 'ridership_2020_2024.csv' (historical).
"""
import argparse
from pathlib import Path


import pandas as pd


def stage_ridership_data(filename: str) -> None:
    """Load, validate, and stage MTA ridership data.

    This function reads a raw ridership CSV, accepting `ridership` values with or
    without thousands separators (e.g., "1,234" or "1234"), strictly parses them as
    integers, filters to subway entries, selects required columns, and writes a
    staged CSV.

    Args:
        filename: Name of the raw ridership CSV in `data/raw/ridership`.

    Raises:
        ValueError: If required columns are missing or `ridership` contains
            non-integer values or negative numbers.
    """
    # Define project root and paths
    PROJECT_ROOT = Path(__file__).resolve().parents[1]
    raw_data_path = PROJECT_ROOT / 'data' / 'raw' / 'ridership' / filename
    staging_path = PROJECT_ROOT / 'data' / 'staging' / 'ridership'

    # Create staging directory if it doesn't exist
    staging_path.mkdir(parents=True, exist_ok=True)

    # Load the ridership data
    print(f"🔄 Loading data from {raw_data_path}...")
    # Restrict to required columns and control dtypes for determinism
    usecols = ['transit_timestamp', 'transit_mode', 'station_complex_id', 'payment_method', 'ridership']
    dtypes = {
        'transit_mode': 'string',
        'station_complex_id': 'string',
        'payment_method': 'string',
    }
    df = pd.read_csv(
        raw_data_path,
        usecols=usecols,
        dtype=dtypes,
        thousands=',',  # accept both "1,234" and "1234" in ridership
        low_memory=False,
    )

    # Strictly parse ridership as integer and cast to nullable Int64
    try:
        df['ridership'] = pd.to_numeric(df['ridership'], errors='raise').astype('Int64')
    except Exception as exc:
        sample = df['ridership'].astype('string').head(5).tolist()
        raise ValueError(f"Failed to parse 'ridership' as integer. Example values: {sample}") from exc

    # Basic schema and value validations
    required = {'transit_timestamp', 'transit_mode', 'station_complex_id', 'payment_method', 'ridership'}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")
    if (df['ridership'].fillna(0) < 0).any():
        raise ValueError("Negative ridership values encountered")

    # Filter for subway data
    transit_modes_to_keep = ['subway']
    print(f"🚇 Filtering for transit modes: {transit_modes_to_keep}...")
    df_filtered = df[df['transit_mode'].isin(transit_modes_to_keep)].copy()

    # Select and rename columns
    columns_to_keep = [
        'transit_timestamp',
        'station_complex_id',
        'payment_method',
        'ridership'
    ]
    print(f"✂️ Selecting columns: {columns_to_keep}...")
    df_final = df_filtered[columns_to_keep]
    
    #! temp fix for data problem: the data include some entries where station_complex_id = '502' and they are incorrectly remained in the data.
    df_final = df_final[df_final['station_complex_id'] != '502']

    # Save the processed data
    output_path = staging_path / filename
    print(f"💾 Saving processed data to {output_path}...")
    df_final.to_csv(output_path, index=False)
    print("✅ Done!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Stage MTA ridership data by filtering and selecting columns."
    )
    parser.add_argument(
        "--filename",
        type=str,
        default="2025.csv",
        help="The name of the raw ridership CSV file to process (e.g., 2025.csv, ridership_2020_2024.csv)."
    )
    args = parser.parse_args()

    stage_ridership_data(filename=args.filename)
