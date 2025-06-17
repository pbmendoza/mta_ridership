"""Processes raw MTA ridership data and prepares it for staging.

This script loads a specified CSV file containing MTA ridership data,
filters it to include only subway-related entries, selects relevant columns,
and saves the processed data to the 'data/staging/ridership' directory.

Usage:
    python scripts/stage_ridership_data.py
    python scripts/stage_ridership_data.py --filename ridership_2020_2024.csv
    python scripts/stage_ridership_data.py --filename YOUR_FILE.csv

If no filename is provided, it defaults to 'ridership_2025.csv'. Other available are 'ridership_2020_2024.csv'.
"""
import argparse
from pathlib import Path


import pandas as pd


def stage_ridership_data(filename: str) -> None:
    """Loads, filters, and stages MTA ridership data.

    Args:
        filename: The name of the CSV file to process.
    """
    # Define project root and paths
    PROJECT_ROOT = Path(__file__).resolve().parents[1]
    raw_data_path = PROJECT_ROOT / 'data' / 'raw' / 'ridership' / filename
    staging_path = PROJECT_ROOT / 'data' / 'staging' / 'ridership'

    # Create staging directory if it doesn't exist
    staging_path.mkdir(parents=True, exist_ok=True)

    # Load the ridership data
    print(f"🔄 Loading data from {raw_data_path}...")
    # Specify dtypes to prevent warnings and ensure correct data types
    dtypes = {
        'station_complex_id': str,
        'ridership': int
    }
    df = pd.read_csv(raw_data_path, dtype=dtypes)

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
    
    #! temp fix for data problem: the data include some entries where station_complex_id = 502 and they are incorrectly remained in the data.
    
    df_final = df_final[df_final['station_complex_id'] != 502]

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
        default="ridership_2025.csv",
        help="The name of the ridership CSV file to process."
    )
    args = parser.parse_args()

    stage_ridership_data(filename=args.filename)
