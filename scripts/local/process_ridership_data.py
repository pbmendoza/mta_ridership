"""Processes staged MTA ridership data to create daily ridership by payment method.

This script loads a specified staged ridership data file and aggregates
ridership data by date, station complex, and payment method, keeping
each payment method as a separate row.

Usage:
    python scripts/local/process_ridership_data.py
    python scripts/local/process_ridership_data.py --filename ridership_2020_2024.csv
"""
import argparse
from pathlib import Path

import pandas as pd


def process_ridership_data(filename: str) -> None:
    """Loads staged data, processes it, and saves the result.

    The processing involves:
    1.  Breaking the transit_timestamp into a date.
    2.  Grouping by date, station complex, and payment method.
    3.  Summing ridership for each combination.

    Args:
        filename: The name of the staged CSV file to process.
    """
    # Define project root and paths
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
    staged_data_path = PROJECT_ROOT / 'data' / 'local' / 'staging' / 'ridership' / filename
    processed_path = PROJECT_ROOT / 'data' / 'local' / 'processed' / 'ridership'

    # Create processed data directory if it doesn't exist
    processed_path.mkdir(parents=True, exist_ok=True)

    # Load the staged ridership data
    print(f"🔄 Loading data from {staged_data_path}...")
    df = pd.read_csv(staged_data_path, parse_dates=['transit_timestamp'])
    
    # Normalize payment method to uppercase to handle inconsistencies
    df['payment_method'] = df['payment_method'].str.upper()

    # 1. Break the transit_timestamp column into a date column
    print("📅 Extracting date from timestamp...")
    df['date'] = df['transit_timestamp'].dt.date

    # 2. Group usage data by date, station_complex_id, and payment_method
    print("📊 Grouping data by date, station, and payment method...")
    daily_ridership = df.groupby(
        ['date', 'station_complex_id', 'payment_method']
    )['ridership'].sum().reset_index()

    # 3. Format the final output
    print("📋 Formatting final output...")
    final_df = daily_ridership[[
        'date',
        'station_complex_id',
        'payment_method',
        'ridership'
    ]]

    # Save the processed data
    output_filename = f"processed_{filename}"
    output_path = processed_path / output_filename
    print(f"💾 Saving processed data to {output_path}...")
    final_df.to_csv(output_path, index=False)
    print("✅ Done!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process staged MTA ridership data to create daily ridership by payment method."
    )
    parser.add_argument(
        "--filename",
        type=str,
        default="2025.csv",
        help="The name of the staged ridership CSV file to process (e.g., 2025.csv, ridership_2020_2024.csv)."
    )
    args = parser.parse_args()

    process_ridership_data(filename=args.filename)
