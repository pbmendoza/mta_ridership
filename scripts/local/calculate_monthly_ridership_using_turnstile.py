#!/usr/bin/env python3
"""
Calculate non-averaged monthly ridership totals from processed turnstile daily data.

This script reads station-level daily ridership records and aggregates them into
monthly totals for 2015–2019. It does not apply averaging, station-specific
baseline year overrides, or outlier filtering. It is intended for historical monthly
exploration or for manual comparisons with the baseline pipeline outputs.

Input:
- data/processed/turnstile/daily_ridership.csv
- references/stations/stations_complexes_official.csv

Output:
    - data/local/ridership_turnstile/raw_monthly_turnstile_2015_2019.csv
- logs/calculate_monthly_ridership_using_turnstile.log

Usage:
    python scripts/local/calculate_monthly_ridership_using_turnstile.py
"""

import pandas as pd
from pathlib import Path
import logging
from datetime import datetime


def find_project_root() -> Path:
    """Find the project root by looking for .git directory."""
    candidates = [Path(__file__).resolve(), *Path(__file__).resolve().parents]

    for current in candidates:
        if (current / '.git').exists():
            return current

    cwd = Path.cwd()
    if (cwd / '.git').exists():
        return cwd

    for parent in cwd.parents:
        if (parent / '.git').exists():
            return parent

    raise RuntimeError("Could not find project root directory.")


def setup_logging(base_dir: Path) -> logging.Logger:
    """Set up logging configuration."""
    log_dir = base_dir / "logs"
    log_dir.mkdir(exist_ok=True)
    
    log_file = log_dir / "calculate_monthly_ridership_using_turnstile.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    
    return logging.getLogger(__name__)


def calculate_raw_monthly_totals(base_dir: Path, logger: logging.Logger):
    """Calculate raw monthly ridership totals from daily data."""
    
    # Define paths
    input_file = base_dir / "data" / "processed" / "turnstile" / "daily_ridership.csv"
    stations_file = base_dir / "references" / "stations" / "stations_complexes_official.csv"
    output_dir = base_dir / "data" / "local" / "ridership_turnstile"
    output_file = output_dir / "raw_monthly_turnstile_2015_2019.csv"
    
    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Reading daily ridership data from {input_file.relative_to(base_dir)}")
    
    # Read daily ridership data
    df = pd.read_csv(input_file)
    
    # Convert DATE to datetime
    df['DATE'] = pd.to_datetime(df['DATE'])
    
    # Filter for 2015-2019
    df_filtered = df[(df['YEAR'] >= 2015) & (df['YEAR'] <= 2019)].copy()
    
    logger.info(f"Loaded {len(df_filtered):,} records for 2015-2019")
    logger.info(f"Processing {df_filtered['Complex ID'].nunique()} unique stations")
    
    # Group by station, year, and month to get monthly totals
    monthly_totals = df_filtered.groupby(['Complex ID', 'YEAR', 'MONTH']).agg({
        'ENTRIES': 'sum',
        'EXITS': 'sum'
    }).reset_index()
    
    # Rename columns for clarity
    monthly_totals.columns = ['complex_id', 'year', 'month', 'entries', 'exits']
    
    # Sort by complex_id, year, month for readability
    monthly_totals = monthly_totals.sort_values(['complex_id', 'year', 'month'])
    
    # Load station names
    logger.info(f"Reading station names from {stations_file.relative_to(base_dir)}")
    stations_df = pd.read_csv(stations_file)
    
    # Create station name by concatenating Stop Name and Display Name
    stations_df['station_name'] = stations_df['Stop Name'] + ' ' + stations_df['Display Name']
    
    # Keep only Complex ID and station_name
    stations_lookup = stations_df[['Complex ID', 'station_name']].drop_duplicates()
    
    # Merge with monthly totals
    monthly_totals = monthly_totals.merge(
        stations_lookup,
        left_on='complex_id',
        right_on='Complex ID',
        how='left'
    )
    
    # Drop the duplicate Complex ID column
    monthly_totals = monthly_totals.drop('Complex ID', axis=1)
    
    # Reorder columns for better readability
    monthly_totals = monthly_totals[['complex_id', 'station_name', 'year', 'month', 'entries', 'exits']]
    
    # Log summary statistics
    logger.info(f"Generated {len(monthly_totals):,} monthly records")
    logger.info(f"Date range: {monthly_totals['year'].min()}-{monthly_totals['year'].max()}")
    logger.info(f"Total entries: {monthly_totals['entries'].sum():,}")
    logger.info(f"Total exits: {monthly_totals['exits'].sum():,}")
    
    # Sample output for verification
    logger.info("\nSample of monthly totals (first 5 records):")
    sample = monthly_totals.head()
    for _, row in sample.iterrows():
        station_name = row['station_name'] if pd.notna(row['station_name']) else f"Complex {row['complex_id']}"
        logger.info(f"  {station_name}: {int(row['year'])}-{int(row['month']):02d} - "
                   f"Entries: {row['entries']:,}, Exits: {row['exits']:,}")
    
    # Save to CSV
    monthly_totals.to_csv(output_file, index=False)
    logger.info(f"\nSaved raw monthly totals to {output_file.relative_to(base_dir)}")
    
    # Create a summary by year
    yearly_summary = monthly_totals.groupby('year').agg({
        'entries': 'sum',
        'exits': 'sum'
    })
    
    logger.info("\nYearly summary:")
    for year, row in yearly_summary.iterrows():
        logger.info(f"  {year}: Entries: {row['entries']:,}, Exits: {row['exits']:,}")
    
    # Station coverage check
    stations_per_year = monthly_totals.groupby('year')['complex_id'].nunique()
    logger.info("\nStations with data per year:")
    for year, count in stations_per_year.items():
        logger.info(f"  {year}: {count} stations")
    
    return monthly_totals


def main():
    """Main execution function."""
    # Find project root
    base_dir = find_project_root()
    
    # Set up logging
    logger = setup_logging(base_dir)
    
    logger.info("="*60)
    logger.info("Starting raw monthly ridership calculation")
    logger.info("="*60)
    start_time = datetime.now()
    
    try:
        monthly_data = calculate_raw_monthly_totals(base_dir, logger)
        
        elapsed_time = datetime.now() - start_time
        logger.info(f"\nProcessing completed in {elapsed_time.total_seconds():.2f} seconds")
        logger.info("="*60)
        
    except Exception as e:
        logger.error(f"Error during calculation: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
