#!/usr/bin/env python3
"""
Calculate Monthly Baseline Ridership from Daily Turnstile Data

This script processes daily ridership data to create three monthly baseline files
using data from 2015-2019:
1. By station (using complex ID)
2. By PUMA (Public Use Microdata Area)
3. By NYC (citywide aggregation)

Monthly Average Calculation:
- Daily ridership data from 2015-2019 is filtered (5 years of data)
- For each station/month combination, all daily entries and exits are summed
- These monthly totals represent 5 years worth of data for that month
- The totals are divided by 5 to get the average monthly ridership
- Example: January entries for Station X across 2015-2019 = 500,000 total
          Monthly average for January at Station X = 500,000 / 5 = 100,000

Special Cases:
- Some stations require different baseline years due to closures or recent openings
- Configuration is stored in: references/baseline_special_cases.csv
- To add/modify special cases, edit the CSV configuration file

Features:
- Processes daily ridership data from 2015-2019
- Creates monthly averages for entries and exits
- Generates three separate baseline files
- Uses station-to-PUMA mapping for geographic aggregation

Usage:
    python scripts/calculate_baseline.py
    python scripts/calculate_baseline.py --years 2017 2018 2019

Output:
    - results/baseline/monthly_baseline_station.csv
    - results/baseline/monthly_baseline_puma.csv
    - results/baseline/monthly_baseline_nyc.csv
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

# Default baseline years for stations not in special cases
DEFAULT_BASELINE_YEARS = [2015, 2016, 2017, 2018, 2019]


def find_project_root() -> Path:
    """Find the project root by looking for .git directory."""
    current = Path.cwd()
    
    if (current / '.git').exists():
        return current
    
    for parent in current.parents:
        if (parent / '.git').exists():
            return parent
    
    return current


def setup_logging(base_dir: Path) -> logging.Logger:
    """Set up logging configuration."""
    log_dir = base_dir / "logs"
    log_dir.mkdir(exist_ok=True)
    
    log_file = log_dir / "calculate_baseline.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    
    return logging.getLogger(__name__)


def load_special_cases_config(base_dir: Path, logger: logging.Logger) -> dict:
    """Load special cases configuration from CSV file."""
    config_file = base_dir / "references" / "baseline_special_cases.csv"
    
    logger.info(f"Loading special cases configuration from {config_file.relative_to(base_dir)}")
    
    # Read CSV file
    df_config = pd.read_csv(config_file)
    
    # Validate required columns exist
    required_columns = ['complex_id', 'baseline_years']
    missing_columns = [col for col in required_columns if col not in df_config.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns in configuration file: {missing_columns}")
    
    # Convert to dictionary format
    special_cases = {}
    for idx, row in df_config.iterrows():
        # Validate required fields are not missing
        if pd.isna(row['complex_id']) or row['complex_id'] == '':
            raise ValueError(f"Missing complex_id at row {idx + 2} in {config_file.name}")
        
        if pd.isna(row['baseline_years']) or row['baseline_years'] == '':
            raise ValueError(f"Missing baseline_years at row {idx + 2} for complex_id {row['complex_id']}")
        
        try:
            complex_id = int(row['complex_id'])
        except ValueError:
            raise ValueError(f"Invalid complex_id '{row['complex_id']}' at row {idx + 2} - must be an integer")
        
        # Parse baseline_years from string to list of integers
        # Handle both single year (e.g., "2019") and multiple years (e.g., "2018,2019")
        baseline_years_str = str(row['baseline_years'])
        try:
            baseline_years = [int(year.strip()) for year in baseline_years_str.split(',')]
        except ValueError:
            raise ValueError(f"Invalid baseline_years '{baseline_years_str}' at row {idx + 2} - must be comma-separated integers")
        
        special_cases[complex_id] = {
            'station_name': row['station_name'] if pd.notna(row['station_name']) else f"Station {complex_id}",
            'baseline_years': baseline_years,
            'reason': row['reason'] if pd.notna(row['reason']) else '',
            'notes': row['notes'] if pd.notna(row['notes']) else ''
        }
    
    logger.info(f"Loaded {len(special_cases)} special case configurations")
    
    return special_cases


def calculate_baselines(base_dir: Path, logger: logging.Logger,
                        baseline_years: list[int] | None = None):
    """Calculate monthly baselines from daily ridership data.

    Args:
        base_dir: Project root directory.
        logger: Logger instance.
        baseline_years: Years to average over for regular stations.
            Defaults to DEFAULT_BASELINE_YEARS when None.
    """
    if baseline_years is None:
        baseline_years = DEFAULT_BASELINE_YEARS
    baseline_years = sorted(baseline_years)
    
    # Define paths
    input_file = base_dir / "data" / "processed" / "turnstile" / "daily_ridership.csv"
    station_puma_file = base_dir / "references" / "stations" / "station_to_puma.csv"
    output_dir = base_dir / "results" / "baseline"
    
    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Output files
    station_output = output_dir / "monthly_baseline_station.csv"
    puma_output = output_dir / "monthly_baseline_puma.csv"
    nyc_output = output_dir / "monthly_baseline_nyc.csv"
    
    logger.info(f"Reading daily ridership data from {input_file.relative_to(base_dir)}")
    
    # Load special cases configuration
    special_cases = load_special_cases_config(base_dir, logger)
    
    # Read daily ridership data
    df = pd.read_csv(input_file)
    
    # Convert DATE to datetime
    df['DATE'] = pd.to_datetime(df['DATE'])
    
    # Collect all years needed (default + special cases) for the initial filter
    all_needed_years = set(baseline_years)
    for case_info in special_cases.values():
        all_needed_years.update(case_info['baseline_years'])
    df_all_baseline_years = df[df['YEAR'].isin(all_needed_years)].copy()
    
    years_str = ', '.join(map(str, sorted(all_needed_years)))
    logger.info(f"📅 Baseline years for regular stations: {baseline_years}")
    logger.info(f"Loaded data for years [{years_str}]: {len(df_all_baseline_years):,} records")
    
    # Initialize list to collect all monthly data
    all_monthly_data = []
    
    # Get list of special case complex IDs
    special_case_ids = list(special_cases.keys())
    
    # Process regular stations (those not in special cases)
    regular_stations_mask = ~df_all_baseline_years['Complex ID'].isin(special_case_ids)
    df_regular = df_all_baseline_years[regular_stations_mask & 
                                       df_all_baseline_years['YEAR'].isin(baseline_years)].copy()
    
    if len(df_regular) > 0:
        logger.info(f"Processing {df_regular['Complex ID'].nunique()} regular stations with {len(baseline_years)}-year baseline")
        
        # Calculate monthly totals for regular stations
        monthly_regular = df_regular.groupby(['Complex ID', 'MONTH']).agg({
            'ENTRIES': 'sum',
            'EXITS': 'sum'
        }).reset_index()
        
        # Calculate averages based on number of years
        monthly_regular['ENTRIES'] = monthly_regular['ENTRIES'] / len(baseline_years)
        monthly_regular['EXITS'] = monthly_regular['EXITS'] / len(baseline_years)
        
        all_monthly_data.append(monthly_regular)
    
    # Process special case stations
    for complex_id, case_info in special_cases.items():
        station_name = case_info['station_name']
        case_years = case_info['baseline_years']
        reason = case_info['reason']
        
        # Filter data for this station and its specific years
        df_special = df_all_baseline_years[
            (df_all_baseline_years['Complex ID'] == complex_id) & 
            (df_all_baseline_years['YEAR'].isin(case_years))
        ].copy()
        
        if len(df_special) > 0:
            years_str = ', '.join(map(str, case_years))
            logger.info(f"Processing special case: {station_name} ({complex_id}) - "
                       f"{len(df_special):,} records from years {years_str}")
            logger.info(f"  Reason: {reason}")
            
            # Calculate monthly totals
            monthly_special = df_special.groupby(['Complex ID', 'MONTH']).agg({
                'ENTRIES': 'sum',
                'EXITS': 'sum'
            }).reset_index()
            
            # Calculate averages based on number of years
            divisor = len(case_years)
            monthly_special['ENTRIES'] = monthly_special['ENTRIES'] / divisor
            monthly_special['EXITS'] = monthly_special['EXITS'] / divisor
            
            all_monthly_data.append(monthly_special)
        else:
            logger.warning(f"No data found for special case station {station_name} ({complex_id}) "
                          f"in years {case_years}")
    
    # Combine all monthly data
    if all_monthly_data:
        monthly_by_station = pd.concat(all_monthly_data, ignore_index=True)
    else:
        logger.error("No monthly data calculated!")
        raise ValueError("No monthly data was calculated")
    
    # Rename columns for output
    monthly_by_station.columns = ['complex_id', 'month', 'entries', 'exits']
    
    # Create complete station-month grid with 0 placeholders
    logger.info("Creating complete station-month grid for baseline...")
    station_complexes_file = base_dir / "references" / "stations" / "stations_complexes_official.csv"
    stations_df = pd.read_csv(station_complexes_file)
    
    # Exclude Staten Island Railway (SIR) stations
    stations_df = stations_df[stations_df['Daytime Routes'] != 'SIR']
    all_complex_ids = stations_df['Complex ID'].unique()
    
    # Create grid for all months (1-12) and all stations
    complete_grid = []
    for month in range(1, 13):
        for complex_id in all_complex_ids:
            complete_grid.append({
                'complex_id': complex_id,
                'month': month
            })
    
    complete_grid_df = pd.DataFrame(complete_grid)
    
    # Merge with actual data, filling missing values with 0
    monthly_by_station = complete_grid_df.merge(
        monthly_by_station,
        on=['complex_id', 'month'],
        how='left'
    )
    
    # Fill missing values with 0
    monthly_by_station['entries'] = monthly_by_station['entries'].fillna(0)
    monthly_by_station['exits'] = monthly_by_station['exits'].fillna(0)
    
    logger.info(f"Created baseline with {len(monthly_by_station):,} station-month combinations")
    
    # Save station baseline
    monthly_by_station.to_csv(station_output, index=False)
    logger.info(f"Saved station baseline to {station_output.relative_to(base_dir)}")
    
    # Load station to PUMA mapping
    logger.info(f"Reading station-PUMA mapping from {station_puma_file.relative_to(base_dir)}")
    station_puma = pd.read_csv(station_puma_file)
    
    # Merge with PUMA data
    df_with_puma = monthly_by_station.merge(
        station_puma, 
        left_on='complex_id', 
        right_on='Complex ID',
        how='left'
    )
    
    # Check for missing PUMA mappings
    missing_puma = df_with_puma[df_with_puma['PUMA'].isna()]['complex_id'].unique()
    if len(missing_puma) > 0:
        logger.warning(f"Found {len(missing_puma)} stations without PUMA mapping: {missing_puma[:5]}...")
    
    # Group by PUMA and month
    monthly_by_puma = df_with_puma.groupby(['PUMA', 'month']).agg({
        'entries': 'sum',
        'exits': 'sum'
    }).reset_index()
    
    # Rename columns for consistency
    monthly_by_puma.columns = ['puma', 'month', 'entries', 'exits']
    
    # Save PUMA baseline
    monthly_by_puma.to_csv(puma_output, index=False)
    logger.info(f"Saved PUMA baseline to {puma_output.relative_to(base_dir)}")
    
    # Calculate NYC-wide totals
    monthly_by_nyc = monthly_by_station.groupby('month').agg({
        'entries': 'sum',
        'exits': 'sum'
    }).reset_index()
    
    # Add NYC identifier
    monthly_by_nyc.insert(0, 'nyc', 'New York City')
    
    # Save NYC baseline
    monthly_by_nyc.to_csv(nyc_output, index=False)
    logger.info(f"Saved NYC baseline to {nyc_output.relative_to(base_dir)}")


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Calculate monthly baseline ridership from daily turnstile data."
    )
    parser.add_argument(
        "--years",
        type=int,
        nargs="+",
        default=DEFAULT_BASELINE_YEARS,
        help="Baseline years to average over (default: 2015 2016 2017 2018 2019).",
    )
    args = parser.parse_args()

    # Validate year range
    for y in args.years:
        if not 2014 <= y <= 2023:
            parser.error(f"Year {y} is outside the valid data range (2014–2023).")

    return args


def main():
    """Main execution function."""
    args = parse_args()

    # Find project root
    base_dir = find_project_root()
    
    # Set up logging
    logger = setup_logging(base_dir)
    
    logger.info("Starting baseline calculation")
    start_time = datetime.now()
    
    try:
        calculate_baselines(base_dir, logger, baseline_years=args.years)
        
        elapsed_time = datetime.now() - start_time
        logger.info(f"Baseline calculation completed in {elapsed_time.total_seconds():.2f} seconds")
        
    except Exception as e:
        logger.error(f"Error during baseline calculation: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    main()