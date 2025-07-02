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

Features:
- Processes daily ridership data from 2015-2019
- Creates monthly averages for entries and exits
- Generates three separate baseline files
- Uses station-to-PUMA mapping for geographic aggregation

Usage:
    python scripts/calculate_baseline.py

Output:
    - results/baseline/monthly_baseline_station.csv
    - results/baseline/monthly_baseline_puma.csv
    - results/baseline/monthly_baseline_nyc.csv
"""

import pandas as pd
from pathlib import Path
import logging
from datetime import datetime

# Special case stations requiring modified baseline calculations
WTC_CORTLANDT_COMPLEX_ID = 328  # Closed until Sept 2018
SECOND_AVE_SUBWAY_COMPLEX_IDS = [475, 476, 477]  # Opened Jan 2017


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


def calculate_baselines(base_dir: Path, logger: logging.Logger):
    """Calculate monthly baselines from daily ridership data."""
    
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
    
    # Read daily ridership data
    df = pd.read_csv(input_file)
    
    # Convert DATE to datetime
    df['DATE'] = pd.to_datetime(df['DATE'])
    
    # Filter for years 2015-2019
    df_baseline = df[(df['YEAR'] >= 2015) & (df['YEAR'] <= 2019)].copy()
    
    logger.info(f"Filtered data to {len(df_baseline):,} records from 2015-2019")
    
    # Create list of all special case stations
    special_case_ids = [WTC_CORTLANDT_COMPLEX_ID] + SECOND_AVE_SUBWAY_COMPLEX_IDS
    
    # Process regular stations (excluding all special cases)
    df_regular = df_baseline[~df_baseline['Complex ID'].isin(special_case_ids)].copy()
    
    # Calculate monthly totals for regular stations
    monthly_regular = df_regular.groupby(['Complex ID', 'MONTH']).agg({
        'ENTRIES': 'sum',
        'EXITS': 'sum'
    }).reset_index()
    
    # Calculate averages for regular stations (5 years of data)
    monthly_regular['ENTRIES'] = monthly_regular['ENTRIES'] / 5
    monthly_regular['EXITS'] = monthly_regular['EXITS'] / 5
    
    # Process WTC-CORTLANDT separately (only 2019 data)
    df_wtc = df[(df['Complex ID'] == WTC_CORTLANDT_COMPLEX_ID) & (df['YEAR'] == 2019)].copy()
    
    if len(df_wtc) > 0:
        logger.info(f"Processing WTC-CORTLANDT special case: {len(df_wtc):,} records from 2019 only")
        
        # Calculate monthly totals for WTC-CORTLANDT
        monthly_wtc = df_wtc.groupby(['Complex ID', 'MONTH']).agg({
            'ENTRIES': 'sum',
            'EXITS': 'sum'
        }).reset_index()
        
        # No division needed - using only 1 year of data
        logger.info("WTC-CORTLANDT baseline using 2019 data only (station was closed until Sept 2018)")
        
        # Combine regular stations with WTC-CORTLANDT
        monthly_by_station = pd.concat([monthly_regular, monthly_wtc], ignore_index=True)
    else:
        logger.warning("No WTC-CORTLANDT data found for 2019")
        monthly_by_station = monthly_regular
    
    # Process Second Avenue Subway stations (2018-2019 data only)
    df_sas = df[(df['Complex ID'].isin(SECOND_AVE_SUBWAY_COMPLEX_IDS)) & 
                (df['YEAR'].isin([2018, 2019]))].copy()
    
    if len(df_sas) > 0:
        logger.info(f"Processing Second Avenue Subway special case: {len(df_sas):,} records from 2018-2019")
        
        # Get station names for logging
        sas_names = df_sas.groupby('Complex ID')['PRIMARY_STATION_NAME'].first().to_dict()
        station_list = ', '.join([f"{name} ({id})" for id, name in sas_names.items()])
        logger.info(f"Second Avenue Subway stations: {station_list}")
        
        # Calculate monthly totals for Second Avenue Subway stations
        monthly_sas = df_sas.groupby(['Complex ID', 'MONTH']).agg({
            'ENTRIES': 'sum',
            'EXITS': 'sum'
        }).reset_index()
        
        # Calculate averages using 2 years of data
        monthly_sas['ENTRIES'] = monthly_sas['ENTRIES'] / 2
        monthly_sas['EXITS'] = monthly_sas['EXITS'] / 2
        
        logger.info("Second Avenue Subway baseline using 2018-2019 data only (stations opened Jan 2017)")
        
        # Combine with existing data
        monthly_by_station = pd.concat([monthly_by_station, monthly_sas], ignore_index=True)
    else:
        logger.warning("No Second Avenue Subway data found for 2018-2019")
    
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


def main():
    """Main execution function."""
    # Find project root
    base_dir = find_project_root()
    
    # Set up logging
    logger = setup_logging(base_dir)
    
    logger.info("Starting baseline calculation")
    start_time = datetime.now()
    
    try:
        calculate_baselines(base_dir, logger)
        
        elapsed_time = datetime.now() - start_time
        logger.info(f"Baseline calculation completed in {elapsed_time.total_seconds():.2f} seconds")
        
    except Exception as e:
        logger.error(f"Error during baseline calculation: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    main()