#!/usr/bin/env python3
"""
Calculate Monthly Ridership Metrics from Processed Daily Ridership Data

This script processes daily ridership data to create monthly aggregations with OMNY adoption metrics.
It generates three levels of geographic aggregation:
1. By station (using complex ID)
2. By PUMA (Public Use Microdata Area)
3. By NYC (citywide aggregation)

Key Metrics:
- Total monthly ridership (all payment methods)
- OMNY ridership percentage
- Breakdown by payment method

Features:
- Processes all available ridership data files
- Filters out incomplete months (e.g., partial data for current month)
- Calculates monthly totals and OMNY adoption rates
- Generates three separate output files
- Uses station-to-PUMA mapping for geographic aggregation
- Comprehensive logging and error handling

Usage:
    python scripts/local/calculate_ridership.py

Output:
    - data/local/ridership/monthly_ridership_station.csv
    - data/local/ridership/monthly_ridership_puma.csv
    - data/local/ridership/monthly_ridership_nyc.csv
"""

import pandas as pd
from pathlib import Path
import logging
import sys
from datetime import datetime
from typing import List
import calendar

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.utils.runtime import find_project_root, setup_script_logging

DAY_GROUP_ORDER = ['total', 'weekday', 'weekend']


def load_all_ridership_data(data_dir: Path, logger: logging.Logger) -> pd.DataFrame:
    """Load and concatenate all processed ridership data files.
    
    Args:
        data_dir: Directory containing processed ridership CSV files
        logger: Logger instance
        
    Returns:
        Concatenated DataFrame with all ridership data
    """
    ridership_files = list(data_dir.glob("processed_*.csv"))
    
    if not ridership_files:
        raise FileNotFoundError(f"No processed ridership files found in {data_dir}")
    
    logger.info(f"Found {len(ridership_files)} ridership files to process")
    
    dfs: List[pd.DataFrame] = []
    
    for file_path in sorted(ridership_files):
        logger.info(f"Loading {file_path.name}")
        df = pd.read_csv(file_path, parse_dates=['date'])
        dfs.append(df)
        logger.info(f"  - Loaded {len(df):,} records")
    
    # Concatenate all dataframes
    combined_df = pd.concat(dfs, ignore_index=True)
    
    #! temp fix for data problem: the data include some entries where station_complex_id = 502 and they are incorrectly remained in the data.
    combined_df = combined_df[combined_df['station_complex_id'] != 502]
    
    logger.info(f"Total records loaded: {len(combined_df):,}")
    
    return combined_df


def filter_incomplete_months(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """Filter out months that don't have complete data.
    
    Args:
        df: DataFrame with daily ridership data
        logger: Logger instance
        
    Returns:
        DataFrame with only complete months
    """
    # Extract year and month for analysis
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month
    
    # Count unique days per month in the data
    days_per_month = df.groupby(['year', 'month'])['date'].nunique().reset_index()
    days_per_month.columns = ['year', 'month', 'days_in_data']
    
    # Calculate expected days per month
    days_per_month['expected_days'] = days_per_month.apply(
        lambda row: calendar.monthrange(row['year'], row['month'])[1], axis=1
    )
    
    # Identify incomplete months
    incomplete_months = days_per_month[
        days_per_month['days_in_data'] < days_per_month['expected_days']
    ]
    
    if len(incomplete_months) > 0:
        logger.warning("Found incomplete months that will be filtered out:")
        for _, row in incomplete_months.iterrows():
            logger.warning(
                f"  - {calendar.month_name[row['month']]} {row['year']}: "
                f"{row['days_in_data']} days of {row['expected_days']} expected"
            )
    
    # Create a set of complete months for filtering
    complete_months = days_per_month[
        days_per_month['days_in_data'] == days_per_month['expected_days']
    ][['year', 'month']]
    
    # Filter the original dataframe
    initial_count = len(df)
    df_filtered = df.merge(complete_months, on=['year', 'month'], how='inner')
    filtered_count = initial_count - len(df_filtered)
    
    if filtered_count > 0:
        logger.info(f"Filtered out {filtered_count:,} records from incomplete months")
        logger.info(f"Remaining records: {len(df_filtered):,}")
    else:
        logger.info("No incomplete months found - all data retained")
    
    return df_filtered


def calculate_monthly_metrics(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """Calculate monthly ridership metrics by station.
    
    Args:
        df: DataFrame with daily ridership data
        logger: Logger instance
        
    Returns:
        DataFrame with monthly metrics by station
    """
    # Note: year and month columns are already added by filter_incomplete_months
    # Derive weekday/weekend buckets, then add a derived total bucket.
    by_day_group = df.copy()
    by_day_group['day_group'] = by_day_group['date'].dt.dayofweek.map(
        lambda day_num: 'weekday' if day_num < 5 else 'weekend'
    )
    by_total = by_day_group.copy()
    by_total['day_group'] = 'total'
    expanded_df = pd.concat([by_day_group, by_total], ignore_index=True)

    # Group by station, year, month, day bucket, and payment method
    monthly_by_method = expanded_df.groupby(
        ['station_complex_id', 'year', 'month', 'day_group', 'payment_method']
    )['ridership'].sum().reset_index()
    
    # Pivot to get payment methods as columns
    monthly_pivot = monthly_by_method.pivot_table(
        index=['station_complex_id', 'year', 'month', 'day_group'],
        columns='payment_method',
        values='ridership',
        fill_value=0
    ).reset_index()
    
    # Calculate total ridership
    payment_columns = [col for col in monthly_pivot.columns 
                      if col not in ['station_complex_id', 'year', 'month', 'day_group']]
    monthly_pivot['ridership'] = monthly_pivot[payment_columns].sum(axis=1)
    
    # Calculate OMNY percentage
    if 'OMNY' in monthly_pivot.columns:
        monthly_pivot['omny_pct'] = (
            monthly_pivot['OMNY'] / monthly_pivot['ridership'] * 100
        ).round(2)
        monthly_pivot.loc[monthly_pivot['ridership'] == 0, 'omny_pct'] = 0.0
    else:
        monthly_pivot['omny_pct'] = 0.0
    
    # Add period column in YYYY-MM-DD format
    monthly_pivot['period'] = pd.to_datetime(
        monthly_pivot[['year', 'month']].assign(day=1)
    ).dt.strftime('%Y-%m-%d')

    # Keep the output ordering stable and explicit.
    monthly_pivot['day_group'] = pd.Categorical(
        monthly_pivot['day_group'],
        categories=DAY_GROUP_ORDER,
        ordered=True
    )
    monthly_pivot = monthly_pivot.sort_values(
        ['station_complex_id', 'year', 'month', 'day_group']
    ).reset_index(drop=True)
    monthly_pivot['day_group'] = monthly_pivot['day_group'].astype(str)
    
    logger.info(f"Calculated metrics for {len(monthly_pivot):,} station-month-day_group combinations")
    
    return monthly_pivot


def create_complete_station_month_grid(
    monthly_data: pd.DataFrame, 
    station_complexes_file: Path,
    logger: logging.Logger
) -> pd.DataFrame:
    """Create a complete grid of all stations for all months with 0 placeholders.
    
    Args:
        monthly_data: DataFrame with actual monthly ridership data
        station_complexes_file: Path to the station complexes CSV file
        logger: Logger instance
        
    Returns:
        DataFrame with complete station-month grid, filling missing data with zeros
    """
    # Load complete list of station complexes
    stations_df = pd.read_csv(station_complexes_file)
    
    # Exclude Staten Island Railway (SIR) stations
    stations_df = stations_df[stations_df['Daytime Routes'] != 'SIR']
    all_complex_ids = stations_df['Complex ID'].unique()
    logger.info(f"Found {len(all_complex_ids)} unique station complexes (excluding SIR)")
    
    # Get unique year-month combinations from the data
    unique_periods = monthly_data[['year', 'month', 'period']].drop_duplicates()
    logger.info(f"Found {len(unique_periods)} unique year-month periods")
    
    # Create complete grid of all stations x all periods x all day buckets
    complete_grid = pd.DataFrame()
    for _, period in unique_periods.iterrows():
        for day_group in DAY_GROUP_ORDER:
            period_df = pd.DataFrame({
                'station_complex_id': all_complex_ids,
                'year': period['year'],
                'month': period['month'],
                'period': period['period'],
                'day_group': day_group,
            })
            complete_grid = pd.concat([complete_grid, period_df], ignore_index=True)
    
    logger.info(f"Created complete grid with {len(complete_grid):,} station-month-day_group combinations")
    
    # Merge with actual data
    # First, prepare the columns to merge
    merge_cols = ['station_complex_id', 'year', 'month', 'period', 'day_group']
    data_cols = [col for col in monthly_data.columns if col not in merge_cols]
    
    # Perform left join to keep all station-month combinations
    final_data = complete_grid.merge(
        monthly_data,
        on=merge_cols,
        how='left'
    )
    
    # Fill missing values with zeros for numeric columns
    numeric_cols = ['ridership', 'omny_pct'] + [col for col in data_cols if col.isupper()]
    for col in numeric_cols:
        if col in final_data.columns:
            final_data[col] = final_data[col].fillna(0)
    
    # Ensure ridership and omny_pct columns exist
    if 'ridership' not in final_data.columns:
        final_data['ridership'] = 0
    if 'omny_pct' not in final_data.columns:
        final_data['omny_pct'] = 0.0
    
    # Log statistics
    missing_count = (final_data['ridership'] == 0).sum()
    logger.info(f"Filled {missing_count:,} station-month-day_group combinations with zero ridership")
    
    return final_data


def aggregate_by_puma(
    station_df: pd.DataFrame, 
    station_puma_mapping: pd.DataFrame,
    logger: logging.Logger
) -> pd.DataFrame:
    """Aggregate station-level data to PUMA level.
    
    Args:
        station_df: DataFrame with station-level monthly metrics
        station_puma_mapping: DataFrame mapping stations to PUMAs
        logger: Logger instance
        
    Returns:
        DataFrame with PUMA-level monthly metrics
    """
    # Merge with PUMA mapping
    df_with_puma = station_df.merge(
        station_puma_mapping,
        left_on='station_complex_id',
        right_on='Complex ID',
        how='left'
    )
    
    # Check for missing PUMA mappings
    missing_puma = df_with_puma[df_with_puma['PUMA'].isna()]['station_complex_id'].unique()
    if len(missing_puma) > 0:
        logger.warning(f"Found {len(missing_puma)} stations without PUMA mapping")
        logger.debug(f"Missing stations: {missing_puma[:10]}...")
    
    # Filter out stations without PUMA mapping
    df_with_puma = df_with_puma[df_with_puma['PUMA'].notna()]
    
    # Get payment method columns
    payment_columns = [col for col in df_with_puma.columns 
                      if col.isupper() and col not in ['PUMA']]
    
    # Group by PUMA and time
    puma_grouped = df_with_puma.groupby(['PUMA', 'year', 'month', 'period', 'day_group']).agg({
        **{col: 'sum' for col in payment_columns},
        'ridership': 'sum'
    }).reset_index()
    
    # Recalculate OMNY percentage at PUMA level
    if 'OMNY' in puma_grouped.columns:
        puma_grouped['omny_pct'] = (
            puma_grouped['OMNY'] / puma_grouped['ridership'] * 100
        ).round(2)
    else:
        puma_grouped['omny_pct'] = 0.0
    
    logger.info(f"Aggregated to {len(puma_grouped):,} PUMA-month-day_group combinations")
    
    return puma_grouped


def aggregate_to_nyc(station_df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """Aggregate station-level data to NYC level.
    
    Args:
        station_df: DataFrame with station-level monthly metrics
        logger: Logger instance
        
    Returns:
        DataFrame with NYC-level monthly metrics
    """
    # Get payment method columns
    payment_columns = [col for col in station_df.columns 
                      if col.isupper() and col not in ['PUMA']]
    
    # Group by time periods only
    nyc_grouped = station_df.groupby(['year', 'month', 'period', 'day_group']).agg({
        **{col: 'sum' for col in payment_columns},
        'ridership': 'sum'
    }).reset_index()
    
    # Recalculate OMNY percentage at NYC level
    if 'OMNY' in nyc_grouped.columns:
        nyc_grouped['omny_pct'] = (
            nyc_grouped['OMNY'] / nyc_grouped['ridership'] * 100
        ).round(2)
    else:
        nyc_grouped['omny_pct'] = 0.0
    
    # Add NYC identifier
    nyc_grouped.insert(0, 'geography', 'New York City')
    
    logger.info(f"Aggregated to {len(nyc_grouped):,} NYC-month-day_group combinations")
    
    return nyc_grouped


def format_output_columns(df: pd.DataFrame, level: str) -> pd.DataFrame:
    """Format and select appropriate columns for output.
    
    Args:
        df: DataFrame to format
        level: Geographic level ('station', 'puma', or 'nyc')
        
    Returns:
        Formatted DataFrame ready for output
    """
    # Define column order based on level
    base_columns = ['year', 'month', 'period', 'day_group', 'ridership', 'omny_pct']
    
    if level == 'station':
        # Rename station_complex_id to complex_id for consistency
        df = df.rename(columns={'station_complex_id': 'complex_id'})
        id_columns = ['complex_id']
    elif level == 'puma':
        # Rename PUMA to puma for consistency
        df = df.rename(columns={'PUMA': 'puma'})
        id_columns = ['puma']
    else:  # nyc
        # NYC file doesn't need geography column since there's only one geography
        id_columns = []
    
    # Combine columns in desired order (no payment method columns)
    column_order = id_columns + base_columns
    
    # Select and reorder columns
    return df[column_order].copy()


def save_results(
    station_df: pd.DataFrame,
    puma_df: pd.DataFrame,
    nyc_df: pd.DataFrame,
    output_dir: Path,
    logger: logging.Logger
) -> None:
    """Save all result DataFrames to CSV files.
    
    Args:
        station_df: Station-level monthly metrics
        puma_df: PUMA-level monthly metrics
        nyc_df: NYC-level monthly metrics
        output_dir: Directory to save output files
        logger: Logger instance
    """
    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Format and save each file
    outputs = [
        ('station', station_df, 'monthly_ridership_station.csv'),
        ('puma', puma_df, 'monthly_ridership_puma.csv'),
        ('nyc', nyc_df, 'monthly_ridership_nyc.csv')
    ]
    
    for level, df, filename in outputs:
        formatted_df = format_output_columns(df, level)

        if 'day_group' in formatted_df.columns:
            formatted_df['day_group'] = pd.Categorical(
                formatted_df['day_group'],
                categories=DAY_GROUP_ORDER,
                ordered=True
            )
            if level == 'station':
                sort_cols = ['year', 'month', 'complex_id', 'day_group']
            elif level == 'puma':
                sort_cols = ['year', 'month', 'puma', 'day_group']
            else:
                sort_cols = ['year', 'month', 'day_group']
            formatted_df = formatted_df.sort_values(sort_cols, ignore_index=True)
            formatted_df['day_group'] = formatted_df['day_group'].astype(str)

        output_path = output_dir / filename
        formatted_df.to_csv(output_path, index=False)
        logger.info(f"Saved {level} results to {output_path.relative_to(output_dir.parent.parent)}")
        logger.info(f"  - Shape: {formatted_df.shape}")


def main():
    """Main execution function."""
    # Find project root
    base_dir = find_project_root()
    
    # Set up logging
    logger, _ = setup_script_logging(
        base_dir=base_dir,
        logger_name=__name__,
        log_filename="calculate_ridership.log",
    )
    
    logger.info("="*60)
    logger.info("Starting monthly ridership calculation")
    logger.info("="*60)
    start_time = datetime.now()
    
    try:
        # Define paths
        ridership_dir = base_dir / "data" / "local" / "processed" / "ridership"
        station_puma_file = base_dir / "references" / "stations" / "station_to_puma.csv"
        output_dir = base_dir / "data" / "local" / "ridership"
        
        # Load all ridership data
        logger.info("\n📊 Loading ridership data...")
        ridership_df = load_all_ridership_data(ridership_dir, logger)
        
        # Filter out incomplete months
        logger.info("\n🔍 Filtering incomplete months...")
        ridership_df = filter_incomplete_months(ridership_df, logger)
        
        # Calculate monthly metrics by station
        logger.info("\n📈 Calculating monthly metrics...")
        monthly_by_station = calculate_monthly_metrics(ridership_df, logger)
        
        # Create complete station-month grid with 0 placeholders
        logger.info("\n📊 Creating complete station-month grid...")
        station_complexes_file = base_dir / "references" / "stations" / "stations_complexes_official.csv"
        monthly_by_station = create_complete_station_month_grid(
            monthly_by_station, station_complexes_file, logger
        )
        
        # Load station to PUMA mapping
        logger.info("\n🗺️  Loading geographic mappings...")
        station_puma = pd.read_csv(station_puma_file)
        logger.info(f"Loaded {len(station_puma):,} station-PUMA mappings")
        
        # Aggregate to PUMA level
        logger.info("\n🏘️  Aggregating to PUMA level...")
        monthly_by_puma = aggregate_by_puma(monthly_by_station, station_puma, logger)
        
        # Aggregate to NYC level
        logger.info("\n🏙️  Aggregating to NYC level...")
        monthly_by_nyc = aggregate_to_nyc(monthly_by_station, logger)
        
        # Save all results
        logger.info("\n💾 Saving results...")
        save_results(
            monthly_by_station,
            monthly_by_puma,
            monthly_by_nyc,
            output_dir,
            logger
        )
        
        # Summary statistics
        elapsed_time = datetime.now() - start_time
        logger.info("\n" + "="*60)
        logger.info("✅ Processing completed successfully!")
        logger.info(f"⏱️  Total time: {elapsed_time.total_seconds():.2f} seconds")
        logger.info("="*60)
        
    except Exception as e:
        logger.error(f"❌ Error during processing: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
