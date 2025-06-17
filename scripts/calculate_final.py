#!/usr/bin/env python3
"""
Calculate Final Ridership Metrics with Baseline Comparisons

This script merges monthly ridership data with baseline data to create final analysis files.
It adds baseline ridership and comparison metrics to show how current ridership compares
to the 2015-2019 baseline period.

Features:
- Reads ridership data from results/ridership/
- Reads baseline data from results/baseline/
- Merges data by geographic level and month
- Calculates baseline comparison (ridership / baseline_ridership)
- Outputs enhanced files to results/final/

Output columns added:
- baseline_ridership: Average monthly ridership from 2015-2019
- baseline_comparison: Percentage comparison (ridership / baseline_ridership)

Usage:
    python scripts/calculate_final.py

Output:
    - results/final/monthly_ridership_station.csv
    - results/final/monthly_ridership_puma.csv
    - results/final/monthly_ridership_nyc.csv
"""

import pandas as pd
from pathlib import Path
import logging
from datetime import datetime
from typing import Tuple


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
    
    log_file = log_dir / "calculate_final.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    
    return logging.getLogger(__name__)


def load_data_pair(
    ridership_path: Path,
    baseline_path: Path,
    level: str,
    logger: logging.Logger
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Load ridership and baseline data for a given geographic level.
    
    Args:
        ridership_path: Path to ridership CSV file
        baseline_path: Path to baseline CSV file
        level: Geographic level ('station', 'puma', or 'nyc')
        logger: Logger instance
        
    Returns:
        Tuple of (ridership_df, baseline_df)
    """
    logger.info(f"Loading {level} data...")
    
    # Load ridership data
    ridership_df = pd.read_csv(ridership_path)
    logger.info(f"  - Loaded {len(ridership_df):,} ridership records")
    
    # Load baseline data
    baseline_df = pd.read_csv(baseline_path)
    logger.info(f"  - Loaded {len(baseline_df):,} baseline records")
    
    return ridership_df, baseline_df


def merge_with_baseline(
    ridership_df: pd.DataFrame,
    baseline_df: pd.DataFrame,
    level: str,
    logger: logging.Logger
) -> pd.DataFrame:
    """Merge ridership data with baseline data and calculate comparison metrics.
    
    Args:
        ridership_df: DataFrame with current ridership data
        baseline_df: DataFrame with baseline ridership data
        level: Geographic level ('station', 'puma', or 'nyc')
        logger: Logger instance
        
    Returns:
        Merged DataFrame with baseline comparison columns
    """
    # Define merge keys based on level
    if level == 'station':
        geo_col = 'complex_id'
    elif level == 'puma':
        geo_col = 'puma'
    else:  # nyc
        geo_col = None
    
    # Prepare baseline data - use entries as baseline_ridership
    baseline_df = baseline_df.rename(columns={
        'entries': 'baseline_ridership'
    })
    
    # Select only needed columns from baseline
    if geo_col:
        baseline_cols = [geo_col, 'month', 'baseline_ridership']
    else:
        baseline_cols = ['month', 'baseline_ridership']
    
    baseline_df = baseline_df[baseline_cols]
    
    # Merge ridership with baseline
    if geo_col:
        merge_keys = [geo_col, 'month']
    else:
        merge_keys = ['month']
    
    merged_df = ridership_df.merge(
        baseline_df,
        on=merge_keys,
        how='left'
    )
    
    # Calculate baseline comparison as percentage (ridership / baseline_ridership)
    merged_df['baseline_comparison'] = (
        merged_df['ridership'] / merged_df['baseline_ridership']
    ).round(4)
    
    # Handle cases where baseline is missing (new stations)
    missing_baseline = merged_df['baseline_ridership'].isna().sum()
    if missing_baseline > 0:
        logger.warning(f"  - {missing_baseline} records without baseline data")
        # Fill NaN comparisons with None (will appear as empty in CSV)
        merged_df.loc[merged_df['baseline_ridership'].isna(), 'baseline_comparison'] = None
    
    # Log summary statistics
    avg_comparison = merged_df['baseline_comparison'].mean()
    if pd.notna(avg_comparison):
        logger.info(f"  - Average baseline comparison: {avg_comparison:.3f}")
    
    return merged_df


def process_geographic_level(
    level: str,
    base_dir: Path,
    logger: logging.Logger
) -> pd.DataFrame:
    """Process data for a specific geographic level.
    
    Args:
        level: Geographic level ('station', 'puma', or 'nyc')
        base_dir: Project root directory
        logger: Logger instance
        
    Returns:
        Processed DataFrame with baseline comparisons
    """
    # Define file paths
    ridership_file = f"monthly_ridership_{level}.csv"
    baseline_file = f"monthly_baseline_{level}.csv"
    
    ridership_path = base_dir / "results" / "ridership" / ridership_file
    baseline_path = base_dir / "results" / "baseline" / baseline_file
    
    # Load data
    ridership_df, baseline_df = load_data_pair(
        ridership_path, baseline_path, level, logger
    )
    
    # Merge and calculate comparisons
    final_df = merge_with_baseline(
        ridership_df, baseline_df, level, logger
    )
    
    return final_df


def format_final_output(df: pd.DataFrame, level: str) -> pd.DataFrame:
    """Format the final output with appropriate column order.
    
    Args:
        df: DataFrame to format
        level: Geographic level ('station', 'puma', or 'nyc')
        
    Returns:
        Formatted DataFrame
    """
    # Define column order
    if level == 'station':
        id_cols = ['complex_id']
    elif level == 'puma':
        id_cols = ['puma']
    else:  # nyc
        id_cols = []
    
    # Common columns in order
    time_cols = ['year', 'month', 'period']
    metric_cols = ['ridership', 'baseline_ridership', 'baseline_comparison', 'omny_pct']
    
    # Combine all columns in desired order
    column_order = id_cols + time_cols + metric_cols
    
    # Ensure all columns exist
    available_cols = [col for col in column_order if col in df.columns]
    
    return df[available_cols]


def save_final_results(
    station_df: pd.DataFrame,
    puma_df: pd.DataFrame,
    nyc_df: pd.DataFrame,
    output_dir: Path,
    logger: logging.Logger
) -> None:
    """Save all final result DataFrames to CSV files.
    
    Args:
        station_df: Station-level final metrics
        puma_df: PUMA-level final metrics
        nyc_df: NYC-level final metrics
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
        formatted_df = format_final_output(df, level)
        output_path = output_dir / filename
        formatted_df.to_csv(output_path, index=False)
        logger.info(f"Saved {level} results to {output_path.relative_to(output_dir.parent.parent)}")
        logger.info(f"  - Shape: {formatted_df.shape}")
        
        # Log sample of baseline comparisons
        if 'baseline_comparison' in formatted_df.columns:
            sample_comparisons = formatted_df['baseline_comparison'].describe()
            logger.info(f"  - Baseline comparison stats:")
            logger.info(f"    Mean: {sample_comparisons['mean']:.3f}")
            logger.info(f"    Min:  {sample_comparisons['min']:.3f}")
            logger.info(f"    Max:  {sample_comparisons['max']:.3f}")


def main():
    """Main execution function."""
    # Find project root
    base_dir = find_project_root()
    
    # Set up logging
    logger = setup_logging(base_dir)
    
    logger.info("="*60)
    logger.info("Starting final ridership calculation with baseline comparisons")
    logger.info("="*60)
    start_time = datetime.now()
    
    try:
        # Define output directory
        output_dir = base_dir / "results" / "final"
        
        # Process each geographic level
        logger.info("\n=� Processing station-level data...")
        station_df = process_geographic_level('station', base_dir, logger)
        
        logger.info("\n<�  Processing PUMA-level data...")
        puma_df = process_geographic_level('puma', base_dir, logger)
        
        logger.info("\n<�  Processing NYC-level data...")
        nyc_df = process_geographic_level('nyc', base_dir, logger)
        
        # Save all results
        logger.info("\n=� Saving final results...")
        save_final_results(
            station_df,
            puma_df,
            nyc_df,
            output_dir,
            logger
        )
        
        # Summary
        elapsed_time = datetime.now() - start_time
        logger.info("\n" + "="*60)
        logger.info(" Final processing completed successfully!")
        logger.info(f"�  Total time: {elapsed_time.total_seconds():.2f} seconds")
        logger.info("="*60)
        
    except Exception as e:
        logger.error(f"L Error during processing: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    main()