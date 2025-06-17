#!/usr/bin/env python3
"""
Enrich and Sort Final Data

This script enriches the final ridership analysis files by adding human-readable names
for PUMAs and subway station complexes, then sorts the data for consistent output.

Features:
- Adds puma_name to monthly_ridership_puma.csv from NYC PUMA crosswalk
- Adds station_name to monthly_ridership_station.csv from station complex reference
- Sorts all files by year and month for chronological ordering
- Additional sorting by puma/station_name for geographic consistency
- Creates backups of original files before modification
- Handles missing mappings gracefully with warnings
- Provides detailed logging of enrichment statistics

Usage:
    python scripts/enrich_final_data.py

Output:
    - Enriches and sorts results/final/monthly_ridership_puma.csv
    - Enriches and sorts results/final/monthly_ridership_station.csv
    - Sorts results/final/monthly_ridership_nyc.csv
    - Creates backups as .bak files before modification

Performance:
    - Processes files in-memory using pandas
    - Typically completes in under 1 second for standard datasets
"""

import pandas as pd
from pathlib import Path
import logging
from datetime import datetime
import shutil
from typing import Tuple, Dict


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
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"enrich_final_data_{timestamp}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info(f"Logging to: {log_file.relative_to(base_dir)}")
    
    return logger


def create_backup(file_path: Path, logger: logging.Logger) -> None:
    """Create a backup of the original file."""
    backup_path = file_path.with_suffix(file_path.suffix + '.bak')
    shutil.copy2(file_path, backup_path)
    logger.info(f"Created backup: {backup_path.name}")


def load_puma_crosswalk(base_dir: Path, logger: logging.Logger) -> pd.DataFrame:
    """Load PUMA crosswalk data."""
    puma_file = base_dir / "data" / "external" / "puma" / "nyc_puma_crosswalk_2020.csv"
    
    logger.info(f"Loading PUMA crosswalk from: {puma_file.relative_to(base_dir)}")
    df = pd.read_csv(puma_file)
    
    # Ensure puma_code is string for consistent joining
    df['puma_code'] = df['puma_code'].astype(str)
    
    logger.info(f"Loaded {len(df)} PUMA mappings")
    return df


def load_station_reference(base_dir: Path, logger: logging.Logger) -> pd.DataFrame:
    """Load station complex reference data."""
    station_file = base_dir / "references" / "stations" / "stations_complexes_official.csv"
    
    logger.info(f"Loading station reference from: {station_file.relative_to(base_dir)}")
    df = pd.read_csv(station_file)
    
    # Select only needed columns and rename for clarity
    df = df[['Complex ID', 'Stop Name']].copy()
    df.columns = ['complex_id', 'station_name']
    
    # Ensure complex_id is string for consistent joining
    df['complex_id'] = df['complex_id'].astype(str)
    
    # Remove duplicates if any (shouldn't be any for complex ID)
    df = df.drop_duplicates(subset=['complex_id'])
    
    logger.info(f"Loaded {len(df)} station mappings")
    return df


def enrich_puma_data(base_dir: Path, logger: logging.Logger) -> Tuple[int, int]:
    """Enrich PUMA ridership data with PUMA names."""
    puma_file = base_dir / "results" / "final" / "monthly_ridership_puma.csv"
    
    logger.info(f"\nEnriching PUMA data: {puma_file.relative_to(base_dir)}")
    
    # Create backup
    create_backup(puma_file, logger)
    
    # Load data
    df = pd.read_csv(puma_file)
    original_count = len(df)
    logger.info(f"Loaded {original_count:,} ridership records")
    
    # Convert puma to string for joining
    df['puma'] = df['puma'].astype(str)
    
    # Load crosswalk
    puma_crosswalk = load_puma_crosswalk(base_dir, logger)
    
    # Get unique PUMAs in ridership data
    unique_pumas = df['puma'].unique()
    logger.info(f"Found {len(unique_pumas)} unique PUMAs in ridership data")
    
    # Check if puma_name already exists
    if 'puma_name' in df.columns:
        logger.info("Column 'puma_name' already exists, updating values...")
        # Drop existing puma_name column to refresh it
        df = df.drop('puma_name', axis=1)
    
    # Merge with crosswalk
    df_enriched = df.merge(
        puma_crosswalk[['puma_code', 'puma_name']], 
        left_on='puma', 
        right_on='puma_code', 
        how='left'
    )
    
    # Drop the duplicate puma_code column
    df_enriched = df_enriched.drop('puma_code', axis=1)
    
    # Reorder columns to put puma_name after puma
    cols = list(df_enriched.columns)
    cols.remove('puma_name')
    puma_idx = cols.index('puma')
    cols.insert(puma_idx + 1, 'puma_name')
    df_enriched = df_enriched[cols]
    
    # Check for missing mappings
    missing_mask = df_enriched['puma_name'].isna()
    missing_count = missing_mask.sum()
    
    if missing_count > 0:
        missing_pumas = df_enriched[missing_mask]['puma'].unique()
        logger.warning(f"Found {missing_count} records with unmapped PUMAs: {sorted(missing_pumas)}")
    
    matched_count = original_count - missing_count
    logger.info(f"Successfully matched {matched_count:,} of {original_count:,} records ({matched_count/original_count*100:.1f}%)")
    
    # Sort by year, month, and puma
    logger.info("Sorting data by year, month, and puma...")
    df_enriched = df_enriched.sort_values(['year', 'month', 'puma'], ignore_index=True)
    
    # Save enriched and sorted data
    df_enriched.to_csv(puma_file, index=False)
    logger.info(f"Saved enriched and sorted PUMA data to: {puma_file.relative_to(base_dir)}")
    
    return matched_count, missing_count


def enrich_station_data(base_dir: Path, logger: logging.Logger) -> Tuple[int, int]:
    """Enrich station ridership data with station names."""
    station_file = base_dir / "results" / "final" / "monthly_ridership_station.csv"
    
    logger.info(f"\nEnriching station data: {station_file.relative_to(base_dir)}")
    
    # Create backup
    create_backup(station_file, logger)
    
    # Load data
    df = pd.read_csv(station_file)
    original_count = len(df)
    logger.info(f"Loaded {original_count:,} ridership records")
    
    # Convert complex_id to string for joining
    df['complex_id'] = df['complex_id'].astype(str)
    
    # Load station reference
    station_ref = load_station_reference(base_dir, logger)
    
    # Get unique complexes in ridership data
    unique_complexes = df['complex_id'].unique()
    logger.info(f"Found {len(unique_complexes)} unique station complexes in ridership data")
    
    # Check if station_name already exists
    if 'station_name' in df.columns:
        logger.info("Column 'station_name' already exists, updating values...")
        # Drop existing station_name column to refresh it
        df = df.drop('station_name', axis=1)
    
    # Merge with reference
    df_enriched = df.merge(
        station_ref, 
        on='complex_id', 
        how='left'
    )
    
    # Reorder columns to put station_name after complex_id
    cols = list(df_enriched.columns)
    cols.remove('station_name')
    complex_idx = cols.index('complex_id')
    cols.insert(complex_idx + 1, 'station_name')
    df_enriched = df_enriched[cols]
    
    # Check for missing mappings
    missing_mask = df_enriched['station_name'].isna()
    missing_count = missing_mask.sum()
    
    if missing_count > 0:
        missing_complexes = df_enriched[missing_mask]['complex_id'].unique()
        logger.warning(f"Found {missing_count} records with unmapped station complexes: {sorted(missing_complexes)}")
    
    matched_count = original_count - missing_count
    logger.info(f"Successfully matched {matched_count:,} of {original_count:,} records ({matched_count/original_count*100:.1f}%)")
    
    # Sort by year, month, and station_name
    logger.info("Sorting data by year, month, and station_name...")
    df_enriched = df_enriched.sort_values(['year', 'month', 'station_name'], ignore_index=True)
    
    # Save enriched and sorted data
    df_enriched.to_csv(station_file, index=False)
    logger.info(f"Saved enriched and sorted station data to: {station_file.relative_to(base_dir)}")
    
    return matched_count, missing_count


def sort_nyc_data(base_dir: Path, logger: logging.Logger) -> int:
    """Sort NYC-wide ridership data by year and month."""
    nyc_file = base_dir / "results" / "final" / "monthly_ridership_nyc.csv"
    
    logger.info(f"\nSorting NYC data: {nyc_file.relative_to(base_dir)}")
    
    # Create backup
    create_backup(nyc_file, logger)
    
    # Load data
    df = pd.read_csv(nyc_file)
    record_count = len(df)
    logger.info(f"Loaded {record_count:,} ridership records")
    
    # Sort by year and month
    logger.info("Sorting data by year and month...")
    df = df.sort_values(['year', 'month'], ignore_index=True)
    
    # Save sorted data
    df.to_csv(nyc_file, index=False)
    logger.info(f"Saved sorted NYC data to: {nyc_file.relative_to(base_dir)}")
    
    return record_count


def main():
    """Main execution function."""
    # Find project root and set up logging
    base_dir = find_project_root()
    logger = setup_logging(base_dir)
    
    logger.info("=" * 60)
    logger.info("Starting final data enrichment and sorting process")
    logger.info("=" * 60)
    
    try:
        # Enrich PUMA data
        puma_matched, puma_missing = enrich_puma_data(base_dir, logger)
        
        # Enrich station data
        station_matched, station_missing = enrich_station_data(base_dir, logger)
        
        # Sort NYC data (no enrichment needed)
        nyc_records = sort_nyc_data(base_dir, logger)
        
        # Summary statistics
        logger.info("\n" + "=" * 60)
        logger.info("Processing Summary:")
        logger.info("-" * 60)
        logger.info(f"PUMA data:    {puma_matched:,} matched, {puma_missing:,} missing")
        logger.info(f"Station data: {station_matched:,} matched, {station_missing:,} missing")
        logger.info(f"NYC data:     {nyc_records:,} records sorted")
        logger.info("=" * 60)
        
        logger.info("Final data enrichment and sorting completed successfully!")
        
    except Exception as e:
        logger.error(f"Error during processing: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    main()