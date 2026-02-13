#!/usr/bin/env python3
"""
Stage MTA Turnstile Data
========================

Purpose:
    A comprehensive pipeline for staging MTA turnstile data that:
    1. Combines all weekly turnstile files in modern format (from October 18, 2014 onwards)
    2. Filters and enriches the data with station complex information
        
    Filters Applied:
        - Excludes stations: "ORCHARD BEACH"
        - Includes only divisions: "BMT", "IND", "IRT" (subway systems only)
    
    3. Creates unique identifiers and applies data quality filters

Unique Identifiers Created:
    - turnstile_id: Combination of UNIT + C/A + SCP (identifies individual turnstiles)
    - station_id: Combination of STATION + LINENAME (identifies unique station-line combinations)

Data Processing:
    - Removes redundant columns: C/A, UNIT, SCP, DIVISION
    - Converts DATE column to datetime format for processing, then back to string for output
    - Sorts data by DATE and TIME for chronological ordering
    - Merges with station mapping data to add Complex ID information

Features:
    - 🔍 Intelligent file detection: Automatically identifies modern format files
    - 🛡️ Robust error handling: Gracefully handles corrupted or malformed files
    - 📊 Progress tracking: Shows real-time progress with detailed logging
    - 🎯 Smart filtering: Removes non-subway stations and divisions
    - 🔗 Data enrichment: Adds station complex mapping information
    - ⚡ Memory efficient: Processes files incrementally


Usage:
    From project root:
        python scripts/local/stage_turnstile_data.py
    
    From scripts/local directory:
        python stage_turnstile_data.py

Output:
    Final processed file saved in data/local/staging/turnstile/:
    - turnstile_combined.csv: Complete processed dataset ready for analysis

Performance:
    - Processes files incrementally for memory efficiency
    - Uses pandas for efficient data manipulation
    - Provides comprehensive progress feedback
"""

import os
import sys
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import pandas as pd
from tqdm import tqdm
import time


# Configuration constants
EXCLUDED_STATIONS = ["ORCHARD BEACH"]
ALLOWED_DIVISIONS = ["BMT", "IND", "IRT"]
MODERN_FORMAT_START_DATE = 141018  # October 18, 2014
BATCH_SIZE = 50  # Number of files to process in each batch


def find_project_root() -> Path:
    """
    Find the project root by looking for .git directory.
    
    Returns:
        Path: Project root directory
    """
    current = Path.cwd()
    
    # First check if we're already at the root (has .git)
    if (current / '.git').exists():
        return current
    
    # Otherwise, search up the directory tree
    for parent in current.parents:
        if (parent / '.git').exists():
            return parent
    
    # If no .git found, assume current directory is the root
    return current


class TurnstileDataPipeline:
    """
    Complete pipeline for MTA turnstile data processing.
    
    This class handles the entire workflow from raw file combination
    to final processed dataset with enrichment and filtering.
    """
    
    def __init__(self, base_dir: Optional[Path] = None):
        """
        Initialize the pipeline with base directory paths.
        
        Args:
            base_dir: Optional base directory path. If None, finds project root.
        """
        # Use provided base_dir or find project root
        self.base_dir = base_dir or find_project_root()
        
        # Define paths relative to project root
        self.raw_dir = self.base_dir / "data" / "local" / "raw" / "turnstile"
        self.staging_dir = self.base_dir / "data" / "local" / "staging" / "turnstile"
        self.references_dir = self.base_dir / "references" / "stations"
        
        # Ensure directories exist
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        
        # Setup logging
        self._setup_logging()
        
        # Define column names for modern format
        self.column_names = [
            'C/A', 'UNIT', 'SCP', 'STATION', 'LINENAME',
            'DIVISION', 'DATE', 'TIME', 'DESC', 'ENTRIES', 'EXITS'
        ]
        
    def _setup_logging(self) -> None:
        """Configure comprehensive logging for the pipeline."""
        # Create formatter with emojis for better readability
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        
        # Configure logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(console_handler)
        
        # Prevent duplicate logs
        self.logger.propagate = False
        
    def _get_modern_format_files(self) -> List[Path]:
        """
        Get all turnstile files in modern format (from October 18, 2014 onwards).
        
        Returns:
            List[Path]: List of file paths for modern format files
        """
        all_files = sorted(self.raw_dir.glob("turnstile_*.txt"))
        
        # Modern format starts with turnstile_141018.txt
        modern_files = []
        for file in all_files:
            filename = file.name
            # Extract date from filename (e.g., turnstile_141018.txt -> 141018)
            date_str = filename.replace("turnstile_", "").replace(".txt", "")
            
            # Skip if not numeric
            if not date_str.isdigit() or len(date_str) != 6:
                continue
                
            # Parse as YYMMDD
            file_date = int(date_str)
            
            # Modern format starts from specified date
            if file_date >= MODERN_FORMAT_START_DATE:
                modern_files.append(file)
                
        self.logger.info(f"🔍 Found {len(modern_files)} modern format files")
        return modern_files
        
    def _read_turnstile_file(self, filepath: Path) -> Optional[pd.DataFrame]:
        """
        Read a single turnstile file and return as DataFrame.
        
        Args:
            filepath: Path to the turnstile file
            
        Returns:
            Optional[pd.DataFrame]: DataFrame if successful, None if failed
        """
        try:
            # Read the file
            df = pd.read_csv(filepath, header=0)
            
            # Verify it has the expected columns
            if len(df.columns) == len(self.column_names):
                df.columns = self.column_names
                return df
            else:
                self.logger.warning(f"⚠️  Unexpected column count in {filepath.name}: {len(df.columns)}")
                return None
                
        except Exception as e:
            self.logger.error(f"❌ Error reading {filepath.name}: {str(e)}")
            return None
            
    def _combine_raw_files(self, files: List[Path]) -> pd.DataFrame:
        """
        Combine multiple turnstile files into a single DataFrame using batch processing.
        
        Args:
            files: List of file paths to combine
            
        Returns:
            pd.DataFrame: Combined dataset
        """
        self.logger.info("📊 Reading and combining raw files...")
        self.logger.info(f"   📦 Processing {len(files)} files in batches of {BATCH_SIZE}")
        
        # Create temporary directory for batch files
        temp_dir = self.staging_dir / "temp_batches"
        temp_dir.mkdir(exist_ok=True)
        
        batch_files = []
        total_records = 0
        successful_files = 0
        
        try:
            # Process files in batches
            num_batches = (len(files) + BATCH_SIZE - 1) // BATCH_SIZE
            start_time = time.time()
            
            for batch_idx in range(num_batches):
                batch_start_time = time.time()
                start_idx = batch_idx * BATCH_SIZE
                end_idx = min((batch_idx + 1) * BATCH_SIZE, len(files))
                batch_files_list = files[start_idx:end_idx]
                
                self.logger.info(f"\n   🔄 Processing batch {batch_idx + 1}/{num_batches} ({len(batch_files_list)} files)")
                
                batch_data = []
                
                # Process each file in the batch with progress bar
                for filepath in tqdm(batch_files_list, desc=f"Batch {batch_idx + 1}", leave=False):
                    df = self._read_turnstile_file(filepath)
                    if df is not None:
                        batch_data.append(df)
                        successful_files += 1
                
                if batch_data:
                    # Combine batch data
                    batch_df = pd.concat(batch_data, ignore_index=True)
                    batch_records = len(batch_df)
                    total_records += batch_records
                    
                    # Save batch to temporary file
                    batch_file = temp_dir / f"batch_{batch_idx:03d}.csv"
                    batch_df.to_csv(batch_file, index=False)
                    batch_files.append(batch_file)
                    
                    # Calculate timing
                    batch_time = time.time() - batch_start_time
                    elapsed_time = time.time() - start_time
                    avg_time_per_batch = elapsed_time / (batch_idx + 1)
                    remaining_batches = num_batches - batch_idx - 1
                    eta_seconds = remaining_batches * avg_time_per_batch
                    
                    self.logger.info(f"   ✅ Batch {batch_idx + 1} saved: {batch_records:,} records (took {batch_time:.1f}s)")
                    if remaining_batches > 0:
                        self.logger.info(f"   ⏱️  Estimated time remaining: {eta_seconds/60:.1f} minutes")
                    
                    # Free memory
                    del batch_data
                    del batch_df
                else:
                    self.logger.warning(f"   ⚠️  Batch {batch_idx + 1} had no valid data")
            
            if not batch_files:
                raise ValueError("No data successfully read from files")
            
            # Combine all batch files
            self.logger.info(f"\n   🔗 Combining {len(batch_files)} batch files...")
            
            combined_chunks = []
            for batch_file in tqdm(batch_files, desc="Combining batches"):
                chunk = pd.read_csv(batch_file)
                combined_chunks.append(chunk)
            
            combined_df = pd.concat(combined_chunks, ignore_index=True)
            
            # Convert DATE column to datetime
            self.logger.info("   📅 Converting date formats...")
            combined_df['DATE'] = pd.to_datetime(combined_df['DATE'], format='%m/%d/%Y')
            
            # Sort by date and time
            self.logger.info("   🔀 Sorting by date and time...")
            combined_df = combined_df.sort_values(['DATE', 'TIME'])
            
            self.logger.info(f"\n✅ Combined {successful_files} files with {len(combined_df):,} total records")
            
            return combined_df
            
        finally:
            # Clean up temporary files
            if temp_dir.exists():
                self.logger.info("   🧹 Cleaning up temporary files...")
                for batch_file in batch_files:
                    if batch_file.exists():
                        batch_file.unlink()
                temp_dir.rmdir()
        
    def _apply_filters(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply station and division filters to the dataset.
        
        Args:
            df: Input DataFrame
            
        Returns:
            pd.DataFrame: Filtered DataFrame
        """
        initial_count = len(df)
        
        # Remove excluded stations
        if EXCLUDED_STATIONS:
            self.logger.info(f"🚫 Removing stations: {', '.join(EXCLUDED_STATIONS)}")
            df = df[~df['STATION'].isin(EXCLUDED_STATIONS)]
        
        # Keep only allowed divisions (subway systems)
        self.logger.info(f"🚇 Keeping divisions: {', '.join(ALLOWED_DIVISIONS)}")
        df = df[df['DIVISION'].isin(ALLOWED_DIVISIONS)]
        
        filter_count = len(df)
        reduction = initial_count - filter_count
        self.logger.info(f"   📉 Filtered: {filter_count:,} records remaining ({reduction:,} removed)")
        
        return df
        
    def _create_unique_identifiers(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create unique identifiers for turnstiles and stations.
        
        Args:
            df: Input DataFrame
            
        Returns:
            pd.DataFrame: DataFrame with added identifier columns
        """
        self.logger.info("🔧 Creating unique identifiers...")
        
        # Create turnstile_id: Unit + C/A + SCP
        df['turnstile_id'] = df['UNIT'] + '_' + df['C/A'] + '_' + df['SCP']
        
        # Create station_id: STATION + LINENAME  
        df['station_id'] = df['STATION'] + '_' + df['LINENAME']
        
        self.logger.info(f"   🏷️  Created {df['turnstile_id'].nunique():,} unique turnstiles")
        self.logger.info(f"   🚉 Created {df['station_id'].nunique():,} unique station-line combinations")
        
        return df
        
    def _enrich_with_station_mapping(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Merge station mapping data to add Complex ID information.
        
        Args:
            df: Input DataFrame with station_id
            
        Returns:
            pd.DataFrame: DataFrame enriched with Complex ID information
        """
        self.logger.info("🗺️  Loading station mapping data...")
        
        # Load station mapping
        mapping_path = self.references_dir / "stations_turnstile_mapping.csv"
        
        if not mapping_path.exists():
            self.logger.warning(f"⚠️  Station mapping file not found: {mapping_path}")
            self.logger.info("   Continuing without Complex ID enrichment...")
            return df
            
        mapping_df = pd.read_csv(mapping_path)
        self.logger.info(f"   📋 Loaded {len(mapping_df):,} station mappings")
        
        # Merge on station_id to add Complex ID
        self.logger.info("🔗 Merging station complex information...")
        df_merged = df.merge(
            mapping_df[['station_id', 'Complex ID']], 
            on='station_id', 
            how='left'
        )
        
        # Check merge quality
        unmatched = df_merged['Complex ID'].isna().sum()
        total_records = len(df_merged)
        
        if unmatched > 0:
            self.logger.warning(f"   ⚠️  {unmatched:,} records without Complex ID mapping")
            self.logger.error(f"   ❌ MISSING COMPLEX IDs: {unmatched:,} out of {total_records:,} rows ({unmatched/total_records*100:.1f}%) are missing Complex ID")
        else:
            self.logger.info("   ✅ All records successfully mapped to complexes")
            self.logger.info(f"   ✅ COMPLETE MAPPING: All {total_records:,} rows have Complex ID assigned")
            
        return df_merged
        
    def _clean_final_dataset(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean the final dataset by removing redundant columns and formatting.
        
        Args:
            df: Input DataFrame
            
        Returns:
            pd.DataFrame: Cleaned DataFrame
        """
        self.logger.info("🧹 Cleaning final dataset...")
        
        # Convert DATE back to string format for CSV
        df = df.copy()
        df['DATE'] = df['DATE'].dt.strftime('%m/%d/%Y')
        
        # Remove only redundant raw columns (keep STATION and LINENAME)
        columns_to_remove = ['C/A', 'UNIT', 'SCP', 'DIVISION']
        existing_cols_to_remove = [col for col in columns_to_remove if col in df.columns]
        
        if existing_cols_to_remove:
            df = df.drop(columns=existing_cols_to_remove)
            self.logger.info(f"   🗑️  Removed columns: {', '.join(existing_cols_to_remove)}")
        
        return df
        
    def _save_final_dataset(self, df: pd.DataFrame) -> None:
        """
        Save the final processed dataset.
        
        Args:
            df: Final processed DataFrame
        """
        output_path = self.staging_dir / "turnstile_combined.csv"
        
        self.logger.info("💾 Saving final processed dataset...")
        df.to_csv(output_path, index=False)
        
        self.logger.info(f"   📁 Saved to: {output_path.relative_to(self.base_dir)}")
        self.logger.info(f"   📊 Final dataset: {len(df):,} records, {len(df.columns)} columns")
        
    def run(self) -> None:
        """Execute the complete data processing pipeline."""
        self.logger.info("=" * 80)
        self.logger.info("🚀 Starting MTA Turnstile Data Staging Pipeline")
        self.logger.info(f"📂 Project root: {self.base_dir}")
        self.logger.info("=" * 80)
        
        try:
            # Step 1: Get modern format files
            self.logger.info("\n📝 Step 1: Discovering modern format files...")
            modern_files = self._get_modern_format_files()
            
            if not modern_files:
                raise ValueError("No modern format files found")
                
            # Step 2: Combine raw files
            self.logger.info("\n🔄 Step 2: Combining raw turnstile files...")
            combined_df = self._combine_raw_files(modern_files)
            
            # Step 3: Apply filters
            self.logger.info("\n🎯 Step 3: Applying data filters...")
            filtered_df = self._apply_filters(combined_df)
            
            # Step 4: Create identifiers
            self.logger.info("\n🆔 Step 4: Creating unique identifiers...")
            identified_df = self._create_unique_identifiers(filtered_df)
            
            # Step 5: Enrich with station mapping
            self.logger.info("\n🔗 Step 5: Enriching with station complex data...")
            enriched_df = self._enrich_with_station_mapping(identified_df)
            
            # Step 6: Clean final dataset
            self.logger.info("\n🧹 Step 6: Cleaning final dataset...")
            final_df = self._clean_final_dataset(enriched_df)
            
            # Step 7: Save results
            self.logger.info("\n💾 Step 7: Saving final dataset...")
            self._save_final_dataset(final_df)
            
            # Success summary
            self.logger.info("\n" + "=" * 80)
            self.logger.info("🎉 Data staging completed successfully!")
            self.logger.info(f"✨ Processed {len(modern_files)} files into {len(final_df):,} final records")
            self.logger.info("=" * 80)
            
        except Exception as e:
            self.logger.error(f"💥 Pipeline failed: {str(e)}")
            raise


def main():
    """Main entry point for the turnstile data staging pipeline."""
    # Create and run the complete pipeline
    pipeline = TurnstileDataPipeline()
    pipeline.run()


if __name__ == "__main__":
    main() 