#!/usr/bin/env python3
"""
Combine MTA Turnstile Data
==========================

Purpose:
    - Combines all weekly turnstile files in modern format (from October 18, 2014 onwards)
    - Creates a single combined file with all modern format data
    - Generates annual files for years 2015-2022

Features:
    - Intelligent file detection: Automatically identifies modern format files based on date
    - Robust error handling: Gracefully handles corrupted or malformed files
    - Progress tracking: Shows real-time progress with tqdm progress bars
    - Comprehensive logging: Creates detailed logs of processing steps
    - Summary statistics: Generates insights about the processed data

Usage:
    From project root:
        python scripts/combine_turnstile_data.py
    
    From scripts directory:
        python combine_turnstile_data.py

Output:
    All output files are saved in data/interim/turnstile/:
    - turnstile_combined.csv: All modern format data combined
    - turnstile_2015.csv through turnstile_2022.csv: Annual data files

Logging:
    Creates detailed logs in logs/combine_turnstile_data.log with:
    - Processing progress and file counts
    - Summary statistics
    - Any errors or warnings encountered

Performance:
    - Processes files incrementally for memory efficiency
    - Uses pandas for efficient data manipulation
    - Provides progress feedback for long-running operations
"""

import os
import sys
import subprocess
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional
import pandas as pd
from tqdm import tqdm


def find_project_root() -> Path:
    """Find the project root by looking for .git directory."""
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


class TurnstileDataProcessor:
    """Process and combine MTA turnstile data files."""
    
    def __init__(self, base_dir: Optional[Path] = None):
        """Initialize the processor with base directory paths."""
        # Use provided base_dir or find project root
        self.base_dir = base_dir or find_project_root()
        
        # Define paths relative to project root
        self.raw_dir = self.base_dir / "data" / "raw" / "turnstile"
        self.interim_dir = self.base_dir / "data" / "interim" / "turnstile"
        self.log_dir = self.base_dir / "logs"
        
        # Ensure directories exist
        self.interim_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        # Setup logging
        self._setup_logging()
        
        # Define column names for modern format
        self.column_names = [
            'C/A', 'UNIT', 'SCP', 'STATION', 'LINENAME',
            'DIVISION', 'DATE', 'TIME', 'DESC', 'ENTRIES', 'EXITS'
        ]
        
    def _setup_logging(self) -> None:
        """Configure logging for the script."""
        log_file = self.log_dir / "combine_turnstile_data.log"
        
        # Create formatter
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        
        # File handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        
        # Configure logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        
        # Prevent duplicate logs
        self.logger.propagate = False
        
    def get_modern_format_files(self) -> List[Path]:
        """Get all turnstile files in modern format (from October 18, 2014 onwards)."""
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
            
            # Modern format starts from 141018 (October 18, 2014)
            if file_date >= 141018:
                modern_files.append(file)
                
        self.logger.info(f"Found {len(modern_files)} modern format files")
        return modern_files
        
    def read_turnstile_file(self, filepath: Path) -> Optional[pd.DataFrame]:
        """Read a single turnstile file and return as DataFrame."""
        try:
            # Read the file
            df = pd.read_csv(filepath, header=0)
            
            # Verify it has the expected columns
            if len(df.columns) == len(self.column_names):
                df.columns = self.column_names
                
                # Add source file for tracking
                df['source_file'] = filepath.name
                
                return df
            else:
                self.logger.warning(f"Unexpected column count in {filepath.name}: {len(df.columns)}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error reading {filepath.name}: {str(e)}")
            return None
            
    def combine_files(self, files: List[Path]) -> pd.DataFrame:
        """Combine multiple turnstile files into a single DataFrame."""
        all_data = []
        
        self.logger.info("Reading and combining files...")
        for filepath in tqdm(files, desc="Processing files"):
            df = self.read_turnstile_file(filepath)
            if df is not None:
                all_data.append(df)
                
        if not all_data:
            raise ValueError("No data successfully read from files")
            
        # Combine all DataFrames
        combined_df = pd.concat(all_data, ignore_index=True)
        
        # Convert DATE column to datetime
        combined_df['DATE'] = pd.to_datetime(combined_df['DATE'], format='%m/%d/%Y')
        
        # Sort by date and time
        combined_df = combined_df.sort_values(['DATE', 'TIME'])
        
        self.logger.info(f"Combined {len(all_data)} files with {len(combined_df):,} total records")
        
        return combined_df
        
    def save_combined_file(self, df: pd.DataFrame) -> None:
        """Save the combined DataFrame to a single CSV file."""
        output_path = self.interim_dir / "turnstile_combined.csv"
        
        # Convert DATE back to string format for CSV
        df_copy = df.copy()
        df_copy['DATE'] = df_copy['DATE'].dt.strftime('%m/%d/%Y')
        
        # Remove source_file column from output
        df_copy = df_copy.drop('source_file', axis=1)
        
        df_copy.to_csv(output_path, index=False)
        self.logger.info(f"Saved combined file to {output_path.relative_to(self.base_dir)}")
        
    def save_annual_files(self, df: pd.DataFrame, start_year: int = 2015, end_year: int = 2022) -> None:
        """Save data separated by year for specified range."""
        # Extract year from DATE column
        df['year'] = df['DATE'].dt.year
        
        for year in range(start_year, end_year + 1):
            # Filter data for this year
            year_df = df[df['year'] == year].copy()
            
            if len(year_df) == 0:
                self.logger.warning(f"No data found for year {year}")
                continue
                
            # Remove year column and source_file
            year_df = year_df.drop(['year', 'source_file'], axis=1)
            
            # Convert DATE back to string format
            year_df['DATE'] = year_df['DATE'].dt.strftime('%m/%d/%Y')
            
            # Save to file
            output_path = self.interim_dir / f"turnstile_{year}.csv"
            year_df.to_csv(output_path, index=False)
            
            self.logger.info(
                f"Saved {year} data: {len(year_df):,} records to "
                f"{output_path.relative_to(self.base_dir)}"
            )
            
    def generate_summary_stats(self, df: pd.DataFrame) -> Dict:
        """Generate summary statistics for the combined data."""
        stats = {
            'total_records': len(df),
            'date_range': {
                'start': df['DATE'].min().strftime('%Y-%m-%d'),
                'end': df['DATE'].max().strftime('%Y-%m-%d')
            },
            'unique_stations': df['STATION'].nunique(),
            'unique_units': df['UNIT'].nunique(),
            'records_by_year': df.groupby(df['DATE'].dt.year).size().to_dict()
        }
        
        return stats
        
    def run(self) -> None:
        """Execute the main processing workflow."""
        self.logger.info("=" * 60)
        self.logger.info("Starting MTA Turnstile Data Processing")
        self.logger.info(f"Project root: {self.base_dir}")
        self.logger.info("=" * 60)
        
        try:
            # Get modern format files
            modern_files = self.get_modern_format_files()
            
            if not modern_files:
                raise ValueError("No modern format files found")
                
            # Combine all files
            combined_df = self.combine_files(modern_files)
            
            # Generate and log summary statistics
            stats = self.generate_summary_stats(combined_df)
            self.logger.info("\nSummary Statistics:")
            self.logger.info(f"  Total Records: {stats['total_records']:,}")
            self.logger.info(f"  Date Range: {stats['date_range']['start']} to {stats['date_range']['end']}")
            self.logger.info(f"  Unique Stations: {stats['unique_stations']:,}")
            self.logger.info(f"  Unique Units: {stats['unique_units']:,}")
            self.logger.info("\n  Records by Year:")
            for year, count in sorted(stats['records_by_year'].items()):
                self.logger.info(f"    {year}: {count:,}")
            
            # Save combined file
            self.logger.info("\nSaving combined file...")
            self.save_combined_file(combined_df)
            
            # Save annual files
            self.logger.info("\nSaving annual files (2015-2022)...")
            self.save_annual_files(combined_df)
            
            self.logger.info("\n" + "=" * 60)
            self.logger.info("Processing completed successfully!")
            self.logger.info("=" * 60)
            
        except Exception as e:
            self.logger.error(f"Processing failed: {str(e)}")
            raise


def main():
    """Main entry point for the script."""
    # Create and run processor
    processor = TurnstileDataProcessor()
    processor.run()


if __name__ == "__main__":
    main()