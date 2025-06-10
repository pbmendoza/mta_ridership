#!/usr/bin/env python3
"""
Extract Unique Station Data from MTA Turnstile Records
======================================================

Purpose:
    Extracts unique combinations of STATION, LINENAME, and DIVISION from the combined
    turnstile data and creates a reference file for station metadata.

Features:
    - Reads from combined turnstile data (data/interim/turnstile/turnstile_combined.csv)
    - Extracts only STATION, LINENAME, DIVISION columns
    - Removes duplicate combinations while preserving all three columns
    - Saves unique station reference data to references/stations/stations_turnstile.csv

Usage:
    From project root:
        python scripts/extract_unique_stations_turnstile.py
    
    From scripts directory:
        python extract_unique_stations_turnstile.py

Output:
    references/stations/stations_turnstile.csv: Unique station combinations with
    STATION, LINENAME, and DIVISION columns sorted by station name.
"""

import sys
from pathlib import Path
import pandas as pd


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


def extract_unique_stations() -> None:
    """Extract unique station combinations from turnstile data."""
    # Define paths relative to project root
    base_dir = find_project_root()
    input_file = base_dir / "data" / "interim" / "turnstile" / "turnstile_combined.csv"
    output_dir = base_dir / "references" / "stations"
    output_file = output_dir / "stations_turnstile.csv"
    
    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print("🚇 Extracting unique station combinations from turnstile data...")
    
    # Read only the required columns
    df = pd.read_csv(input_file, usecols=['STATION', 'LINENAME', 'DIVISION'])
    
    print(f"📊 Read {len(df):,} records from {input_file.relative_to(base_dir)}")
    
    # Get unique combinations
    unique_stations = df.drop_duplicates().sort_values('STATION').reset_index(drop=True)
    
    print(f"✨ Found {len(unique_stations):,} unique station combinations")
    
    # Save to CSV
    unique_stations.to_csv(output_file, index=False)
    
    print(f"💾 Saved unique stations to {output_file.relative_to(base_dir)}")
    print("✅ Extraction completed successfully!")


def main():
    """Main entry point for the script."""
    extract_unique_stations()


if __name__ == "__main__":
    main() 