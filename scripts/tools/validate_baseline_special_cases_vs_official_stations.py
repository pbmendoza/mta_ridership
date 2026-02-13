#!/usr/bin/env python3
"""Validate baseline special cases against official station complex definitions.

Inputs:
    - references/baseline_special_cases.csv
      Expected columns: complex_id, station_name, baseline_years
    - references/stations/stations_complexes_official.csv
      Expected columns: Complex ID, Stop Name, Display Name

Behavior:
    1) Loads both CSV files.
    2) Builds the official full station name as ``Stop Name`` + ``Display Name``.
    3) Validates each special-case row to ensure:
       - complex_id exists in the official file,
       - station_name matches one of that complex's official station names.
    4) Prints all discrepancies and duplicate complex_id entries.

Outputs:
    - Human-readable verification report to stdout.
    - Optional file logs/baseline_special_cases_verification.txt when
      discrepancies are found.
"""

import pandas as pd
from pathlib import Path

def find_project_root(start: Path | None = None) -> Path:
    """Find the project root by searching upward for a ``.git`` directory."""
    current = start or Path.cwd()

    for path in [current, *current.parents]:
        if (path / '.git').exists():
            return path

    return current

def main():
    # Set up paths
    base_dir = find_project_root(Path(__file__).resolve().parent)
    special_cases_path = base_dir / "references" / "baseline_special_cases.csv"
    official_stations_path = base_dir / "references" / "stations" / "stations_complexes_official.csv"
    
    # Load data
    special_cases_df = pd.read_csv(special_cases_path)
    official_df = pd.read_csv(official_stations_path)
    
    # Create a mapping of complex_id to station names in official file
    # Combine "Stop Name" and "Display Name" to match the format in special cases
    official_df['Station Full Name'] = official_df['Stop Name'] + official_df['Display Name']
    
    # Create complex_id to station names mapping
    complex_id_to_names = {}
    for _, row in official_df.iterrows():
        complex_id = row['Complex ID']
        station_name = row['Station Full Name']
        if complex_id not in complex_id_to_names:
            complex_id_to_names[complex_id] = []
        complex_id_to_names[complex_id].append(station_name)
    
    # Verify each special case
    print("Verification Report: baseline_special_cases.csv vs stations_complexes_official.csv")
    print("=" * 80)
    
    discrepancies = []
    
    for idx, row in special_cases_df.iterrows():
        complex_id = row['complex_id']
        station_name = row['station_name']
        
        # Check if complex_id exists
        if complex_id not in complex_id_to_names:
            discrepancies.append({
                'row': idx + 2,  # +2 for 1-based indexing and header
                'complex_id': complex_id,
                'station_name': station_name,
                'issue': 'Complex ID not found in official file'
            })
        else:
            # Check if station name matches any of the names for this complex
            official_names = complex_id_to_names[complex_id]
            if station_name not in official_names:
                discrepancies.append({
                    'row': idx + 2,
                    'complex_id': complex_id,
                    'station_name': station_name,
                    'issue': 'Station name mismatch',
                    'official_names': official_names
                })
    
    # Report results
    if not discrepancies:
        print("\n✓ All complex_id and station_name values match correctly!")
    else:
        print(f"\n✗ Found {len(discrepancies)} discrepancies:\n")
        
        for disc in discrepancies:
            print(f"Row {disc['row']}:")
            print(f"  Complex ID: {disc['complex_id']}")
            print(f"  Station Name: {disc['station_name']}")
            print(f"  Issue: {disc['issue']}")
            if 'official_names' in disc:
                print(f"  Expected name(s) in official file:")
                for name in disc['official_names']:
                    print(f"    - {name}")
            print()
    
    # Additional analysis: Show duplicate complex IDs in special cases
    complex_id_counts = special_cases_df['complex_id'].value_counts()
    duplicates = complex_id_counts[complex_id_counts > 1]
    
    if not duplicates.empty:
        print("\nNote: The following complex IDs appear multiple times in special cases:")
        for complex_id, count in duplicates.items():
            print(f"  Complex ID {complex_id}: {count} times")
            # Show the station names for this complex
            entries = special_cases_df[special_cases_df['complex_id'] == complex_id]
            for _, entry in entries.iterrows():
                print(f"    - {entry['station_name']} (years: {entry['baseline_years']})")
        print()
    
    # Summary statistics
    print("\nSummary:")
    print(f"  Total special case entries: {len(special_cases_df)}")
    print(f"  Unique complex IDs in special cases: {special_cases_df['complex_id'].nunique()}")
    print(f"  Total discrepancies found: {len(discrepancies)}")
    
    # Save detailed report
    if discrepancies:
        report_path = base_dir / "logs" / "baseline_special_cases_verification.txt"
        report_path.parent.mkdir(exist_ok=True)
        
        with open(report_path, 'w') as f:
            f.write("Baseline Special Cases Verification Report\n")
            f.write("=" * 80 + "\n\n")
            
            for disc in discrepancies:
                f.write(f"Row {disc['row']}:\n")
                f.write(f"  Complex ID: {disc['complex_id']}\n")
                f.write(f"  Station Name: {disc['station_name']}\n")
                f.write(f"  Issue: {disc['issue']}\n")
                if 'official_names' in disc:
                    f.write(f"  Expected name(s) in official file:\n")
                    for name in disc['official_names']:
                        f.write(f"    - {name}\n")
                f.write("\n")
        
        print(f"\nDetailed report saved to: {report_path.relative_to(base_dir)}")

if __name__ == "__main__":
    main()
