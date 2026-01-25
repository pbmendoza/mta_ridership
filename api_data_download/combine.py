#!/usr/bin/env python3
"""Combine monthly ridership CSVs (202501-202512) into a single 2025.csv."""

import sys
import time
from pathlib import Path

import pandas as pd

# Paths
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
SOURCE_DIR = SCRIPT_DIR / "data"
OUTPUT_PATH = PROJECT_ROOT / "data" / "raw" / "ridership" / "2025.csv"

# Monthly files to combine
MONTHLY_FILES = [f"2025{month:02d}.csv" for month in range(1, 13)]

# Dtype specification for memory efficiency and consistency
# Note: station_complex_id is str because it includes non-numeric IDs like "TRAM1"
DTYPES = {
    "transit_timestamp": str,
    "transit_mode": str,
    "station_complex_id": str,
    "station_complex": str,
    "borough": str,
    "payment_method": str,
    "fare_class_category": str,
    "ridership": "Int64",
    "transfers": "Int64",
    "latitude": float,
    "longitude": float,
    "georeference": str,
}

CHUNK_SIZE = 100_000

# Note: Georeference column is not used by the pipeline, so casing doesn't matter
# Keeping lowercase as provided by source API


def count_rows_fast(filepath: Path) -> int:
    """Count rows in CSV file without loading into memory."""
    with open(filepath, "r") as f:
        return sum(1 for _ in f) - 1  # Subtract header


def main():
    start_time = time.time()
    source_row_counts = {}
    total_written = 0
    first_chunk = True

    print(f"🚀 Combining {len(MONTHLY_FILES)} monthly files into {OUTPUT_PATH.name}")

    # Ensure output directory exists
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    try:
        for filename in MONTHLY_FILES:
            filepath = SOURCE_DIR / filename

            if not filepath.exists():
                print(f"❌ Missing file: {filename}")
                sys.exit(1)

            print(f"📁 Processing {filename}...", end=" ", flush=True)
            file_rows = 0

            for chunk in pd.read_csv(filepath, dtype=DTYPES, chunksize=CHUNK_SIZE):
                chunk.to_csv(
                    OUTPUT_PATH,
                    mode="w" if first_chunk else "a",
                    header=first_chunk,
                    index=False,
                )
                file_rows += len(chunk)
                first_chunk = False

            source_row_counts[filename] = file_rows
            total_written += file_rows
            print(f"({file_rows:,} rows)")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)

    # Validation: count rows in output file
    print("🔍 Validating output...", end=" ", flush=True)
    output_rows = count_rows_fast(OUTPUT_PATH)

    if output_rows == total_written:
        elapsed = time.time() - start_time
        print("✅")
        print(f"✅ Combined {total_written:,} rows from {len(MONTHLY_FILES)} files in {elapsed:.1f}s")
        print(f"📄 Output: {OUTPUT_PATH}")
    else:
        print(f"❌ Row count mismatch! Expected {total_written:,}, got {output_rows:,}")
        sys.exit(1)


if __name__ == "__main__":
    main()
