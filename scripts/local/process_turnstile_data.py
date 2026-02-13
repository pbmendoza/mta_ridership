#!/usr/bin/env python3
"""
Process MTA Turnstile Data for Daily Complex-Level Analysis
===========================================================

Purpose:
    Transform staged turnstile data into daily entry/exit aggregates by station complex.
    
Processing Steps:
    1. Load staged data with Complex ID mappings
    2. Remove outlier turnstiles with abnormally low record counts
    3. Create datetime columns and filter for hour/half-hour intervals (:00:00 or :30:00)
    4. Calculate entry/exit differences per turnstile
    5. Apply data quality filters (remove anomalies, counter resets)
    6. Handle midnight-spanning periods by attributing to start date
    7. Aggregate to daily totals by Complex ID
    8. Handle missing complexes and create summary statistics

Key Logic:
    - Filter for times ending in :00:00, :30:00, or :22:00 (includes Yankee Stadium special case)
    - Remove turnstiles with less than 10% of the modal record count (outliers)
    - Turnstile counters are cumulative, so we calculate differences
    - First readings per turnstile are excluded (avoids losing accumulated counts)
    - Filter unrealistic values (>7200 per 4 hours = 1 swipe per 2 seconds)
    - The 20:00-00:00 period crosses midnight but represents evening activity,
      so we attribute all ridership to the date when the period started (20:00)
    - Group by Complex ID for true station-level ridership
    - Handle counter resets by treating negative differences as zero
    - Output is filtered to START_YEAR through END_YEAR (configurable)

Usage:
    From project root:
        python scripts/local/process_turnstile_data.py
        
Output:
    - data/local/processed/turnstile/daily_ridership.csv
    - Summary statistics logged to console
"""

import sys
import logging
from pathlib import Path
from typing import Optional
import pandas as pd
from scipy import stats


# Configuration constants
USAGE_THRESHOLD_4HOURS = 7200  # Max reasonable: 1 swipe every 2 seconds for 4 hours
MIDNIGHT_SPAN_START = 20  # Hour that starts midnight-spanning period
OUTLIER_THRESHOLD_FACTOR = 0.1  # Remove turnstiles with < 10% of modal record count

# Year filtering configuration
START_YEAR = 2015  # First year to include in output
END_YEAR = 2019    # Last year to include in output


def find_project_root() -> Path:
    """Find the project root by looking for .git directory."""
    current = Path.cwd()
    
    if (current / '.git').exists():
        return current
    
    for parent in current.parents:
        if (parent / '.git').exists():
            return parent
    
    return current


class TurnstileDataProcessor:
    """Process staged turnstile data into daily complex-level aggregates."""
    
    def __init__(self, base_dir: Optional[Path] = None):
        """Initialize processor with paths and logging."""
        self.base_dir = base_dir or find_project_root()
        self.staging_dir = self.base_dir / "data" / "local" / "staging" / "turnstile"
        self.processed_dir = self.base_dir / "data" / "local" / "processed" / "turnstile"
        
        # Ensure output directory exists
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        
        # Setup logging
        self._setup_logging()
        
    def _setup_logging(self) -> None:
        """Configure logging."""
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(console_handler)
        self.logger.propagate = False
        
    def load_staged_data(self) -> pd.DataFrame:
        """Load the staged turnstile data."""
        input_path = self.staging_dir / "turnstile_combined.csv"
        
        if not input_path.exists():
            raise FileNotFoundError(f"Staged data not found: {input_path}")
        
        self.logger.info(f"📂 Loading staged data from: {input_path.relative_to(self.base_dir)}")
        
        df = pd.read_csv(input_path, dtype={
            'STATION': str,
            'LINENAME': str,
            'DESC': str,
            'turnstile_id': str,
            'station_id': str,
            'Complex ID': str
        })
        
        self.logger.info(f"✅ Loaded {len(df):,} records")
        
        # Check Complex ID coverage
        missing_complex = df['Complex ID'].isna().sum()
        if missing_complex > 0:
            self.logger.warning(f"⚠️  {missing_complex:,} records without Complex ID ({missing_complex/len(df)*100:.1f}%)")
        
        return df
        
    def remove_outlier_turnstiles(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove turnstiles with abnormally low record counts."""
        self.logger.info("🔍 Removing outlier turnstiles...")
        
        # Count records per turnstile
        turnstile_counts = df.groupby('turnstile_id').size()
        
        # Calculate the mode (most common count) and threshold
        mode_result = stats.mode(turnstile_counts)
        # Handle both old and new scipy API
        if hasattr(mode_result.mode, 'shape') and mode_result.mode.shape == ():
            # Scalar mode (new scipy)
            mode_count = float(mode_result.mode)
        elif hasattr(mode_result.mode, '__getitem__'):
            # Array mode (old scipy)
            mode_count = float(mode_result.mode[0])
        else:
            # Fallback
            mode_count = float(mode_result.mode)
        threshold = mode_count * OUTLIER_THRESHOLD_FACTOR
        
        # Identify outlier turnstiles
        outlier_turnstiles = turnstile_counts[turnstile_counts < threshold].index
        
        # Filter out outliers
        df_filtered = df[~df['turnstile_id'].isin(outlier_turnstiles)].copy()
        
        if len(outlier_turnstiles) > 0:
            records_removed = len(df) - len(df_filtered)
            self.logger.info(f"   ❌ Removed {len(outlier_turnstiles):,} turnstiles with < {threshold:.0f} records")
            self.logger.info(f"   📊 Mode record count: {mode_count:.0f} records per turnstile")
            self.logger.info(f"   🗑️  {records_removed:,} total records removed")
        else:
            self.logger.info("   ✅ No outlier turnstiles found")
        
        return df_filtered
        
    def prepare_datetime_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create datetime columns and filter for regular intervals."""
        self.logger.info("📅 Creating datetime columns...")
        
        # Convert DATE and create DATETIME
        df['DATE'] = pd.to_datetime(df['DATE'], format='%m/%d/%Y')
        df['DATETIME'] = pd.to_datetime(df['DATE'].astype(str) + ' ' + df['TIME'])
        
        # Filter for times ending in :00:00, :30:00, or :22:00
        # This captures more regular reporting patterns than just 4-hour intervals
        # Note: Station 604 (161 St-Yankee Stadium) reports at :22:00 instead of standard times
        df_filtered = df[
            (df['TIME'].str[-5:] == '00:00') | 
            (df['TIME'].str[-5:] == '30:00') |
            (df['TIME'].str[-5:] == '22:00')  # Special case for Yankee Stadium
        ].copy()
        
        records_removed = len(df) - len(df_filtered)
        self.logger.info(f"   🕐 Kept {len(df_filtered):,} records at :00:00/:30:00/:22:00 marks ({records_removed:,} removed)")
        
        # Extract hour for midnight-spanning logic
        df_filtered['HOUR'] = df_filtered['DATETIME'].dt.hour
        
        return df_filtered
        
    def calculate_ridership(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate entry/exit differences with proper handling of midnight-spanning periods.
        
        For the 20:00-00:00 period that spans midnight:
        - All ridership is attributed to the date of the 20:00 reading
        - This keeps late evening ridership with the correct day
        """
        self.logger.info("🔢 Calculating ridership from cumulative counters...")
        
        # Sort by turnstile and datetime for proper diff calculation
        df = df.sort_values(['turnstile_id', 'DATETIME'])
        
        # Get previous reading time for each record
        df['PREV_DATETIME'] = df.groupby('turnstile_id')['DATETIME'].shift(1)
        
        # Calculate differences within each turnstile
        df['ENTRIES_DIFF'] = df.groupby('turnstile_id')['ENTRIES'].diff()
        df['EXITS_DIFF'] = df.groupby('turnstile_id')['EXITS'].diff()
        
        # Remove first reading of each turnstile (no previous value to calculate diff)
        # This avoids losing accumulated counts when turnstiles first appear
        df = df[df['ENTRIES_DIFF'].notna() & df['EXITS_DIFF'].notna()].copy()
        
        # Apply quality filters using vectorized operations
        df['ENTRIES_DIFF'] = df['ENTRIES_DIFF'].clip(lower=0, upper=USAGE_THRESHOLD_4HOURS)
        df['EXITS_DIFF'] = df['EXITS_DIFF'].clip(lower=0, upper=USAGE_THRESHOLD_4HOURS)
        
        # For midnight-spanning periods (20:00 to 00:00), use the date of the START time
        # This means the 20:00 reading's date will be used for the period's ridership
        df['RIDERSHIP_DATE'] = df['PREV_DATETIME'].dt.date
        
        # For records without previous datetime (first readings), use current date
        df.loc[df['RIDERSHIP_DATE'].isna(), 'RIDERSHIP_DATE'] = df.loc[df['RIDERSHIP_DATE'].isna(), 'DATE'].dt.date
        
        # Log statistics
        total_entries = df['ENTRIES_DIFF'].sum()
        total_exits = df['EXITS_DIFF'].sum()
        first_readings_removed = df.groupby('turnstile_id').size().count()
        self.logger.info(f"   📊 Removed {first_readings_removed:,} first readings (one per turnstile)")
        self.logger.info(f"   🚶 Total entries: {total_entries:,.0f}")
        self.logger.info(f"   🚪 Total exits: {total_exits:,.0f}")
        
        # Check midnight-spanning periods
        midnight_spans = df[
            (df['PREV_DATETIME'].dt.hour == MIDNIGHT_SPAN_START) & 
            (df['DATETIME'].dt.hour == 0)
        ]
        if len(midnight_spans) > 0:
            self.logger.info(f"   🌙 Found {len(midnight_spans):,} midnight-spanning periods (20:00-00:00)")
            self.logger.info(f"      These periods' ridership attributed to the earlier day")
        
        return df
        
    def aggregate_by_complex_daily(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aggregate ridership to daily totals by Complex ID."""
        self.logger.info("📊 Aggregating to daily complex-level ridership...")
        
        # Only process records with Complex ID
        df_with_complex = df[df['Complex ID'].notna()].copy()
        
        if len(df_with_complex) < len(df):
            dropped = len(df) - len(df_with_complex)
            self.logger.warning(f"   ⚠️  Dropped {dropped:,} records without Complex ID")
        
        # Group by Complex ID and RIDERSHIP_DATE (not DATE)
        daily_complex = df_with_complex.groupby(['Complex ID', 'RIDERSHIP_DATE']).agg({
            'ENTRIES_DIFF': 'sum',
            'EXITS_DIFF': 'sum',
            'turnstile_id': 'nunique',  # Count of active turnstiles
            'STATION': lambda x: x.mode()[0] if not x.empty else '',  # Most common station name
            'LINENAME': lambda x: ', '.join(sorted(set(x)))  # All lines serving complex
        }).reset_index()
        
        # Rename columns for clarity
        daily_complex.rename(columns={
            'RIDERSHIP_DATE': 'DATE',
            'ENTRIES_DIFF': 'ENTRIES',
            'EXITS_DIFF': 'EXITS',
            'turnstile_id': 'TURNSTILE_COUNT',
            'STATION': 'PRIMARY_STATION_NAME',
            'LINENAME': 'LINES'
        }, inplace=True)
        
        # Convert DATE back to datetime for consistency
        daily_complex['DATE'] = pd.to_datetime(daily_complex['DATE'])
        
        # Sort by date and complex
        daily_complex = daily_complex.sort_values(['DATE', 'Complex ID'])
        
        self.logger.info(f"   ✅ Created {len(daily_complex):,} daily complex records")
        self.logger.info(f"   🏢 Covering {daily_complex['Complex ID'].nunique()} unique complexes")
        
        return daily_complex
        
    def add_summary_statistics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add useful summary statistics to the dataset."""
        self.logger.info("📈 Adding summary statistics...")
        
        # Add day of week
        df['DAY_OF_WEEK'] = df['DATE'].dt.day_name()
        df['WEEKDAY'] = df['DATE'].dt.dayofweek < 5  # Monday=0, Sunday=6
        
        # Add month and year for easier filtering
        df['YEAR'] = df['DATE'].dt.year
        df['MONTH'] = df['DATE'].dt.month
        
        return df
        
    def save_processed_data(self, df: pd.DataFrame) -> None:
        """Save the processed data to CSV."""
        output_path = self.processed_dir / "daily_ridership.csv"
        
        self.logger.info(f"💾 Saving processed data to: {output_path.relative_to(self.base_dir)}")
        
        # Filter data by year range
        df_filtered = df[(df['YEAR'] >= START_YEAR) & (df['YEAR'] <= END_YEAR)].copy()
        
        if len(df_filtered) < len(df):
            self.logger.info(f"   📅 Filtered to years {START_YEAR}-{END_YEAR}: {len(df_filtered):,} records (from {len(df):,})")
        
        # Convert date back to string for CSV
        df_filtered['DATE'] = df_filtered['DATE'].dt.strftime('%Y-%m-%d')
        
        df_filtered.to_csv(output_path, index=False)
        
        self.logger.info(f"   ✅ Saved {len(df_filtered):,} daily complex records")
        
    def print_summary_stats(self, df: pd.DataFrame) -> None:
        """Print summary statistics about the processed data."""
        self.logger.info("\n" + "="*60)
        self.logger.info("📊 SUMMARY STATISTICS")
        self.logger.info("="*60)
        
        # Date range
        date_min = df['DATE'].min()
        date_max = df['DATE'].max()
        self.logger.info(f"📅 Date range: {date_min.strftime('%Y-%m-%d')} to {date_max.strftime('%Y-%m-%d')}")
        
        # Complex statistics
        self.logger.info(f"🏢 Total complexes: {df['Complex ID'].nunique()}")
        
        # Ridership statistics
        total_entries = df['ENTRIES'].sum()
        total_exits = df['EXITS'].sum()
        
        self.logger.info(f"\n🚇 RIDERSHIP TOTALS:")
        self.logger.info(f"   Entries: {total_entries:,.0f}")
        self.logger.info(f"   Exits: {total_exits:,.0f}")
        
        # Top 10 complexes by entries
        top_complexes = df.groupby('Complex ID').agg({
            'ENTRIES': 'sum',
            'PRIMARY_STATION_NAME': 'first'
        }).sort_values('ENTRIES', ascending=False).head(10)
        
        self.logger.info(f"\n🏆 TOP 10 COMPLEXES BY ENTRIES:")
        for idx, (complex_id, row) in enumerate(top_complexes.iterrows(), 1):
            entries = row['ENTRIES']
            station = row['PRIMARY_STATION_NAME']
            self.logger.info(f"   {idx}. {complex_id} ({station}): {entries:,.0f} entries")
            
        self.logger.info("="*60)
        
    def run(self) -> None:
        """Execute the complete processing pipeline."""
        self.logger.info("="*60)
        self.logger.info("🚀 Starting Turnstile Data Processing")
        self.logger.info(f"📂 Project root: {self.base_dir}")
        self.logger.info("="*60)
        
        try:
            # Load staged data
            df = self.load_staged_data()
            
            # Remove outlier turnstiles
            df = self.remove_outlier_turnstiles(df)
            
            # Prepare datetime columns and filter intervals
            df = self.prepare_datetime_columns(df)
            
            # Calculate ridership differences
            df = self.calculate_ridership(df)
            
            # Aggregate to daily complex level
            daily_df = self.aggregate_by_complex_daily(df)
            
            # Add summary statistics
            daily_df = self.add_summary_statistics(daily_df)
            
            # Save processed data
            self.save_processed_data(daily_df)
            
            # Print summary
            self.print_summary_stats(daily_df)
            
            self.logger.info("\n✨ Processing completed successfully!")
            
        except Exception as e:
            self.logger.error(f"💥 Processing failed: {str(e)}")
            raise


def main():
    """Main entry point."""
    processor = TurnstileDataProcessor()
    processor.run()


if __name__ == "__main__":
    main()