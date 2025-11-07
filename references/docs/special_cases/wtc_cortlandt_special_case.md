# WTC-Cortlandt Station Special Case

Last updated: 2025-11-07

## Overview

WTC-Cortlandt (Complex ID: 328) requires special handling in baseline calculations due to its extended closure following the September 11, 2001 attacks.

## Station History

- **Closed**: September 11, 2001
- **Reopened**: September 29, 2018
- **Data Gap**: 17+ years (2001-2018)

## Impact on Baseline Calculations

The standard baseline calculation uses 2015-2019 data to establish pre-pandemic ridership patterns. However, WTC-Cortlandt presents a unique challenge:

- **2015-2017**: No data (station closed)
- **2018**: Only 3 months of data (Sept 29 - Dec 31)
- **2019**: Full year of data available

## Implementation (CSV-driven)

Configure this station in `references/baseline_special_cases.csv` so the baseline calculator uses only 2019:

```csv
complex_id,baseline_years,station_name,reason,notes
328,2019,WTC-Cortlandt (1),Station closed from 9/11/2001 until 9/29/2018. Only 2019 provides full year of data.,Using single year baseline may not fully represent typical ridership patterns as station was still recovering after 17-year closure.
```

The `scripts/calculate_baseline.py` script reads this configuration and applies the correct averaging (no division since only one year is specified). Use `scripts/verify_baseline_special_cases.py` to validate the configuration against official station references and see `logs/` for verification output.

## Data Verification

Monthly ridership totals for WTC-Cortlandt in 2019:

| Month | Entries | Exits |
|-------|---------|-------|
| Jan | 325,906 | 256,251 |
| Feb | 315,739 | 248,935 |
| Mar | 350,628 | 274,336 |
| Apr | 352,346 | 278,870 |
| May | 356,470 | 289,904 |
| Jun | 352,847 | 280,659 |
| Jul | 389,614 | 316,501 |
| Aug | 344,932 | 287,303 |
| Sep | 346,371 | 280,406 |
| Oct | 390,187 | 333,181 |
| Nov | 345,880 | 276,547 |
| Dec | 402,387 | 318,903 |

## Considerations

1. **Baseline Accuracy**: The 2019-only baseline may not fully represent typical ridership patterns, as:
   - The station was still recovering ridership after 17 years of closure
   - Nearby stations may have absorbed regular commuters during the closure
   - New development in the area may have changed ridership patterns

2. **Alternative Approaches**: Future analyses might consider:
   - Using a weighted average with nearby stations
   - Applying growth factors based on area development
   - Excluding WTC-Cortlandt from baseline comparisons

3. **Documentation**: Any analysis using WTC-Cortlandt baseline data should note this limitation.

## Related Files

- `scripts/calculate_baseline.py`: Contains special handling logic
- `scripts/process_turnstile_data.py`: Processes raw turnstile data
- `data/processed/turnstile/daily_ridership.csv`: Contains the 2018-2019 data