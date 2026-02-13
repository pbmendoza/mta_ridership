# MTA Turnstile Data Documentation

Last updated: 2025-11-07

## Overview

The MTA Turnstile Dataset provides granular subway ridership data from 2010-2023, capturing entry and exit counts for individual turnstiles across all NYC subway stations. This historical dataset forms the foundation for pre-pandemic baseline analysis. By default, the pipeline processes only the modern CSV format (files dated ≥ 2014-10-18); the legacy 2010–2014 format is documented below but skipped unless explicitly enabled.

**Status**: ⚠️ **RETIRED** - No longer actively maintained by MTA (replaced by modern ridership API)

## Data Location

**Raw Files**: [`data/local/raw/turnstile/`](../../data/local/raw/turnstile/)
- Format: Plain text files (`turnstile_YYMMDD.txt`)
- Frequency: Weekly snapshots (Saturdays)
- Size: ~750 files covering 13+ years

## Data Format Evolution

### Legacy Format (May 2010 - October 2014)

**Files**: `turnstile_100505.txt` through `turnstile_141011.txt`

This format is notoriously difficult to parse due to its multi-reading structure.

#### Structure
Each row contains:
1. **Turnstile Identifiers** (3 columns):
   - `CA`: Control Area code
   - `UNIT`: Remote unit ID
   - `SCP`: Subunit Channel Position

2. **Multiple Reading Groups** (up to 8 per row):
   Each group contains 5 fields:
   - `DATE`: MM-DD-YY format (e.g., "04-17-10")
   - `TIME`: 24-hour format (e.g., "00:00:00")
   - `DESC`: Audit type (usually "REGULAR")
   - `ENTRIES`: Cumulative entry count
   - `EXITS`: Cumulative exit count

#### Example
```
A002,R051,02-00-00,04-17-10,00:00:00,REGULAR,002704717,000928793,04-17-10,04:00:00,REGULAR,002704723,000928795,...
```
*One row = multiple readings for the same turnstile*

### Modern Format (October 2014 - May 2023)

**Files**: `turnstile_141018.txt` onwards

Clean CSV format with one reading per row - much easier to process!

#### Column Structure
| Column | Description | Example |
|--------|-------------|---------|
| `C/A` | Control Area code | A002 |
| `UNIT` | Remote unit ID | R051 |
| `SCP` | Subunit Channel Position | 02-00-00 |
| `STATION` | Station name | LEXINGTON AVE |
| `LINENAME` | Lines serving station | NQR456 |
| `DIVISION` | Subway division* | BMT |
| `DATE` | Recording date (MM/DD/YYYY) | 10/31/2015 |
| `TIME` | Recording time (HH:MM:SS) | 00:00:00 |
| `DESC` | Audit type** | REGULAR |
| `ENTRIES` | Cumulative entry count | 5382388 |
| `EXITS` | Cumulative exit count | 1818635 |

*Division values: BMT, IRT, IND (subway) or PTH, SRT, RIT (other systems)
**DESC values: REGULAR (scheduled) or RECOVR AUD (recovered missed audit)

#### Unique Identifier
`UNIT + C/A + SCP` = Unique turnstile ID

#### Example
```
C/A,UNIT,SCP,STATION,LINENAME,DIVISION,DATE,TIME,DESC,ENTRIES,EXITS
A002,R051,02-00-00,LEXINGTON AVE,NQR456,BMT,10/31/2015,00:00:00,REGULAR,0005382388,0001818635
```

## Data Quality Issues

### Corrupted File Alert
**File**: [`turnstile_120714.txt`](../../data/local/raw/turnstile/turnstile_120714.txt) (July 14, 2012)

Severe corruption including:
- Malformed headers with field names as data
- Inconsistent column counts
- Missing timestamps

**Example of corruption**:
```
NUMBER,OF,ENTRIES,2,NUMBER,,07-07-12,11:18:30,002918817,OF,,07-07-12,11:19:55,002918828,EXITS...
```
**Action**: Exclude from analysis or apply specialized preprocessing

### Station Removal: ORCHARD BEACH

The station name "ORCHARD BEACH" appeared in turnstile data from [dates to be determined] associated with the 6 train line. This presents a puzzling anomaly, as the Lexington Avenue/IRT Pelham Line (the "6" and "<6>" trains) has terminated at Pelham Bay Park since the final extension opened on December 20, 1920. The tracks have never been extended the additional mile to Orchard Beach.

Investigation reveals that control area "OB01" wasn't for a subway station at all—it was for a bank of bus-fare turnstiles that once stood in the Orchard Beach bus loop.

The historical context: After World War II, the city's Surface Transportation Corp. (later MABSTOA) installed waist-high turnstiles at the beach to manage the huge crowds. Beachgoers would queue, drop a special "Orchard Beach Turnstile" token, and board return buses to Pelham Bay Park without requiring the driver to collect fares. These tokens first appeared in June 1949 and were struck again in 1954. Since these were legitimate fare-control equipment, when NYCT computerized the system in the 1990s, it assigned the location a standard four-character control-area ID: OB01 (Orchard Beach, control area #1).

**Processing Action**: These records are filtered out and quarantined during data processing. See [`data/local/quarantine/turnstile/ORCHARD_BEACH_records.csv`](../../data/local/quarantine/turnstile/ORCHARD_BEACH_records.csv). Also see filtering rationale in [`references/docs/data_filters.md`](data_filters.md).

## Processed Data Summary

### Combined Dataset
**File**: [`data/local/staging/turnstile/turnstile_combined.csv`](../../data/local/staging/turnstile/turnstile_combined.csv)
- **Total Records**: 90,263,930
- **Date Range**: October 11, 2014 - May 5, 2023
- **Unique Stations**: 572
- **Unique Control Areas**: 471

Counts are approximate and may vary slightly with changes to quality filters or parsing.

### Raw Monthly Historical Output (2015–2019)
For non-averaged monthly historical totals based on 2015–2019, see:
- [`data/local/ridership_turnstile/raw_monthly_turnstile_2015_2019.csv`](../../data/local/ridership_turnstile/raw_monthly_turnstile_2015_2019.csv)
  - Columns: `complex_id`, `station_name`, `year`, `month`, `entries`, `exits`

## 🔗 Alternative Data Sources

While our raw files are no longer updated, NY Open Data provides consolidated annual datasets:

| Year | Records | NY Open Data Link |
|------|---------|-------------------|
| 2014 | - | [MTA Subway Turnstile Usage Data 2014](https://data.ny.gov/Transportation/MTA-Subway-Turnstile-Usage-Data-2014/i55r-43gk/) |
| 2015 | 10.1M | [MTA Subway Turnstile Usage Data 2015](https://data.ny.gov/Transportation/MTA-Subway-Turnstile-Usage-Data-2015/ug6q-shqc/) |
| 2016 | 10.1M | [MTA Subway Turnstile Usage Data 2016](https://data.ny.gov/Transportation/MTA-Subway-Turnstile-Usage-Data-2016/ekwu-khcy/) |
| 2017 | 10.3M | [MTA Subway Turnstile Usage Data 2017](https://data.ny.gov/Transportation/MTA-Subway-Turnstile-Usage-Data-2017/v5y5-mwpb/) |
| 2018 | 10.3M | [MTA Subway Turnstile Usage Data 2018](https://data.ny.gov/Transportation/MTA-Subway-Turnstile-Usage-Data-2018/bjcb-yee3/) |
| 2019 | 10.7M | [MTA Subway Turnstile Usage Data 2019](https://data.ny.gov/Transportation/MTA-Subway-Turnstile-Usage-Data-2019/xfn5-qji9/) |
| 2020 | 10.9M | [MTA Subway Turnstile Usage Data 2020](https://data.ny.gov/Transportation/MTA-Subway-Turnstile-Usage-Data-2020/py8k-a8wg/) |
| 2021 | 10.9M | [MTA Subway Turnstile Usage Data 2021](https://data.ny.gov/Transportation/MTA-Subway-Turnstile-Usage-Data-2021/uu7b-3kff/) |
| 2022 | 11.0M | [MTA Subway Turnstile Usage Data 2022](https://data.ny.gov/Transportation/MTA-Subway-Turnstile-Usage-Data-2022/k7j9-jnct/) |

## ✅ Validation Results

Our processing pipeline produces results that closely match MTA's official statistics:

### Total Annual Entries Comparison

| Year | Our Calculation | MTA Official | Difference |
|------|----------------|--------------|------------|
| 2015 | 1,755,851,468 | 1,762,565,419 | -0.38% |
| 2016 | 1,746,225,104 | 1,756,814,800 | -0.60% |
| 2017 | 1,712,429,242 | 1,727,366,607 | -0.86% |
| 2018 | 1,667,636,818 | 1,680,060,402 | -0.74% |
| 2019 | 1,684,697,825 | 1,697,787,002 | -0.77% |

*Source: [MTA Subway & Bus Ridership 2019](https://www.mta.info/agency/new-york-city-transit/subway-bus-ridership-2019)*

The small differences (<1%) are likely due to:
- Data cleaning and outlier removal
- Handling of counter resets
- Exclusion of non-subway stations

## 🛠️ Processing Pipeline

### Key Scripts
1. [`scripts/local/stage_turnstile_data.py`](../../scripts/local/stage_turnstile_data.py) - Combines and standardizes raw files
2. [`scripts/local/process_turnstile_data.py`](../../scripts/local/process_turnstile_data.py) - Calculates daily ridership from cumulative counters

### Processing Features
- **Counter Reset Detection**: Handles when cumulative counters reset to zero
- **Outlier Removal**: Filters unrealistic entry/exit values
- **Station Mapping**: Links to official station complex IDs
- **Time Period Handling**: Special logic for midnight-spanning periods

## 📝 Usage Notes

1. **Cumulative Counters**: Entry/exit values are cumulative - calculate differences between readings
2. **4-Hour Intervals**: Regular audits occur every 4 hours (sometimes more frequent)
3. **Station Names**: Use [`stations_turnstile_mapping.csv`](../stations/stations_turnstile_mapping.csv) to map to official complex IDs
4. **Baseline Period**: 2015-2019 data serves as pre-pandemic baseline
