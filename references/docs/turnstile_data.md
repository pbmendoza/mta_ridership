# MTA Turnstile Data Documentation

## Overview

This dataset contains historical turnstile data from the New York City Subway system, capturing entry and exit counts for individual control areas (turnstiles) across all stations. The data provides granular ridership information collected at regular intervals, typically every four hours.

**Important Note:** These datasets are now retired and no longer actively maintained or updated by the MTA.

## Data Location

The turnstile data files are stored in plain text format (.txt) within the following directory:
```
data/raw/turnstile/
```

## Data Format Evolution

The MTA turnstile dataset has undergone significant structural changes throughout its history. Understanding these format variations is crucial for proper data processing and analysis.

### Legacy Format (May 2010 - October 2014)

Files from `turnstile_100505.txt` through `turnstile_141011.txt` utilize a complex, non-standard format that presents unique parsing challenges:

#### Structure Characteristics:
- **Multi-reading rows**: Each line contains multiple timestamp readings for a single turnstile device
- **Identifier columns**: Each row begins with three key identifiers:
  - `CA`: Control Area code
  - `UNIT`: Remote unit ID  
  - `SCP`: Subunit Channel Position
- **Reading groups**: Following the identifiers, data appears in repeating groups of five fields:
  - `DATE`: Recording date (MM-DD-YY format, e.g., "04-17-10")
  - `TIME`: Recording time (24-hour format, e.g., "00:00:00")
  - `DESC`: Description/audit type (typically "REGULAR")
  - `ENTRIES`: Cumulative entry count
  - `EXITS`: Cumulative exit count
- **Variable column count**: Rows contain up to 8 reading groups (40 data columns plus 3 identifiers = 43 total columns)
- **Inconsistent structure**: Not all rows contain the same number of readings

#### Example Record (Legacy Format):
```
A002,R051,02-00-00,04-17-10,00:00:00,REGULAR,002704717,000928793,04-17-10,04:00:00,REGULAR,002704723,000928795,...
```

This format requires specialized parsing logic to extract individual readings and transform them into a standard tabular structure.

### Modern Standardized Format (October 2014 - May 2023)

Beginning with `turnstile_141018.txt` (October 18, 2014), the MTA adopted a clean, consistent CSV format that significantly improved data accessibility:

#### Structure Characteristics:
- **Header row**: Files include a descriptive header with column names
- **One reading per row**: Each row represents a single turnstile reading at a specific timestamp
- **Consistent columns**: All rows maintain the same column structure:
  - `C/A`: Control Area code
  - `UNIT`: Remote unit ID
  - `SCP`: Subunit Channel Position  
  - `STATION`: Station name
  - `LINENAME`: Subway lines serving the station
  - `DIVISION`: Operating division (BMT, IND, or IRT)
  - `DATE`: Recording date (MM/DD/YYYY format)
  - `TIME`: Recording time (HH:MM:SS format)
  - `DESC`: Audit description
  - `ENTRIES`: Cumulative entry count
  - `EXITS`: Cumulative exit count
- **Standard formatting**: Consistent date/time formats and field delimiters

#### Example Record (Modern Format):
```
A002,R051,02-00-00,LEXINGTON AVE,NQR456,BMT,10/31/2015,00:00:00,REGULAR,0005382388,0001818635
```

### Data Quality Issues

#### Corrupted File: turnstile_120714.txt

The file dated July 14, 2012 (`turnstile_120714.txt`) contains severely corrupted data that deviates from the expected format of its time period. Issues include:

- **Malformed headers**: Field names appear as data values (e.g., "NUMBER,OF,ENTRIES,2,NUMBER")
- **Inconsistent structure**: Irregular column counts and field patterns
- **Missing data**: Incomplete timestamps and missing required fields
- **Mixed content**: Combination of corrupted headers and partial data records

Example of corruption:
```
NUMBER,OF,ENTRIES,2,NUMBER,,07-07-12,11:18:30,002918817,OF,,07-07-12,11:19:55,002918828,EXITS,,07-07-12,,002918839,3,,07-07-12,,000000022,000995988,,,
```

**Recommendation:** This file should be excluded from analysis or requires specialized preprocessing to salvage usable data.

## Data Availability

While the original MTA source is no longer active, consolidated annual datasets are available through NY Open Data:

| Year | Link |
|------|------|
| 2014 | [https://data.ny.gov/Transportation/MTA-Subway-Turnstile-Usage-Data-2014/i55r-43gk/](https://data.ny.gov/Transportation/MTA-Subway-Turnstile-Usage-Data-2014/i55r-43gk/) |
| 2015 | [https://data.ny.gov/Transportation/MTA-Subway-Turnstile-Usage-Data-2015/ug6q-shqc/](https://data.ny.gov/Transportation/MTA-Subway-Turnstile-Usage-Data-2015/ug6q-shqc/) |
| 2016 | [https://data.ny.gov/Transportation/MTA-Subway-Turnstile-Usage-Data-2016/ekwu-khcy/](https://data.ny.gov/Transportation/MTA-Subway-Turnstile-Usage-Data-2016/ekwu-khcy/) |
| 2017 | [https://data.ny.gov/Transportation/MTA-Subway-Turnstile-Usage-Data-2017/v5y5-mwpb/](https://data.ny.gov/Transportation/MTA-Subway-Turnstile-Usage-Data-2017/v5y5-mwpb/) |
| 2018 | [https://data.ny.gov/Transportation/MTA-Subway-Turnstile-Usage-Data-2018/bjcb-yee3/](https://data.ny.gov/Transportation/MTA-Subway-Turnstile-Usage-Data-2018/bjcb-yee3/) |
| 2019 | [https://data.ny.gov/Transportation/MTA-Subway-Turnstile-Usage-Data-2019/xfn5-qji9/](https://data.ny.gov/Transportation/MTA-Subway-Turnstile-Usage-Data-2019/xfn5-qji9/) |
| 2020 | [https://data.ny.gov/Transportation/MTA-Subway-Turnstile-Usage-Data-2020/py8k-a8wg/](https://data.ny.gov/Transportation/MTA-Subway-Turnstile-Usage-Data-2020/py8k-a8wg/) |
| 2021 | [https://data.ny.gov/Transportation/MTA-Subway-Turnstile-Usage-Data-2021/uu7b-3kff/](https://data.ny.gov/Transportation/MTA-Subway-Turnstile-Usage-Data-2021/uu7b-3kff/) |
| 2022 | [https://data.ny.gov/Transportation/MTA-Subway-Turnstile-Usage-Data-2022/k7j9-jnct/](https://data.ny.gov/Transportation/MTA-Subway-Turnstile-Usage-Data-2022/k7j9-jnct/) |