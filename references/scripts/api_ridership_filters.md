# API Ridership Filters

Shared data filters applied in the API-based ridership scripts to ensure consistency between ongoing ridership tracking and baseline computation.

## Scripts

| Script | Purpose |
|---|---|
| `scripts/api/calculate_ridership_by_station.py` | Monthly station ridership from 2020+ API data |
| `scripts/api/calculate_baseline.py` | 2017-2019 baseline averages from `t69i-h2me` dataset |

## Filters

### 1. Subway Only

```sql
transit_mode = 'subway'
```

Excludes bus, commuter rail, and other non-subway transit modes.

### 2. Exclude Station Complex 502

```sql
station_complex_id != '502'
```

Station 502 is excluded from all queries. This is a known data quality exclusion carried over from the local pipeline.

### 3. SIR Exclusion (Grid-Fill)

```python
stations_df[stations_df["Daytime Routes"] != "SIR"]
```

Staten Island Railway stations are excluded during grid-fill. They are not present in the API ridership data, so they would otherwise appear as zero-filled rows.

### 4. Weekday / Weekend Split

```sql
-- Weekday (Mon-Fri)
date_extract_dow(transit_timestamp) NOT IN (0, 6)

-- Weekend (Sat-Sun)
date_extract_dow(transit_timestamp) IN (0, 6)
```

Ridership is fetched in two separate queries — weekday and weekend — then a `total` row is derived by summing both. The `day_group` column takes values: `total`, `weekday`, `weekend`.

## Script-Specific Filters

### `calculate_ridership_by_station.py` Only

- **Incomplete month check** — A month is skipped if not every calendar day in that month has at least one data row. This guards against partial-month data in the ongoing pipeline.

### `calculate_baseline.py` Only

- **January 2017 exclusion** — All January 2017 data is dropped because the first 12 days are missing. January baseline uses a 2-year average (2018-2019); all other months use 3 years.
- **Special cases** — Stations listed in `references/baseline_special_cases.csv` use a restricted set of baseline years (e.g., stations closed for renovation). The allowed years are intersected with \(\{2017, 2018, 2019\}\).
