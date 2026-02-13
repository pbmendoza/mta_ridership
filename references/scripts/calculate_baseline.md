# calculate_baseline.py

Compute monthly baseline ridership (2017-2019) from the MTA Subway Hourly Ridership API, with weekday/weekend/total breakdown and aggregation to PUMA and NYC levels.

## Purpose

`scripts/api/calculate_baseline.py` fetches 2017-2019 subway ridership from the Socrata API, averages it by station/month/day_group, and writes baseline CSVs for comparison against ongoing ridership data.

It produces:
- `results/baseline_api/monthly_baseline_station.csv`
- `results/baseline_api/monthly_baseline_puma.csv`
- `results/baseline_api/monthly_baseline_nyc.csv`

## Data Source

- **Dataset**: MTA Subway Hourly Ridership: 2017-2019
- **Dataset ID**: `t69i-h2me`
- **Endpoint**: `https://data.ny.gov/resource/t69i-h2me.json`

This is a single dataset covering all three years, separate from the 2020+ datasets in `references/dataset_id_on_nyopendata.json`.

## Authentication

Same credential resolution as other Socrata scripts:
1. Environment variables (`SOCRATA_APP_TOKEN`, `SOCRATA_SECRET_TOKEN`)
2. `.env` file at repository root

If no app token is found, the script continues with anonymous access (lower rate limits).

## Dependencies

- `pandas`
- `requests`

## CLI Usage

```bash
python scripts/api/calculate_baseline.py
```

No arguments. The script runs as a single end-to-end job (~5 minutes due to server-side aggregation latency).

## Output Files

| File | Columns | Rows |
|---|---|---|
| `monthly_baseline_station.csv` | `complex_id, month, day_group, ridership` | 15,264 (424 stations × 12 months × 3 day_groups) |
| `monthly_baseline_puma.csv` | `puma, month, day_group, ridership` | 1,764 (49 PUMAs × 12 months × 3 day_groups) |
| `monthly_baseline_nyc.csv` | `month, day_group, ridership` | 36 (12 months × 3 day_groups) |

`day_group` values: `total`, `weekday`, `weekend`.

Ridership values are rounded to 2 decimal places.

## Query Strategy

The script issues **6 SoQL queries** (3 years × 2 day_groups) with server-side aggregation:

```
$select:  station_complex_id, date_extract_m(transit_timestamp) as month, sum(ridership) as ridership
$where:   transit_mode = 'subway' AND station_complex_id != '502'
          AND transit_timestamp >= '{year}-01-01T00:00:00'
          AND transit_timestamp < '{year+1}-01-01T00:00:00'
          AND date_extract_dow(transit_timestamp) [NOT] IN (0, 6)
$group:   station_complex_id, month
```

Each query returns ~5,000 rows (one per station per month). Pagination is included as a safety measure (`$limit=50000`).

Timeout per query: **180 seconds** (server-side aggregation over hourly data is slow).

## Filters

See `references/scripts/api_ridership_filters.md` for full filter documentation shared with `calculate_ridership_by_station.py`.

Summary:
- `transit_mode = 'subway'`
- `station_complex_id != '502'`
- SIR stations excluded during grid-fill
- Weekday/weekend split via `date_extract_dow`

## Processing Pipeline

1. **Fetch** — 6 API queries (weekday + weekend for each of 2017, 2018, 2019). Results are combined into a single DataFrame with columns: `station_complex_id, year, month, ridership, day_group`.

2. **Drop January 2017** — All rows where `year=2017` and `month=1` are removed (first 12 days of data are missing).

3. **Load special cases** — `references/baseline_special_cases.csv` defines per-station allowed baseline years. Each station's allowed years are intersected with `{2017, 2018, 2019}`.

4. **Compute averages** — Two paths:
   - **Regular stations**: Sum ridership across years, divide by year count (2 for January, 3 for other months).
   - **Special-case stations**: For each station/month, determine valid years (allowed years minus `{2017}` for January). Sum ridership over valid years and divide by count. If no valid years remain (e.g., station 6 in January), that station-month is left unfilled (becomes 0 after grid-fill).

5. **Derive total** — For each station/month, sum weekday + weekend ridership to produce the `total` row.

6. **Grid-fill** — Build a complete grid of all non-SIR stations × 12 months × 3 day_groups using `references/stations/stations_complexes_official.csv`. Left-join with computed baseline; fill missing with 0.

7. **Sort and save** — Sort by month, complex_id (numeric), day_group. Round ridership to 2 decimal places. Save station-level CSV.

8. **Aggregate to PUMA** — Join station baseline with `references/stations/station_to_puma.csv`, group by PUMA/month/day_group, sum ridership.

9. **Aggregate to NYC** — Group station baseline by month/day_group, sum ridership.

## Retry and Error Handling

API requests use the shared `request_json` from `scripts/utils/socrata.py`:
- timeout: 180s (overridden from default 60s)
- max retries: 5
- exponential backoff on request exceptions and HTTP 429

## Related Files

- Script: `scripts/api/calculate_baseline.py`
- Shared filters doc: `references/scripts/api_ridership_filters.md`
- Special cases: `references/baseline_special_cases.csv`
- Station reference: `references/stations/stations_complexes_official.csv`
- Station-to-PUMA mapping: `references/stations/station_to_puma.csv`
- Socrata utilities: `scripts/utils/socrata.py`
