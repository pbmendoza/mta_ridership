# MTA Ridership Pipeline

This repository powers OSDC's NYC Subway Recovery Tracker data processing.

The current production pipeline is API-first:
- Ridership is pulled directly from NY Open Data (Socrata API).
- Baseline is calculated from the MTA 2017-2019 hourly ridership API dataset.
- Final outputs are produced from API-derived intermediate files.

Local raw-file and turnstile workflows are still in the repo as legacy code and are not the active production path.

## Current Pipeline (Active)

### 1) Build station-level monthly ridership from API (2020+)

```bash
python scripts/api/calculate_ridership_by_station.py
```

Output:
- `data/api/ridership/monthly_ridership_station.csv`

Notes:
- Default mode is incremental (only fetches missing months).
- Supports targeted refresh (`--year`, `--month`) and full refresh (`--full-refresh`).
- Filters to subway only, excludes complex `502`, skips incomplete months, and outputs `total/weekday/weekend`.

### 2) Aggregate station ridership to PUMA and NYC

```bash
python scripts/api/aggregate_puma_nyc.py
```

Outputs:
- `data/api/ridership/monthly_ridership_puma.csv`
- `data/api/ridership/monthly_ridership_nyc.csv`

### 3) Calculate baseline from API ridership (2017-2019)

```bash
python scripts/api/calculate_baseline.py
```

Outputs:
- `data/api/baseline/monthly_baseline_station.csv`
- `data/api/baseline/monthly_baseline_puma.csv`
- `data/api/baseline/monthly_baseline_nyc.csv`

Baseline details:
- Source dataset: `t69i-h2me` (MTA Subway Hourly Ridership: 2017-2019).
- January 2017 is excluded (known incomplete month); January baseline uses 2018-2019.
- Special-case station year rules come from `references/baseline_special_cases.csv`.

### 4) Merge ridership + baseline

```bash
python scripts/calculate_final.py
```

Outputs:
- `data/api/processed/monthly_ridership_station.csv`
- `data/api/processed/monthly_ridership_puma.csv`
- `data/api/processed/monthly_ridership_nyc.csv`

### 5) Enrich with names and publish production files

```bash
python scripts/enrich_final_data.py
```

Outputs:
- `data/production/monthly_ridership_station.csv`
- `data/production/monthly_ridership_puma.csv`
- `data/production/monthly_ridership_nyc.csv`

## Quick Start

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Configure Socrata credentials (recommended to avoid rate limits):

```bash
cp .env.example .env
```

Then set:
- `SOCRATA_APP_TOKEN`
- `SOCRATA_SECRET_TOKEN` (optional)

3. Run active pipeline steps in order:

```bash
python scripts/api/calculate_ridership_by_station.py
python scripts/api/aggregate_puma_nyc.py
python scripts/api/calculate_baseline.py
python scripts/calculate_final.py
python scripts/enrich_final_data.py
```

## Data Model Summary

All three geographic levels (station, PUMA, NYC) use:
- `year`, `month`, `period`
- `day_group` in `total`, `weekday`, `weekend`
- `ridership`
- `omny_pct` (ridership outputs)
- `baseline`, `baseline_comparison` (processed outputs)

Station and PUMA production files are enriched with:
- `station_name` (station-level)
- `puma_name` (PUMA-level)

## Repository Layout

Active paths:
- `scripts/api/` - API data extraction and aggregation scripts
- `scripts/calculate_final.py` - API ridership + baseline merge
- `scripts/enrich_final_data.py` - final enrichment and sorting
- `data/api/` - API-derived intermediate outputs
- `data/production/` - final published outputs

Legacy paths (not active production workflow):
- `scripts/local/`
- `pipelines/calculate_ridership_local.py`
- `pipelines/calculate_baseline_local_turnstile.py`
- `data/local/`

## Reference Docs

- `references/docs/socrata_api_setup.md`
- `references/scripts/api_ridership_filters.md`
- `references/scripts/calculate_baseline.md`
- `references/docs/puma_mapping.md`

## Maintainers

Maintained by OSDC / NYS Office of the State Comptroller for the NYC Subway Recovery Tracker project.
