# MTA Ridership Pipeline

This repository powers OSDC's NYC Subway Recovery Tracker data processing.

The current production pipeline is API-first:
- Ridership is pulled directly from NY Open Data (Socrata API).
- Baseline is calculated from the MTA 2017-2019 hourly ridership API dataset.
- Final outputs are produced from API-derived intermediate files.

Local raw-file and turnstile workflows are still in the repo as legacy code and are not the active production path.

## Recommended Usage

Use the bootstrap runner from the project root:

```bash
python run_pipeline.py
```

If `python` is not recognized on Windows, use:

```powershell
py run_pipeline.py
```

The bootstrap runner is the supported workflow for first-time and repeat users. It:
- creates `.venv` automatically when needed
- installs or refreshes project dependencies when needed
- runs the pipeline inside the repo virtual environment
- reuses existing baseline files unless they are missing or you ask to rebuild them

Instructions:
- [`docs/run-pipeline.md`](docs/run-pipeline.md)

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

1. Install Python 3.10 or newer.

```bash
python --version
```

```powershell
py --version
```

2. Configure Socrata credentials if you want higher API rate limits.

```bash
cp .env.example .env
```

```powershell
Copy-Item .env.example .env
```

Then set:
- `SOCRATA_APP_TOKEN`
- `SOCRATA_SECRET_TOKEN` (optional)

3. Run the supported pipeline command from the project root:

```bash
python run_pipeline.py
```

```powershell
py run_pipeline.py
```

If your Windows console is still using a legacy code page, emoji in log output may render as replacement characters. That is acceptable. The pipeline should not emit `--- Logging error ---` tracebacks.

4. Common options:

```bash
python run_pipeline.py --year 2025 --month 2
python run_pipeline.py --full-refresh
python run_pipeline.py --rebuild-baseline
```

5. Advanced users can still run the active steps individually:

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
- `run_pipeline.py` - supported bootstrap runner for first-time and repeat use
- `docs/` - end-user instructions for first-time and repeat runs
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
