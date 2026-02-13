# MTA Ridership Data Processing Pipeline

Last updated: 2026-02-12

## Overview

The local processing workflow is split into three pipeline runners:

1. `pipelines/calculate_baseline_local_turnstile.py` generates baseline files from historical turnstile data (2014–2023 source, 2015–2019 baseline period).
2. `pipelines/calculate_ridership_local.py` processes modern local ridership data (2020–present) into monthly ridership outputs.
3. `run_pipeline.py` performs final merge/enrichment using those precomputed baseline and ridership outputs.

## Quick Start

```bash
# Generate baseline files (only needed once, or to regenerate)
python pipelines/calculate_baseline_local_turnstile.py

# Generate modern local ridership outputs
python pipelines/calculate_ridership_local.py

# Final merge + enrichment
python run_pipeline.py
```

`run_pipeline.py` requires existing baseline files in `results/baseline/`
and existing local ridership files in `results/ridership_local/`.

## Pipeline Architecture

### Data Flow Diagram

```
┌─────────────────────┐     ┌────────────────────┐
│ Raw Turnstile Files │     │ Raw Ridership Files│
│    (Historical)     │     │     (Modern)       │
└──────────┬──────────┘     └─────────┬──────────┘
           │                           │
           ▼                           ▼
    ┌─────────────┐             ┌─────────────┐
    │   STAGING   │             │   STAGING   │
    │ (Combined)  │             │ (Filtered)  │
    └──────┬──────┘             └──────┬──────┘
           │                           │
           ▼                           ▼
    ┌─────────────┐             ┌─────────────┐
    │ PROCESSING  │             │ PROCESSING  │
    │ (Aggregated)│             │ (Aggregated)│
    └──────┬──────┘             └──────┬──────┘
           │                           │
           ▼                           │
    ┌─────────────┐                    │
    │  BASELINE   │                    │
    │ (2015-2019) │                    │
    └──────┬──────┘                    │
           │                           │
           └─────────┬─────────────────┘
                     ▼
              ┌─────────────┐
              │   FINAL     │
              │  ANALYSIS   │
              └─────────────┘
```

## Pipeline Steps

### Modern Local Ridership Pipeline

**Runner**: `pipelines/calculate_ridership_local.py`

This runner executes the modern local branch end-to-end:
- Stages each raw ridership file from `data/local/raw/ridership/`
- Processes staged files into daily aggregates
- Runs `scripts/local/calculate_ridership.py` to generate monthly metrics
- Writes outputs to `results/ridership_local/`

### Final Merge Pipeline

**Runner**: `run_pipeline.py`

This runner requires precomputed inputs and then:
1. Validates baseline files in `results/baseline/`
2. Validates modern local ridership files in `results/ridership_local/`
3. Runs `scripts/local/calculate_final.py` for baseline comparisons
4. Optionally runs `scripts/enrich_final_data.py` (skip via `--skip-enrich`)

**Important Note on Baseline Metrics**:
- The baseline data includes both `entries` and `exits` from 2015–2019
- Final outputs use baseline **entries only** as `baseline_ridership`
- Rationale: ensures consistency with modern ridership (combined entry counts across all payment methods)
- Formula used in final outputs: `baseline_comparison = ridership / baseline_ridership`

## Performance Optimizations

### Turnstile Data Caching

The historical turnstile pipeline (`pipelines/calculate_baseline_local_turnstile.py`) caches the combined turnstile file:

- **First run**: Takes 15-20 minutes to combine 488 files
- **Subsequent runs**: Skips turnstile staging, saves ~10 minutes
- **File size**: ~8.8GB combined CSV

### When to Regenerate Turnstile Data

Regenerate the combined turnstile file only when:

1. **New historical data is added** (unlikely, as data ends in May 2023)
2. **Data corruption is suspected**
3. **Processing logic changes** in `stage_turnstile_data.py`

To regenerate:

```bash
# Force regeneration of turnstile_combined.csv and recalculate baseline
python pipelines/calculate_baseline_local_turnstile.py --force-stage
```

### Memory Management

The updated `stage_turnstile_data.py` processes files in batches:
- Batch size: 50 files
- Memory usage: ~3-4GB peak
- Progress indicators: Shows batch progress and ETA

## Output Files

### Final Results (`results/final/`)

1. **monthly_ridership_station.csv**: Station-level metrics
2. **monthly_ridership_puma.csv**: Neighborhood-level aggregations  
3. **monthly_ridership_nyc.csv**: City-wide totals

#### Final Results Schemas
- `results/final/monthly_ridership_station.csv`:
  - Columns: `complex_id`, `station_name`, `year`, `month`, `period`, `ridership`, `baseline_ridership`, `baseline_comparison`, `omny_pct`
- `results/final/monthly_ridership_puma.csv`:
  - Columns: `puma`, `puma_name`, `year`, `month`, `period`, `ridership`, `baseline_ridership`, `baseline_comparison`, `omny_pct`
- `results/final/monthly_ridership_nyc.csv`:
  - Columns: `year`, `month`, `period`, `ridership`, `baseline_ridership`, `baseline_comparison`, `omny_pct`

### Intermediate Files

- `data/local/staging/`: Combined raw data files
- `data/local/processed/`: Cleaned and aggregated data
- `results/baseline/`: Historical baseline metrics
- `results/ridership_local/`: Modern local ridership metrics

#### Baseline Results Schemas (`results/baseline/`)
- `monthly_baseline_station.csv`: `complex_id`, `month`, `entries`, `exits`
- `monthly_baseline_puma.csv`: `puma`, `month`, `entries`, `exits`
- `monthly_baseline_nyc.csv`: `nyc`, `month`, `entries`, `exits`

#### Modern Ridership Schemas (`results/ridership_local/`)
- `monthly_ridership_station.csv`: `complex_id`, `year`, `month`, `period`, `day_group`, `ridership`, `omny_pct`
- `monthly_ridership_puma.csv`: `puma`, `year`, `month`, `period`, `day_group`, `ridership`, `omny_pct`
- `monthly_ridership_nyc.csv`: `year`, `month`, `period`, `day_group`, `ridership`, `omny_pct`
  - `day_group` values: `total`, `weekday`, `weekend`

## Execution Time

Typical pipeline execution times:

| Run Mode | Typical Time |
|------|------------------|
| Modern local ridership (`python pipelines/calculate_ridership_local.py`) | 10-15 min |
| Final merge pipeline (`python run_pipeline.py`) | 1-3 min |
| Historical pipeline (`python pipelines/calculate_baseline_local_turnstile.py`) | 15-25 min |

Notes:
- Times and sizes are approximate and depend on hardware and storage (e.g., ~16GB RAM Mac, SSD). Capture date: 2026-02-11.

## Troubleshooting

### Out of Memory Errors

If turnstile staging fails with memory errors:

1. Check available memory: `vm_stat` (macOS) or `free -h` (Linux)
2. Close other applications
3. Reduce batch size in `stage_turnstile_data.py` (default: 50)

### File Not Found Errors

Ensure all directories exist:

```bash
mkdir -p data/local/{raw,staging,processed,quarantine}/{turnstile,ridership}
mkdir -p results/{baseline,ridership_local,final}
mkdir -p logs
```

### OneDrive Sync Issues

If using OneDrive, large file operations may cause sync delays:

1. Pause OneDrive sync during processing
2. Resume after pipeline completion
3. Consider moving data directory outside OneDrive

### Baseline Special Cases Verification
- Use `scripts/verify_baseline_special_cases.py` to validate entries in `references/baseline_special_cases.csv` against official station references.
- Check verification and calculation logs under `logs/` (e.g., `baseline_special_cases_verification.txt`, `calculate_baseline.log`).

## Maintenance

### Adding New Data

1. **New turnstile files**: Place in `data/local/raw/turnstile/`
2. **New ridership files**: Place in `data/local/raw/ridership/`
3. Run `python pipelines/calculate_ridership_local.py`
4. Run `python run_pipeline.py` (run `python pipelines/calculate_baseline_local_turnstile.py` first if baseline needs regeneration)

### Updating Processing Logic

When modifying processing scripts:

1. Test changes on a subset of data first
2. Clear relevant intermediate files
3. Document any new dependencies
4. Update this documentation if needed

## See Also

- `CLAUDE.md`: AI assistant instructions
- `README.md`: Project overview
- Individual script docstrings: Detailed processing logic
