# MTA Ridership Data Processing Pipeline

Last updated: 2026-02-11

## Overview

The MTA Ridership Data Processing Pipeline (`run_pipeline.py`) is an automated workflow that processes NYC subway ridership data from two different eras:

1. **Historical Data (2010–May 2023)**: Individual turnstile swipe counts (raw files available 2010–May 2023; processed/staged from 2014-10-18 modern format)
2. **Modern Data (2020–present)**: Payment method data including OMNY adoption (months with incomplete days are filtered out)

The pipeline combines these datasets to create comprehensive ridership metrics at multiple geographic levels with baseline comparisons.

By default, the runner executes the modern branch only and expects existing baseline files.
The historical turnstile + baseline branch is opt-in via `--include-historical`.

## Quick Start

```bash
# Default mode: modern branch + final merge + enrichment
python run_pipeline.py

# Include historical turnstile processing and baseline regeneration
python run_pipeline.py --include-historical
```

Default mode requires existing baseline files in `results/baseline/`.
If those files are missing, rerun with `python run_pipeline.py --include-historical`.

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

### Step 1: Historical Branch (Optional)

**Enabled by**: `--include-historical`

**Scripts**: `stage_turnstile_data.py`, `process_turnstile_data.py`, `calculate_baseline.py`

- Stages historical turnstile files (cached `turnstile_combined.csv` is reused unless forced)
- Processes turnstile data into daily complex-level metrics
- Calculates monthly baseline files for 2015-2019

### Step 2: Modern Ridership Calculation (Default Path)

**Script**: `calculate_ridership.py`

- Processes 2020-present data
- Tracks payment method distribution (MetroCard vs OMNY)
- Aggregates to station, PUMA, and city-wide levels
- Filters out partial months to ensure complete monthly totals

### Step 3: Final Analysis

**Script**: `calculate_final.py`

- Merges modern ridership with baseline
- Calculates percentage changes
- Generates comparison metrics

**Important Note on Baseline Metrics**:
- The baseline data includes both `entries` and `exits` from 2015–2019
- Final outputs use baseline **entries only** as `baseline_ridership`
- Rationale: ensures consistency with modern ridership (combined entry counts across all payment methods)
- Formula used in final outputs: `baseline_comparison = ridership / baseline_ridership`

### Step 4: Data Enrichment

**Script**: `enrich_final_data.py`

- Adds human-readable station names
- Adds PUMA neighborhood names
- Sorts output for easy analysis

## Performance Optimizations

### Turnstile Data Caching

Historical turnstile data (2014–2023 modern-format subset) never changes, so the historical branch caches the combined file:

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
# Remove the cached file
rm data/staging/turnstile/turnstile_combined.csv

# Option 1: Run with historical branch enabled (will recreate it)
python run_pipeline.py --include-historical

# Option 2: Run just the staging script
python scripts/stage_turnstile_data.py
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

- `data/staging/`: Combined raw data files
- `data/processed/`: Cleaned and aggregated data
- `results/baseline/`: Historical baseline metrics
- `results/ridership/`: Modern ridership metrics

#### Baseline Results Schemas (`results/baseline/`)
- `monthly_baseline_station.csv`: `complex_id`, `month`, `entries`, `exits`
- `monthly_baseline_puma.csv`: `puma`, `month`, `entries`, `exits`
- `monthly_baseline_nyc.csv`: `nyc`, `month`, `entries`, `exits`

#### Modern Ridership Schemas (`results/ridership/`)
- `monthly_ridership_station.csv`: `complex_id`, `year`, `month`, `period`, `day_group`, `ridership`, `omny_pct`
- `monthly_ridership_puma.csv`: `puma`, `year`, `month`, `period`, `day_group`, `ridership`, `omny_pct`
- `monthly_ridership_nyc.csv`: `year`, `month`, `period`, `day_group`, `ridership`, `omny_pct`
  - `day_group` values: `total`, `weekday`, `weekend`

## Execution Time

Typical pipeline execution times:

| Run Mode | Typical Time |
|------|-----------------|
| Default (`python run_pipeline.py`) | 10-15 min |
| With historical (`--include-historical`) | 25-35 min |

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
mkdir -p data/{raw,staging,processed,quarantine}/{turnstile,ridership}
mkdir -p results/{baseline,ridership,final}
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

1. **New turnstile files**: Place in `data/raw/turnstile/`
2. **New ridership files**: Place in `data/raw/ridership/`
3. Run `python run_pipeline.py` (or include `--include-historical` when baseline needs regeneration)

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
