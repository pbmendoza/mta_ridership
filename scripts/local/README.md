# Local Pipeline Scripts

Scripts in this directory implement the data processing pipeline that operates on **local data files** (raw CSVs stored in `data/`), as opposed to calculating results directly from the API.

These scripts are orchestrated by pipeline runners in [`pipelines/`](../../pipelines/), primarily:

- [`calculate_ridership_local.py`](../../pipelines/calculate_ridership_local.py) for modern local ridership generation
- [`run_pipeline.py`](../../run_pipeline.py) for final merge and optional enrichment

> **Note:** `enrich_final_data.py` lives in `scripts/` (one level up) since it is data-source-agnostic — it operates on `results/` and reference data regardless of whether the upstream data came from local files or the API.
