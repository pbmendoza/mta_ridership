# Local Pipeline Scripts

Scripts in this directory implement the data processing pipeline that operates on **local data files** (raw CSVs stored in `data/`), as opposed to calculating results directly from the API.

These scripts are orchestrated by [`run_pipeline.py`](../../run_pipeline.py) at the project root.

> **Note:** `calculate_baseline.py` and `enrich_final_data.py` have been moved to `scripts/` (one level up) since they are data-source-agnostic — they operate on `results/` and reference data regardless of whether the upstream data came from local files or the API.
