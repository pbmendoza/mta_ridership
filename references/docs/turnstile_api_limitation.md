# Why Turnstile Data Cannot Be Processed Via API

The modern ridership data (2020+) is successfully queried via the NY Open Data SODA API in `scripts/api/calculate_ridership_by_station.py`. This document explains why the same approach **cannot** be applied to the historical turnstile data used for baseline calculation.

## Data Format Difference

| Aspect | Modern Ridership (API-friendly) | Historical Turnstile (local-only) |
|---|---|---|
| **Values** | Pre-aggregated `ridership` per station per day | Raw **cumulative counter** readings per turnstile |
| **Key operation** | `SUM(ridership) GROUP BY station` | `ENTRIES[t] - ENTRIES[t-1]` per turnstile (sequential diff) |
| **Granularity** | Station complex level | Individual turnstile (C/A + UNIT + SCP) |
| **Datasets** | `wujg-7c2s`, `5wq4-mkjj` | `ug6q-shqc`, `ekwu-khcy`, `v5y5-mwpb`, etc. |
| **Reference file** | `references/dataset_id_on_nyopendata.json` | `references/turnstile_data_nyopendata.json` |

## Technical Blockers

### 1. Sequential Diffs Require Window Functions
Turnstile counters are cumulative — ridership is computed as the difference between consecutive readings for each turnstile. This requires `LAG()` / `LEAD()` window functions, which the **SODA API does not support**. The API only offers `$select`, `$where`, `$group`, `$order`, and basic aggregation functions (`SUM`, `COUNT`, etc.).

### 2. Outlier Turnstile Removal
The pipeline removes turnstiles whose record count falls below 10% of the modal count across all turnstiles (`process_turnstile_data.py`). This statistical filtering requires a global view of all turnstile record counts, which cannot be expressed in a single SODA query.

### 3. Value Clipping
After computing diffs, unrealistic values are clipped: negative diffs (counter resets) are set to zero, and values exceeding 7,200 per 4-hour period are capped. This row-level conditional logic depends on the diff calculation from blocker #1.

### 4. Midnight-Spanning Period Attribution
The 20:00–00:00 period crosses midnight. The pipeline attributes this ridership to the **start date** (the 20:00 reading's date), not the end date. This requires access to the previous row's timestamp — again needing window functions.

### 5. Station-to-Complex Mapping
Turnstile data uses `STATION + LINENAME` identifiers, which must be mapped to `Complex ID` via a local reference file (`references/stations/stations_turnstile_mapping.csv`). The modern ridership data already includes `station_complex_id`.

### 6. Special-Case Baseline Years
The baseline calculation uses station-specific year ranges for ~28 stations that were closed during parts of 2015–2019 (configured in `references/baseline_special_cases.csv`). This per-station year filtering adds another layer of logic that would be cumbersome to replicate via API.

## Practical Note

The baseline covers **2015–2019 only** and is static. The results are cached in `data/local/baseline_turnstile/`. To regenerate baseline files, run `pipelines/calculate_baseline_local_turnstile.py`. Modern local ridership outputs are generated separately via `pipelines/calculate_ridership_local.py`. Even if an API approach were feasible, the benefit would be minimal since the baseline is computed once and never changes.
