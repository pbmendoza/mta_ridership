# update_data.py

Download monthly MTA ridership CSVs from NY Open Data (Socrata SODA3), with completeness checks, row-count validation, optional duplicate diagnostics, and concurrent execution.

## Purpose

`scripts/update_data.py` downloads monthly ridership data into:

`data/raw/ridership/{year}/{month}.csv`

It supports:
- downloading one month, one year, or all configured years/months
- skipping future months
- skipping already-complete local files unless forced
- validating API row counts against written rows
- optional duplicate inspection without mutating files

## Data Source Configuration

Year-to-dataset mapping is loaded from:

`references/dataset_id_on_nyopendata.json`

Each year is mapped to a Socrata dataset ID, and the script builds:

`https://data.ny.gov/resource/{dataset_id}.json`

## Authentication

The script sends:
- `X-App-Token` (required by script logic)
- `X-App-Token-Secret` (optional, included when provided)

Credential resolution order:
1. CLI args (`--app-token`, `--secret-token`)
2. Environment variables (`SOCRATA_APP_TOKEN`, `SOCRATA_SECRET_TOKEN`)
3. Built-in defaults in the script

If app token resolves to an empty value, the script exits with code `1`.

## Dependencies

- Required: `requests`
- Optional: `rich` for multi-task progress UI

If `rich` is not installed, the script falls back to plain-text logging.

## CLI Usage

```bash
# All configured years, months 1-12 (future months are skipped)
python scripts/update_data.py

# All months in one year
python scripts/update_data.py --year 2025

# One specific month
python scripts/update_data.py --year 2025 --month 6

# Re-download even if local file exists and appears complete
python scripts/update_data.py --year 2025 --month 6 --force

# Duplicate diagnostics only (no file rewriting)
python scripts/update_data.py --year 2025 --month 6 --verify-duplicates

# Limit worker concurrency
python scripts/update_data.py --year 2025 --max-workers 2
```

## Arguments

| Argument | Type | Default | Behavior |
|---|---|---|---|
| `--year` | int | `None` | If set, process only this year. If omitted, process all years in dataset config. |
| `--month` | int | `None` | If set, process only this month. If omitted, process months `1..12`. |
| `--force` | flag | `False` | Re-download month even if local file exists and row count matches API count. |
| `--page-size` | int | `50000` | `$limit` for paginated SODA requests. |
| `--max-workers` | int (`>=1`) | `cpu_count - 1` (minimum `1`) | Max concurrent month tasks. Actual pool size is `min(max_workers, number_of_tasks)`. |
| `--verify-duplicates` | flag | `False` | After download, inspect for exact duplicate rows and log count + sample rows (up to 5). No data rewriting. |
| `--app-token` | str | env/default | Overrides app token source. |
| `--secret-token` | str | env/default | Overrides secret token source. |

## Output Files

Final files:
- `data/raw/ridership/{year}/{month}.csv`

Temporary/intermediate files:
- download temp: `{month}.csv.tmp` (same directory as final output)
- duplicate-check SQLite (only when `--verify-duplicates`): `{month}.csv.tmp.dupecheck.sqlite`

Temp files are cleaned up on completion/error paths handled by the script.

## Query and Pagination Behavior

Monthly query window:
- start: inclusive, `YYYY-MM-01T00:00:00`
- end: exclusive, first day of next month at `00:00:00`

Pagination params:
- `$where`: month window
- `$order`: `transit_timestamp ASC, station_complex_id ASC, payment_method ASC, fare_class_category ASC, :id ASC`
- `$limit`: `--page-size`
- `$offset`: cumulative by page length

The `:id` tie-breaker is used to keep ordering deterministic when business columns tie.

## Per-Month Processing Pipeline

For each `(year, month)` task:

1. Count rows for the month using `$select=count(1)`.
2. If count is `0`:
- remove local temp and final file (if present)
- mark task as incomplete (`no data`)
3. Check completeness by verifying there is data on the month’s last day (`00:00:00` to `23:59:59` inclusive).
4. If last-day check fails:
- remove local temp and final file (if present)
- mark task as incomplete
5. If final output exists and `--force` is not set:
- count local CSV data rows (header excluded)
- if local rows match API count, skip download
- otherwise log mismatch and re-download
6. Download rows into `.tmp` CSV via pagination.
7. Validate `written_rows == API_count`; if mismatch, fail task and remove temp.
8. If `--verify-duplicates` is enabled:
- scan temp CSV for exact duplicate rows using disk-backed SQLite
- log duplicate row count
- if duplicates exist, log up to 5 sample duplicate rows (JSON form)
- do not modify downloaded CSV contents
9. Atomically replace final output with temp file.

## CSV Writing Rules

- Header is built from the first API page:
- preferred column order first (`transit_timestamp`, `transit_mode`, `station_complex_id`, `station_complex`, `borough`, `payment_method`, `fare_class_category`, `ridership`, `transfers`, `latitude`, `longitude`, `georeference`)
- then any other non-metadata keys from that first page (keys not starting with `:`)
- Rows lacking `transit_timestamp` are skipped.
- Dict/list cell values are JSON-serialized with compact separators.

## Duplicate Verification (`--verify-duplicates`)

Implementation details:
- Uses SQLite table with `UNIQUE` across all CSV columns.
- Uses upsert to increment `seen_count`.
- Duplicate count is computed as `SUM(seen_count - 1)`.
- Sample duplicates are selected from rows with `seen_count > 1`, ordered by first-seen sequence, limited to 5.

Behavior:
- Diagnostic only.
- Does not rewrite or delete downloaded data due to duplicates.
- If duplicate scan row count does not match downloaded row count, task fails.

## Retry and Error Handling

For API requests:
- timeout: 60s
- max retries: 5
- retries on request exceptions and HTTP `429`
- exponential backoff: `min(2**attempt, 60)` seconds

Errors that fail a month task include:
- request failures after retries
- invalid/unexpected API payload shape
- row-count mismatch between API count and downloaded rows
- duplicate-scan count mismatch (when `--verify-duplicates` is enabled)

## Concurrency and UI

- Uses `ThreadPoolExecutor` for month-level parallelism.
- Each task uses its own `requests.Session`.
- With `rich`, shows live per-task progress and status.
- Without `rich`, prints plain logs.

Internal task statuses include:
- `queued`
- `counting rows`
- `checking completeness`
- `validating existing`
- `downloading`
- `verifying duplicates`
- `validating`
- terminal states such as `done`, `skipped`, `incomplete`, `error`, `empty`, `no data`

## Summary and Exit Code

After processing, the script prints summary counts:
- Downloaded
- Skipped
- Incomplete
- Errors

Exit code:
- `0` when `Errors == 0`
- `1` when `Errors > 0`

## Current Behavioral Notes

- `--month` is described as `1-12`, but validation is not explicitly enforced at argparse level; invalid values fail later when date logic runs.
- Header discovery is based on the first API page. Columns that appear only in later pages are not added to the CSV header.
- Future months are skipped before task creation and counted as `Skipped`.

## Related Files

- Script: `scripts/update_data.py`
- Dataset mapping: `references/dataset_id_on_nyopendata.json`
- Similar downloader: `api_data_download/download.py`
- Pipeline docs: `references/docs/PIPELINE.md`
