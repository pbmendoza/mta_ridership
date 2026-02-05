# update_data.py

Download MTA ridership data by month via the Socrata SODA3 API.

## Overview

This script downloads monthly ridership records from the NY Open Data portal, organizing files by year and month. It automatically uses the correct dataset ID for each year based on the configuration in `references/dataset_id_on_nyopendata.json`.

## Usage

```bash
# Download all years and months (1-12) defined in config
python scripts/update_data.py

# Download all months for a specific year
python scripts/update_data.py --year 2025

# Download a specific month
python scripts/update_data.py --year 2025 --month 6

# Force overwrite existing files
python scripts/update_data.py --force

# Combine options
python scripts/update_data.py --year 2025 --month 6 --force
```

## Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--year` | int | None | Year to download (e.g., 2025). If omitted, downloads all years in config. |
| `--month` | int | None | Month to download (1-12). If omitted, downloads months 1-12. |
| `--force` | flag | False | Overwrite existing files instead of skipping. |
| `--page-size` | int | 50000 | Rows per API request (max ~50k). |
| `--app-token` | str | env/default | Socrata app token. |
| `--secret-token` | str | env/default | Socrata secret token. |

## Environment Variables

| Variable | Description |
|----------|-------------|
| `SOCRATA_APP_TOKEN` | App token for Socrata API authentication |
| `SOCRATA_SECRET_TOKEN` | Secret token for Socrata API authentication |

## Output Structure

Files are saved to:
```
data/raw/ridership/{year}/{month}.csv
```

Example:
```
data/raw/ridership/
├── 2020/
│   ├── 1.csv
│   ├── 2.csv
│   └── ...
├── 2025/
│   ├── 1.csv
│   ├── 2.csv
│   └── ...
```

## How It Works

### 1. Configuration Loading
The script reads `references/dataset_id_on_nyopendata.json` to determine which dataset ID to use for each year. Different years may use different datasets:
- 2020-2024: Dataset `wujg-7c2s` (historical combined dataset)
- 2025+: Dataset `5wq4-mkjj` (current year dataset)

### 2. File Caching
Before downloading each month, the script checks if the output file already exists:
- **Exists**: Skip (unless `--force` is specified)
- **Does not exist**: Proceed with download

### 3. Future Month Filtering
Months beyond the current date are automatically skipped. For example, if today is February 5, 2026, months March 2026 onwards will be skipped.

### 4. Completeness Validation
After downloading a month's data, the script checks if the data is complete by querying for records on the **last day of the month**:
- **Has data for last day**: Data is complete, file is kept
- **No data for last day**: Data is incomplete, file is deleted

This prevents saving partial data for months that haven't fully concluded or where data hasn't been published yet.

### 5. API Pagination
The SODA3 API returns a maximum of ~50,000 rows per request. The script automatically paginates through results until all data is retrieved.

### 6. Retry Logic
API requests include automatic retry with exponential backoff:
- Max retries: 5
- Handles HTTP 429 (rate limit) responses
- Timeout: 60 seconds per request

## Status Indicators

| Emoji | Meaning |
|-------|---------|
| 🔄 | Currently downloading |
| ⏭️ | Skipped (exists or future month) |
| ✅ | Successfully downloaded and complete |
| ⚠️ | Incomplete data (removed) |
| ❌ | Error occurred |

## Example Output

```
🔄 2025/01: Downloading...
   Progress: 1245678/1245678 (100.0%)
   ✅ Saved 1245678 rows to data/raw/ridership/2025/1.csv
⏭️  2025/02: File exists, skipping.
🔄 2025/03: Downloading...
   Progress: 892341/892341 (100.0%)
   ⚠️  Incomplete data (no data for last day), removing file.
⏭️  2026/03: Future month, skipping.

📊 Summary:
   Downloaded: 1
   Skipped:    2
   Incomplete: 1
   Errors:     0
```

## Related Files

- Configuration: `references/dataset_id_on_nyopendata.json`
- Similar script: `api_data_download/download.py` (supports daily/annual downloads)
- Pipeline docs: `references/docs/PIPELINE.md`
