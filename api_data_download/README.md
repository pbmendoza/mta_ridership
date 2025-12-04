# MTA Ridership Data Download

Download MTA ridership data from the [NY Open Data Portal](https://data.ny.gov/) via the Socrata SODA3 API.

## Usage

```bash
# Download full year (outputs 2025.csv)
python api_data_download/download.py --year 2025

# Download full month (outputs 202501.csv)
python api_data_download/download.py --year 2025 --month 1

# Download single day (outputs 20250115.csv)
python api_data_download/download.py --year 2025 --month 1 --day 15
```

## Arguments

| Argument | Required | Default | Description |
|----------|----------|---------|-------------|
| `--year` | Yes | 2025 | Year to download (4 digits) |
| `--month` | No | None | Month to download (1-12). If omitted, downloads full year. |
| `--day` | No | None | Day to download (1-31). Requires `--month`. |
| `--output` | No | Auto | Custom output CSV path. Defaults to `data/{year}{month}{day}.csv`. |
| `--page-size` | No | 50000 | Rows per API request (max ~50k). |
| `--app-token` | No | Built-in | Socrata app token. |
| `--secret-token` | No | Built-in | Socrata secret token. |

## Output Filenames

| Scope | Example Command | Output File |
|-------|-----------------|-------------|
| Full year | `--year 2025` | `data/2025.csv` |
| Full month | `--year 2025 --month 1` | `data/202501.csv` |
| Single day | `--year 2025 --month 1 --day 15` | `data/20250115.csv` |

## Environment Variables

Override default credentials via environment:

```bash
export SOCRATA_APP_TOKEN="your_app_token"
export SOCRATA_SECRET_TOKEN="your_secret_token"
python api_data_download/download.py --year 2025
```

## Data Source

- **Dataset ID**: `5wq4-mkjj`
- **Endpoint**: `https://data.ny.gov/resource/5wq4-mkjj.json`

## Output Columns

The CSV includes the following columns (in order):

- `transit_timestamp`
- `transit_mode`
- `station_complex_id`
- `station_complex`
- `borough`
- `payment_method`
- `fare_class_category`
- `ridership`
- `transfers`
- `latitude`
- `longitude`
- `georeference`
