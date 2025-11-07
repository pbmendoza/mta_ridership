# 🚇 MTA Ridership Analysis Pipeline

## 📋 Overview

This repository contains the data processing pipeline for **OSDC's NYC Subway Recovery Tracker** project ([https://www.osc.ny.gov/osdc/subway-recovery-tracker](https://www.osc.ny.gov/osdc/subway-recovery-tracker)). The pipeline processes NYC subway ridership data from two primary sources to track post-pandemic recovery patterns:

- **Historical turnstile data (2014–2023; baseline uses 2015–2019)**: Individual turnstile swipe counts used to establish the pre-pandemic baseline
- **Modern ridership data (2020–present)**: Station-level ridership including OMNY adoption metrics

The pipeline produces monthly ridership metrics at three geographic levels:
- **Station Complex**: Individual subway station complexes
- **PUMA**: Public Use Microdata Areas (neighborhood-level)
- **NYC**: City-wide aggregation

## 📚 Documentation

### 📖 Available Documentation

For detailed information about specific aspects of the project, please refer to these documentation files:

#### 1. [**Subway Station Mapping**](references/docs/subway_stations.md) 🏷️
Explains the complex challenge of mapping inconsistent station names across different data sources. Details the manual crosswalk created to link historical turnstile data with modern station complex IDs. Essential for understanding how we ensure data consistency across time periods.

#### 2. [**Turnstile Data Documentation**](references/docs/turnstile_data.md) 🎫
Comprehensive guide to the historical turnstile dataset (2010-2023). Covers the evolution from legacy to modern formats, data quality issues including the infamous Orchard Beach mystery, and validation results showing <1% difference from official MTA statistics.

#### 3. [**PUMA Mapping Documentation**](references/docs/puma_mapping.md) 🗺️
Details how we use geospatial analysis to assign each subway station to its corresponding Public Use Microdata Area (PUMA). Includes the algorithm, data sources, and edge cases for neighborhood-level ridership analysis.

#### 4. [**SCP Identifier Documentation**](references/docs/scp_identifier.md) 🔐
Technical explanation of the Subunit Channel Position (SCP) identifier system used in turnstile data. Understanding SCPs is crucial for correctly identifying individual turnstiles and avoiding data duplication.

#### 5. [**Data Processing Filters**](references/docs/data_filters.md) 🔍
Complete list of all filters, thresholds, and data quality controls applied during processing. Includes outlier detection, counter reset handling, and station exclusions with detailed rationales for each decision.

## 📊 Data Flow Architecture

```
Raw Turnstile Files → stage → process → calculate_baseline ↘
                                                            calculate_final → enrich_final → Results
Raw Ridership Files → stage → process → calculate_ridership ↗
```

## 📁 Project Structure

```
mta_ridership/
├── data/
│   ├── raw/                # Original data files
│   ├── staging/            # Intermediate processing
│   ├── processed/          # Clean, aggregated data
│   └── external/           # Reference data (stations, PUMA boundaries)
├── results/
│   ├── baseline/           # 2015-2019 monthly averages
│   ├── ridership/          # Current ridership metrics
│   └── final/              # Final analysis with comparisons
├── references/
│   ├── docs/               # Detailed documentation
│   └── stations/           # Station metadata and mappings
├── scripts/                # Processing scripts
└── logs/                   # Execution logs
```

## 🔧 Key Features

### Data Quality Controls
- **Outlier Detection**: Removes turnstiles with abnormal record counts
- **Threshold Filtering**: Caps unrealistic swipe counts (max 7,200 per 4-hour period)
- **Counter Reset Handling**: Manages when cumulative counters reset to zero
- **Station Mapping**: Links historical names to official complex IDs

### Processing Capabilities
- Processes modern-format turnstile files (Oct 2014+)
- Tracks OMNY adoption alongside traditional MetroCard usage
- Aggregates data to multiple geographic levels
- Calculates baseline comparisons for recovery tracking
- Enriches final output with human-readable PUMA and station names

## 📈 Output Files

### Station-Level Metrics
- `results/final/monthly_ridership_station.csv`: Monthly ridership by station complex

### Neighborhood-Level Metrics
- `results/final/monthly_ridership_puma.csv`: Monthly ridership by PUMA

### City-Wide Metrics
- `results/final/monthly_ridership_nyc.csv`: NYC total monthly ridership

Each file includes:
- Total ridership counts
- OMNY adoption percentages (post-2020)
- Comparison to 2015-2019 baseline (uses entries only, not exits)
- Period key (`period`, e.g., `YYYY-MM-01`)
- Human-readable station and PUMA names (enriched output)

**Note on Baseline Comparisons**: The `baseline_ridership` field represents average monthly **entries** from 2015-2019. While both entries and exits are calculated during baseline processing, only entries are used for comparison metrics. This provides a consistent measure for tracking subway usage recovery.

## 🛠️ Development

### Project Conventions
- All scripts use relative paths based on project root
- Comprehensive logging to `logs/` directory
- Automatic project root detection via `.git` directory

### Adding New Data
1. Place raw files in appropriate `data/raw/` subdirectory
2. Update staging scripts if format differs
3. Run full pipeline to regenerate results, e.g.:

```bash
bash run.sh
```

## 📊 Data Sources

- **Turnstile Data**: Historical MTA turnstile records (retired dataset)
- **Modern Ridership**: MTA ridership API data
- **Station Mapping**: MTA official station complex definitions
- **PUMA Boundaries**: 2020 Census geographic boundaries (updated 2025)

## 🤝 Contributing

This project is maintained by the Office of the [Office of the State Deputy Comptroller for NYC](https://www.osc.ny.gov/osdc) and [Better Data Initiative](https://bdin.org). For questions or contributions related to the NYC Subway Recovery Tracker, please refer to the official project page.

## 📝 License

This project is part of the public data analysis efforts by the New York State Comptroller's Office.