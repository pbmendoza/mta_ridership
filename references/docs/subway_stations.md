# NYC Subway Station Mapping Documentation

Last updated: 2025-11-07

## Overview

This document explains the complex challenge of mapping NYC subway station names across different MTA data sources and time periods. Station naming conventions have evolved significantly, creating a critical need for standardized mapping to enable accurate ridership analysis.

## 🎯 The Challenge

### Why Station Mapping Matters

The MTA provides ridership data from two distinct systems:
- **📊 Turnstile Data (2010-2020)**: Legacy system with inconsistent station naming
- **📱 Modern Ridership Data (2020+)**: Standardized system using official station complex IDs

To compare pre-pandemic baseline ridership (from turnstile data) with post-pandemic patterns (from modern data), we need accurate station mapping across both systems.

### Key Issues

1. **🏷️ Naming Inconsistencies**: The same station appears with different names across time periods
2. **🔀 Station Complexes**: Multiple connected stations are grouped differently in various datasets
3. **🚊 Line Designations**: Service patterns recorded inconsistently (e.g., "456" vs "4-5-6" vs "456S")

## 🗂️ Data Sources and Files

### 📁 Generated Mapping Files

#### 🔗 Station-to-Complex Crosswalk (CRITICAL FILE)
**File**: [`references/stations/stations_turnstile_mapping.csv`](../stations/stations_turnstile_mapping.csv)

This manually-created crosswalk is the cornerstone of our analysis. It maps every unique station name + line combination from turnstile data to official station complex IDs.

**Key Features**:
- Maps historical turnstile station names → official complex IDs
- Handles all naming variations across 2010-2020
- Manually verified for accuracy
- Enables seamless data integration

**Column Definitions**:

| Column | Purpose | Source |
|--------|---------|--------|
| `STATION` | Station name from turnstile data | Turnstile files |
| `LINENAME` | Line services from turnstile data | Turnstile files |
| `station_id` | Unique identifier for each STATION+LINENAME combination | Generated |
| `Complex ID` | Official MTA complex identifier (target mapping) | MTA official data |
| `Complex Name` | Human-readable complex name for manual verification | MTA official data |
| `Daytime Routes` | Current service patterns for manual verification | MTA official data |

**Linking Logic**:
- **Primary Join Keys**: `STATION` + `LINENAME` → `Complex ID`
- **Verification Fields**: `Complex Name` and `Daytime Routes` loosely correspond to `STATION` and `LINENAME` for manual inspection
- **Unique Identifier**: `station_id` serves as a unique key in turnstile data processing

---

### 📊 Station Data Extracted from Raw Sources

#### 🎫 Turnstile Station List
**File**: [`references/stations/stations_turnstile.csv`](../stations/stations_turnstile.csv)

Unique stations extracted from turnstile data files.

**Variables**:
- `STATION`: Station name as it appears in turnstile data
- `LINENAME`: Concatenated line services (e.g., "456", "NQRW")

**Example Records**:
```
STATION,LINENAME
"TIMES SQ-42 ST","1237ACENQRS"
"TIMES SQ-42 ST","NQRS"
"42 ST-TIMES SQ","1237ACE"
```
*Note how Times Square appears with multiple naming variations!*

---

### Official MTA Reference Files

#### 🌐 MTA Subway Stations and Complexes
**File**: [`references/stations/stations_complexes_official.csv`](../stations/stations_complexes_official.csv)  
**Source**: [NY Open Data - MTA Subway Stations and Complexes](https://data.ny.gov/Transportation/MTA-Subway-Stations-and-Complexes/5f5g-n3cz/)

The authoritative list of all subway stations **aggregated by station complex**.

**Key Information**:
- 445 station complexes (count based on current reference file; may change with upstream updates)
- Includes Complex IDs (our primary mapping target)
- ADA accessibility status
- Manhattan CBD designation
- GTFS Stop IDs for transit app integration

#### 🚉 MTA Subway Stations (Individual)
**File**: [`references/stations/stations_official.csv`](../stations/stations_official.csv)  
**Source**: [NY Open Data - MTA Subway Stations](https://data.ny.gov/Transportation/MTA-Subway-Stations/39hk-dx4f/)

Lists all 496 individual subway stations (not aggregated by complex; count may change with upstream updates).

**Used For**: Understanding how individual stations group into complexes.

---

## Important Considerations

### Station Complex Aggregation
- **Why Complexes?**: MTA cannot accurately allocate ridership between connected stations
- **Example**: Times Square complex includes multiple interconnected platforms
- **Impact**: All analysis must be at complex level, not individual station level

### Temporal Stability
- Station names in turnstile data change over time
- Our mapping handles all historical variations
- New stations or changes require mapping updates

## Terminology

- `Complex ID`: Official MTA identifier for a station complex (numeric), sourced from `references/stations/stations_complexes_official.csv`.
- `station_id`: Internal identifier used for normalizing turnstile records; composed as `STATION + '_' + LINENAME` during staging.
- `GTFS Stop ID`: Transit feed identifier for individual stops; useful for app integrations and reference joins.

## Usage in Analysis Pipeline

1. **Turnstile Processing**: [`scripts/stage_turnstile_data.py`](../../scripts/stage_turnstile_data.py) applies the mapping
2. **Baseline Calculation**: Ensures historical data aligns with modern complex definitions
3. **Final Analysis**: Enables accurate year-over-year comparisons

