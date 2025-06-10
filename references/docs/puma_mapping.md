# 🗺️ PUMA Mapping Documentation

## 📋 Overview

This document explains how we determine the Public Use Microdata Area (PUMA) for each subway station using geospatial coordinates. PUMAs are statistical geographic areas defined by the US Census Bureau for the dissemination of Public Use Microdata Sample (PUMS) data. 

### Key Facts about NYC PUMAs:
- Minimum population of 100,000 per PUMA
- Aggregated from census tracts
- Approximate Community Districts (CDs) or combinations of CDs
- NYC has 55 PUMAs (compared to 59 Community Districts, due to combinations)
- Boundaries derived from the US Census Bureau's TIGER project
- Geographically modified to fit the New York City base map

## 🎯 Purpose

Mapping stations to PUMAs allows us to:
- Aggregate ridership data at the neighborhood level
- Analyze transit patterns across different areas of NYC
- Compare ridership trends between neighborhoods
- Link transit data with census demographic information

## 📊 Data Sources

### 1. PUMA Boundaries (2020 Census, Updated 2025)
**File**: [`data/external/puma/2020_PUMAs.csv`](../../data/external/puma/2020_PUMAs.csv)  

**Primary Sources**:
- [NYC Open Data - 2020 Public Use Microdata Areas](https://data.cityofnewyork.us/City-Government/2020-Public-Use-Microdata-Areas-PUMAs-/pikk-p9nv/)
- [NYC Planning - Public Use Microdata Areas](https://www.nyc.gov/content/planning/pages/resources/datasets/public-use-microdata-areas)

This file contains:
- `PUMA`: 5-digit PUMA identifier (stored as string to preserve leading zeros)
- `the_geom`: PUMA boundary polygons in WKT (Well-Known Text) format
- Coordinate system: EPSG:4326 (WGS-84 latitude/longitude)

### 2. Station Coordinates
**File**: [`data/external/stations/stations_complexes.csv`](../../data/external/stations/stations_complexes.csv)

Contains subway station complex information:
- `Complex ID`: Unique identifier for each station complex
- `Station Name`: Human-readable station name
- `Latitude`: Station latitude coordinate
- `Longitude`: Station longitude coordinate

### 3. PUMA Names Reference
**Sources**:
- [US Census Bureau - PUMA Names Files (PDF)](https://www2.census.gov/geo/pdfs/reference/puma2020/2020_PUMA_Names.pdf)
- [Census Bureau PUMA Guidance](https://www.census.gov/programs-surveys/geography/guidance/geo-areas/pumas.html)

Official PUMA codes and their corresponding neighborhood names for human-readable references. The names file is issued by the Census Bureau's Geography Division (the legal authority for PUMA definitions), with the latest version dated August 2022.

## 🔧 Mapping Process

### Script
**File**: [`scripts/add_puma_to_stations.py`](../../scripts/add_puma_to_stations.py)

### Algorithm Steps

1. **Load PUMA Boundaries**
   ```python
   # Parse WKT strings into geometric polygons
   pumas = pd.read_csv(puma_path)
   pumas['geometry'] = pumas['the_geom'].apply(wkt.loads)
   # Create GeoDataFrame with proper coordinate system
   pumas_gdf = gpd.GeoDataFrame(pumas, crs="EPSG:4326")
   ```

2. **Create Station Points**
   ```python
   # Convert lat/lon to geometric points
   stations_gdf = gpd.GeoDataFrame(
       stations,
       geometry=gpd.points_from_xy(stations["Longitude"], stations["Latitude"]),
       crs="EPSG:4326"
   )
   ```

3. **Spatial Join**
   ```python
   # Find which PUMA polygon contains each station point
   stations_with_puma = gpd.sjoin(
       stations_gdf,
       pumas_gdf[["PUMA", "geometry"]],
       how="left",
       predicate="within"
   )
   ```

4. **Output Mapping**
   The result is saved to [`references/stations/station_to_puma.csv`](../stations/station_to_puma.csv)

### Technical Details

- **Spatial Predicate**: Uses "within" to check if station points fall completely inside PUMA polygons
- **Coordinate System**: Both datasets use EPSG:4326 (WGS-84) for consistency
- **Join Type**: Left join ensures all stations are retained, even if they fall outside PUMA boundaries
- **Libraries Used**:
  - `geopandas`: Main geospatial operations
  - `shapely`: Geometry parsing and manipulation
  - `pandas`: Data manipulation
  - `rtree`: Spatial indexing for performance

## 📍 Example Mapping

```
Complex ID | Station Name          | PUMA  | PUMA Name
-----------|--------------------- |-------|---------------------------
1          | Times Sq-42 St       | 03810 | Midtown-Midtown South
2          | Grand Central-42 St  | 03809 | Murray Hill-Gramercy
83         | 125 St               | 03803 | Central Harlem North
```

## 🚨 Special Considerations

### Edge Cases
- **Stations on PUMA boundaries**: The "within" predicate assigns stations to PUMAs only if they're completely inside. Stations exactly on boundaries may not be assigned.
- **Stations outside NYC**: Some stations (e.g., in New Jersey for PATH) may fall outside NYC PUMA boundaries and will have null PUMA values.

### Data Quality
- Station coordinates must be accurate for proper PUMA assignment
- PUMA boundaries are based on 2020 Census geography
- Any station relocations or new stations require re-running the mapping

## 📈 Usage in Analysis

The station-to-PUMA mapping enables aggregation in our analysis pipeline:

1. **Daily Processing**: Station-level ridership data
2. **PUMA Aggregation**: Sum ridership for all stations within each PUMA
3. **Output Files**:
   - [`results/baseline/monthly_baseline_puma.csv`](../../results/baseline/monthly_baseline_puma.csv)
   - [`results/ridership/monthly_ridership_puma.csv`](../../results/ridership/monthly_ridership_puma.csv)
   - [`results/final/monthly_ridership_puma.csv`](../../results/final/monthly_ridership_puma.csv)

This neighborhood-level analysis reveals ridership patterns that aren't visible at the city-wide or station level, enabling better understanding of how different areas of NYC use the subway system.