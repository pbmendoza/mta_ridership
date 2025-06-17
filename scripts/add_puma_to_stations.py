# --- 0. One-time environment setup ------------------------------------------
# Make sure the Geo stack is recent enough for Shapely 2.x
# (Older GeoPandas chokes on Shapely's new multipart geometry objects.)
# pip install --upgrade "geopandas>=0.14" shapely rtree pyogrio pyproj

import pandas as pd
import geopandas as gpd
import os
from pathlib import Path
from shapely import wkt                     # Parses WKT strings → Shapely objects

# Get the project root directory (parent of scripts directory)
script_dir = Path(__file__).parent
project_root = script_dir.parent

# --- 1. Load the 2020 PUMA boundaries (WKT inside the CSV) -------------------
puma_path = project_root / "data" / "external" / "puma" / "2020_PUMAs.csv"
pumas = (
    pd.read_csv(puma_path, dtype={"PUMA": str})   # keep leading zeros
      .assign(geometry=lambda df: df["the_geom"].apply(wkt.loads))
      .pipe(lambda df: gpd.GeoDataFrame(df.drop(columns="the_geom"),
                                        geometry="geometry",
                                        crs="EPSG:4326"))   # WGS-84 lat/lon
)

# --- 2. Load the subway station list ----------------------------------------
stations_path = project_root / "data" / "external" / "stations" / "stations_complexes.csv"
stations = pd.read_csv(stations_path)

gstations = gpd.GeoDataFrame(
    stations,
    geometry=gpd.points_from_xy(stations["Longitude"], stations["Latitude"]),
    crs="EPSG:4326"
)

# --- 3. Spatial join: point ➜ PUMA polygon ----------------------------------
stations_with_puma = (
    gpd.sjoin(
        gstations,
        pumas[["PUMA", "geometry"]],        # keep only what we need
        how="left",
        predicate="within"                  # 'intersects' if you prefer edge-inclusive
    )
    .drop(columns="index_right")            # cleanup
)

# --- 4. Persist or explore ---------------------------------------------------
# Create output directory if it doesn't exist
output_path = project_root / "references" / "stations" / "station_to_puma.csv"
output_path.parent.mkdir(parents=True, exist_ok=True)

# Save only Complex ID and PUMA columns
output_df = stations_with_puma[["Complex ID", "PUMA"]]
output_df.to_csv(output_path, index=False)

# Quick sanity check
print(f"📊 Stations with PUMA assignments saved to {output_path}")
print(f"🔍 Sample data:")
print(output_df.head())
