#!/usr/bin/env python3
"""
Merge all 51 US state census block shapefiles into a single GeoParquet file.
This creates a unified census blocks dataset for efficient spatial joins.
"""

import os
import json
import pandas as pd
import geopandas as gpd
from tqdm import tqdm
import warnings
warnings.filterwarnings('ignore')

# Load configuration
with open('setting.json') as f:
    config = json.load(f)

census_dir = config['census_data_2020']
output_dir = config['census_data_2020']

# Output files
output_parquet = os.path.join(output_dir, 'us_census_blocks_2020.parquet')
output_geoparquet = os.path.join(output_dir, 'us_census_blocks_2020.geoparquet')

print("=" * 60)
print("Merging US Census Blocks (All 51 States)")
print("=" * 60)
print(f"Source: {census_dir}")
print(f"Output: {output_parquet}")
print()

# Find all census ZIP files
census_files = sorted([f for f in os.listdir(census_dir) if f.endswith('_tabblock20.zip')])
print(f"Found {len(census_files)} state files")


# Read and merge all states
gdfs = []
total_blocks = 0

for census_file in tqdm(census_files, desc="Loading states"):
    state_fips = census_file.split('_')[2]
    zip_path = os.path.join(census_dir, census_file)
    
    try:
        # Read from ZIP using geopandas (automatically handles /vsizip/)
        gdf = gpd.read_file(f'zip://{zip_path}')
        
        # Select and compute fields
        gdf = gdf[['GEOID20', 'STATEFP20', 'COUNTYFP20', 'TRACTCE20', 'BLOCKCE20', 'geometry']]
        
        # Compute block area in square meters (project to EPSG:5070 - NAD83/Conus Albers)
        gdf_projected = gdf.to_crs('EPSG:5070')
        gdf['block_area_m2'] = gdf_projected.geometry.area
        gdf['block_diameter_m'] = (gdf['block_area_m2'] / 3.14159) ** 0.5 * 2
        
        # Keep geometry in EPSG:4326 (WGS84) for consistency
        gdf = gdf.to_crs('EPSG:4326')
        
        gdfs.append(gdf)
        total_blocks += len(gdf)
        
    except Exception as e:
        print(f"\n⚠ Warning: Failed to load {census_file}: {e}")
        continue

print()
print(f"Loaded {total_blocks:,} census blocks from {len(gdfs)} states")
print()

# Concatenate all states
print("Merging all states...")
merged_gdf = gpd.GeoDataFrame(
    pd.concat(gdfs, ignore_index=True),
    crs='EPSG:4326'
)

print(f"✓ Total blocks: {len(merged_gdf):,}")
print()

# Save as GeoParquet (includes geometry)
print(f"Saving as GeoParquet...")
merged_gdf.to_parquet(output_geoparquet, compression='snappy')
print(f"✓ Saved: {output_geoparquet}")

# Also save without geometry column for non-spatial queries (smaller file)
print(f"Saving as regular Parquet (no geometry)...")
merged_df = merged_gdf.drop(columns=['geometry'])
merged_df.to_parquet(output_parquet, compression='snappy')
print(f"✓ Saved: {output_parquet}")

print()
print("=" * 60)
print("Summary Statistics")
print("=" * 60)
print(f"Total census blocks: {len(merged_gdf):,}")
print(f"Total states: {merged_gdf['STATEFP20'].nunique()}")
print(f"Total counties: {merged_gdf['COUNTYFP20'].nunique()}")
print()
print("Blocks by state (top 10):")
print(merged_gdf['STATEFP20'].value_counts().head(10))
print()
print("File sizes:")
os.system(f"ls -lh {output_geoparquet} {output_parquet}")
print()
print("=" * 60)
print("✓ Census data merge complete!")
print("=" * 60)
print()
print("Usage in DuckDB:")
print(f"  SELECT * FROM read_parquet('{output_geoparquet}');")
print()
print("Usage in Python:")
print(f"  gdf = gpd.read_parquet('{output_geoparquet}')")
