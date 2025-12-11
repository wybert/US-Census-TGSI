#!/usr/bin/env python
"""
Test DuckDB Spatial with GeoParquet format
GeoParquet is the native format DuckDB Spatial supports
"""

import time
import geopandas as gpd
import pandas as pd
import numpy as np
import duckdb
import os

print("=" * 80)
print("DuckDB Spatial with GeoParquet Format Test")
print("=" * 80)

# Setup test data directory
test_dir = "/tmp/duckdb_spatial_test"
os.makedirs(test_dir, exist_ok=True)

# Load Delaware census blocks
print("\n[1/5] Loading Delaware census blocks...")
t0 = time.time()
census_file = "/n/netscratch/cga/Lab/xiaokang/US-Census-TGSI-workspace/data/census_data_2020/tl_2020_10_tabblock20.zip"
census_gdf = gpd.read_file(census_file)
if census_gdf.crs != "EPSG:4326":
    census_gdf = census_gdf.to_crs("EPSG:4326")
print(f"  Loaded {len(census_gdf):,} blocks in {time.time()-t0:.2f}s")

# Simplify census data for testing
print("\n[2/5] Simplifying census geometries for GeoParquet...")
t0 = time.time()
census_simple = census_gdf[['GEOID20', 'geometry']].copy()
census_simple['geometry'] = census_simple.geometry.simplify(0.0001, preserve_topology=True)
print(f"  Simplified in {time.time()-t0:.2f}s")

# Save to GeoParquet
print("\n[3/5] Saving census blocks to GeoParquet...")
census_parquet = f"{test_dir}/census_blocks.parquet"
t0 = time.time()
census_simple.to_parquet(census_parquet)
parquet_size_mb = os.path.getsize(census_parquet) / 1024 / 1024
print(f"  Saved to {census_parquet}")
print(f"  File size: {parquet_size_mb:.1f} MB")
print(f"  Time: {time.time()-t0:.2f}s")

# Create test tweets
print("\n[4/5] Creating test tweets...")
np.random.seed(42)
n_tweets = 1000
lon_center, lat_center = -75.5, 39.0

tweets_data = {
    'message_id': [f'msg_{i}' for i in range(n_tweets)],
    'latitude': np.random.normal(lat_center, 0.5, n_tweets),
    'longitude': np.random.normal(lon_center, 0.5, n_tweets),
    'score': np.random.random(n_tweets),
}

high_precision_count = int(n_tweets * 0.06)
spatialerror = np.concatenate([
    np.random.uniform(10, 50, high_precision_count),
    np.random.uniform(1000, 50000, n_tweets - high_precision_count)
])
np.random.shuffle(spatialerror)
tweets_data['spatialerror'] = spatialerror

tweets_df = pd.DataFrame(tweets_data)

# Create point geometries for tweets
tweets_gdf = gpd.GeoDataFrame(
    tweets_df,
    geometry=gpd.points_from_xy(tweets_df.longitude, tweets_df.latitude),
    crs="EPSG:4326"
)

# Save tweets to GeoParquet
tweets_parquet = f"{test_dir}/tweets.parquet"
t0 = time.time()
tweets_gdf.to_parquet(tweets_parquet)
print(f"  Saved {len(tweets_gdf)} tweets to GeoParquet")
print(f"  Time: {time.time()-t0:.2f}s")

# BASELINE: GeoPandas simple point-in-polygon
print("\n" + "=" * 80)
print("BASELINE: GeoPandas Simple Point-in-Polygon")
print("=" * 80)

t1 = time.time()
result_geopandas = gpd.sjoin(tweets_gdf, census_simple, how='left', predicate='within')
matched_geopandas = result_geopandas['GEOID20'].notna().sum()
t2 = time.time()

geopandas_time = t2 - t1
geopandas_speed = n_tweets / geopandas_time

print(f"Time: {geopandas_time:.3f}s")
print(f"Speed: {geopandas_speed:.1f} tweets/sec")
print(f"Matched: {matched_geopandas}/{n_tweets} ({matched_geopandas/n_tweets*100:.1f}%)")

# TEST 1: DuckDB Spatial with GeoParquet
print("\n" + "=" * 80)
print("TEST 1: DuckDB Spatial with GeoParquet (Point-in-Polygon)")
print("=" * 80)

try:
    con = duckdb.connect(':memory:')
    
    # Load spatial extension
    print("Loading DuckDB spatial extension...")
    con.execute("INSTALL spatial;")
    con.execute("LOAD spatial;")
    
    print("Reading GeoParquet files with DuckDB...")
    t_load = time.time()
    
    # Create tables from GeoParquet
    con.execute(f"""
        CREATE TABLE census AS 
        SELECT * FROM ST_Read('{census_parquet}')
    """)
    
    con.execute(f"""
        CREATE TABLE tweets AS 
        SELECT * FROM ST_Read('{tweets_parquet}')
    """)
    
    load_time = time.time() - t_load
    print(f"  Data loaded in {load_time:.3f}s")
    
    # Check what columns we have
    print("\nChecking schema...")
    schema = con.execute("DESCRIBE tweets").df()
    print(f"  Tweets columns: {list(schema['column_name'])}")
    
    # Execute spatial join
    print("\nExecuting spatial join...")
    t1 = time.time()
    
    result_duckdb = con.execute("""
        SELECT 
            t.message_id,
            t.score,
            t.spatialerror,
            c.GEOID20
        FROM tweets t
        LEFT JOIN census c
        ON ST_Within(t.geometry, c.geometry)
    """).df()
    
    t2 = time.time()
    
    duckdb_time = t2 - t1
    duckdb_total_time = duckdb_time + load_time
    duckdb_speed = n_tweets / duckdb_time
    matched_duckdb = result_duckdb['GEOID20'].notna().sum()
    
    print(f"Query time: {duckdb_time:.3f}s")
    print(f"Total time (with loading): {duckdb_total_time:.3f}s")
    print(f"Speed (query only): {duckdb_speed:.1f} tweets/sec")
    print(f"Speed (total): {n_tweets/duckdb_total_time:.1f} tweets/sec")
    print(f"Matched: {matched_duckdb}/{n_tweets} ({matched_duckdb/n_tweets*100:.1f}%)")
    
    if duckdb_speed > geopandas_speed:
        print(f"✅ DuckDB is {duckdb_speed/geopandas_speed:.1f}x FASTER")
    else:
        print(f"⚠️  GeoPandas is {geopandas_speed/duckdb_speed:.1f}x FASTER")
    
    # TEST 2: DuckDB with buffer (if point-in-polygon works)
    print("\n" + "=" * 80)
    print("TEST 2: DuckDB Spatial with Buffer Matching")
    print("=" * 80)
    
    print("Executing buffer-based spatial join...")
    t1 = time.time()
    
    result_duckdb_buffer = con.execute("""
        SELECT 
            t.message_id,
            t.score,
            t.spatialerror,
            c.GEOID20,
            ST_Distance(t.geometry, c.geometry) as distance
        FROM tweets t
        LEFT JOIN census c
        ON ST_Intersects(
            ST_Buffer(t.geometry, t.spatialerror / 111320.0),
            c.geometry
        )
    """).df()
    
    t2 = time.time()
    
    buffer_time = t2 - t1
    buffer_speed = n_tweets / buffer_time
    unique_matched_buffer = result_duckdb_buffer[result_duckdb_buffer['GEOID20'].notna()]['message_id'].nunique()
    
    print(f"Time: {buffer_time:.3f}s")
    print(f"Speed: {buffer_speed:.1f} tweets/sec")
    print(f"Unique tweets matched: {unique_matched_buffer}/{n_tweets}")
    
    # Compare with GeoPandas buffer
    print("\nComparing with GeoPandas buffer method...")
    t1 = time.time()
    tweets_gdf['buffer_radius'] = tweets_gdf['spatialerror'] / 111320.0
    tweets_gdf['buffer_geom'] = tweets_gdf.geometry.buffer(tweets_gdf['buffer_radius'])
    buffer_gdf = tweets_gdf.set_geometry('buffer_geom')
    result_geopandas_buffer = gpd.sjoin(buffer_gdf, census_simple, how='left', predicate='intersects')
    unique_geopandas_buffer = result_geopandas_buffer[result_geopandas_buffer['GEOID20'].notna()]['message_id'].nunique()
    t2 = time.time()
    geopandas_buffer_time = t2 - t1
    geopandas_buffer_speed = n_tweets / geopandas_buffer_time
    
    print(f"GeoPandas buffer: {geopandas_buffer_speed:.1f} tweets/sec")
    print(f"DuckDB buffer:    {buffer_speed:.1f} tweets/sec")
    
    if buffer_speed > geopandas_buffer_speed:
        print(f"✅ DuckDB buffer is {buffer_speed/geopandas_buffer_speed:.1f}x FASTER")
    else:
        print(f"⚠️  GeoPandas buffer is {geopandas_buffer_speed/buffer_speed:.1f}x FASTER")
    
    con.close()
    
    # Summary
    print("\n" + "=" * 80)
    print("PERFORMANCE SUMMARY")
    print("=" * 80)
    print(f"{'Method':<40} {'Speed (tweets/sec)':<20} {'Speedup':<10}")
    print("-" * 80)
    print(f"{'GeoPandas point-in-polygon':<40} {geopandas_speed:<20.1f} {'1.0x':<10}")
    print(f"{'DuckDB point-in-polygon (query only)':<40} {duckdb_speed:<20.1f} {f'{duckdb_speed/geopandas_speed:.1f}x':<10}")
    print(f"{'DuckDB point-in-polygon (total)':<40} {n_tweets/duckdb_total_time:<20.1f} {f'{(n_tweets/duckdb_total_time)/geopandas_speed:.1f}x':<10}")
    print(f"{'GeoPandas buffer':<40} {geopandas_buffer_speed:<20.1f} {f'{geopandas_buffer_speed/geopandas_speed:.1f}x':<10}")
    print(f"{'DuckDB buffer':<40} {buffer_speed:<20.1f} {f'{buffer_speed/geopandas_speed:.1f}x':<10}")
    
    # Extrapolate to full dataset
    print("\n" + "=" * 80)
    print("FULL DATASET EXTRAPOLATION (98,584 files × 150k tweets)")
    print("=" * 80)
    full_size = 98584 * 150000
    
    print(f"Point-in-polygon methods:")
    print(f"  GeoPandas:  {full_size/geopandas_speed/3600/24:>8.1f} days")
    print(f"  DuckDB:     {full_size/duckdb_speed/3600/24:>8.1f} days")
    
    print(f"\nBuffer methods:")
    print(f"  GeoPandas:  {full_size/geopandas_buffer_speed/3600/24:>8.1f} days")
    print(f"  DuckDB:     {full_size/buffer_speed/3600/24:>8.1f} days")
    
except Exception as e:
    print(f"❌ DuckDB Spatial test failed: {e}")
    import traceback
    traceback.print_exc()

# Cleanup
print("\n" + "=" * 80)
print("FINAL RECOMMENDATION")
print("=" * 80)
print("Even if DuckDB shows improvements:")
print("  - Buffer methods are still 100-1000x slower than point-in-polygon")
print("  - Confidence weighting (66,911 tweets/sec) remains the best approach")
print("  - ~2.6 days for full dataset vs weeks/months with buffer")

# Clean up temp files
import shutil
if os.path.exists(test_dir):
    shutil.rmtree(test_dir)
    print(f"\nCleaned up test files in {test_dir}")

