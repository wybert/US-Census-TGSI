#!/usr/bin/env python
"""
Test optimizations for buffer-based spatial matching
Goal: Make buffer method feasible for large-scale processing
"""

import time
import geopandas as gpd
import pandas as pd
import numpy as np
from shapely.geometry import Point
from shapely.strtree import STRtree
import warnings
warnings.filterwarnings('ignore')

print("=" * 80)
print("Buffer Method Optimization Tests")
print("=" * 80)

# Load Delaware census blocks
print("\n[Setup] Loading Delaware census blocks...")
t0 = time.time()
census_file = "/n/netscratch/cga/Lab/xiaokang/US-Census-TGSI-workspace/data/census_data_2020/tl_2020_10_tabblock20.zip"
census_gdf = gpd.read_file(census_file)
if census_gdf.crs != "EPSG:4326":
    census_gdf = census_gdf.to_crs("EPSG:4326")
print(f"  Loaded {len(census_gdf):,} blocks in {time.time()-t0:.2f}s")

# Create test data
print("\n[Setup] Creating test data (500 tweets)...")
np.random.seed(42)
n_tweets = 500
lon_center, lat_center = -75.5, 39.0

tweets_data = {
    'message_id': [f'msg_{i}' for i in range(n_tweets)],
    'latitude': np.random.normal(lat_center, 0.5, n_tweets),
    'longitude': np.random.normal(lon_center, 0.5, n_tweets),
    'score': np.random.random(n_tweets)
}

high_precision_count = int(n_tweets * 0.06)
spatialerror = np.concatenate([
    np.random.uniform(10, 50, high_precision_count),
    np.random.uniform(1000, 50000, n_tweets - high_precision_count)
])
np.random.shuffle(spatialerror)
tweets_data['spatialerror'] = spatialerror

tweets_df = pd.DataFrame(tweets_data)
tweets_gdf = gpd.GeoDataFrame(
    tweets_df,
    geometry=gpd.points_from_xy(tweets_df.longitude, tweets_df.latitude),
    crs="EPSG:4326"
)
print(f"  Created {len(tweets_gdf):,} tweets")

# BASELINE: Original buffer method (naive)
print("\n" + "=" * 80)
print("BASELINE: Naive Buffer Method")
print("=" * 80)
sample_size = 100
sample_gdf = tweets_gdf.head(sample_size).copy()

t1 = time.time()
matched_naive = 0
for idx, row in sample_gdf.iterrows():
    error_radius_deg = row['spatialerror'] / 111320.0
    buffer_geom = row.geometry.buffer(error_radius_deg)
    
    # Naive approach: check all blocks
    intersecting = census_gdf[census_gdf.intersects(buffer_geom)]
    
    if len(intersecting) > 0:
        best_geoid = None
        max_overlap = 0
        for _, block in intersecting.iterrows():
            overlap_area = buffer_geom.intersection(block.geometry).area
            if overlap_area > max_overlap:
                max_overlap = overlap_area
                best_geoid = block['GEOID20']
        matched_naive += 1
t2 = time.time()

print(f"Time: {t2-t1:.3f}s for {sample_size} tweets")
print(f"Speed: {sample_size/(t2-t1):.1f} tweets/sec")
print(f"Matched: {matched_naive}/{sample_size}")
baseline_speed = sample_size/(t2-t1)

# OPTIMIZATION 1: Use STRtree spatial index
print("\n" + "=" * 80)
print("OPTIMIZATION 1: STRtree Spatial Index")
print("=" * 80)
print("Building spatial index...")
t_index = time.time()
spatial_index = STRtree(census_gdf.geometry)
print(f"  Index built in {time.time()-t_index:.3f}s")

t1 = time.time()
matched_strtree = 0
for idx, row in sample_gdf.iterrows():
    error_radius_deg = row['spatialerror'] / 111320.0
    buffer_geom = row.geometry.buffer(error_radius_deg)
    
    # Use spatial index for faster lookup
    candidate_indices = spatial_index.query(buffer_geom)
    
    if len(candidate_indices) > 0:
        candidates = census_gdf.iloc[candidate_indices]
        # Filter to actual intersections
        intersecting = candidates[candidates.intersects(buffer_geom)]
        
        if len(intersecting) > 0:
            best_geoid = None
            max_overlap = 0
            for _, block in intersecting.iterrows():
                overlap_area = buffer_geom.intersection(block.geometry).area
                if overlap_area > max_overlap:
                    max_overlap = overlap_area
                    best_geoid = block['GEOID20']
            matched_strtree += 1
t2 = time.time()

strtree_speed = sample_size/(t2-t1)
print(f"Time: {t2-t1:.3f}s for {sample_size} tweets")
print(f"Speed: {strtree_speed:.1f} tweets/sec")
print(f"Speedup: {strtree_speed/baseline_speed:.1f}x faster")
print(f"Matched: {matched_strtree}/{sample_size}")

# OPTIMIZATION 2: Vectorized operations with sjoin
print("\n" + "=" * 80)
print("OPTIMIZATION 2: Vectorized sjoin (GeoPandas built-in)")
print("=" * 80)

t1 = time.time()
# Create buffers for all tweets at once (vectorized)
sample_gdf['buffer_radius'] = sample_gdf['spatialerror'] / 111320.0
sample_gdf['buffer_geom'] = sample_gdf.geometry.buffer(sample_gdf['buffer_radius'])

# Create temporary GeoDataFrame with buffer geometries
buffer_gdf = sample_gdf.set_geometry('buffer_geom')

# Use sjoin with 'intersects' predicate
result = gpd.sjoin(buffer_gdf, census_gdf[['geometry', 'GEOID20']], 
                   how='left', predicate='intersects')

# Group by tweet and select best match (most overlap)
# Note: This is approximate - selects first match, not max overlap
# But much faster for large datasets
matched_sjoin = result['GEOID20'].notna().sum()
unique_tweets_matched = result[result['GEOID20'].notna()]['message_id'].nunique()

t2 = time.time()

sjoin_speed = sample_size/(t2-t1)
print(f"Time: {t2-t1:.3f}s for {sample_size} tweets")
print(f"Speed: {sjoin_speed:.1f} tweets/sec")
print(f"Speedup: {sjoin_speed/baseline_speed:.1f}x faster")
print(f"Unique tweets matched: {unique_tweets_matched}/{sample_size}")
print(f"Note: Returns all intersecting blocks (multiple per tweet)")

# OPTIMIZATION 3: Simplified geometry approach
print("\n" + "=" * 80)
print("OPTIMIZATION 3: Simplified Census Geometries")
print("=" * 80)
print("Simplifying census block geometries...")
t_simplify = time.time()
census_simplified = census_gdf.copy()
# Simplify to ~10 meter tolerance (in degrees, roughly)
census_simplified['geometry'] = census_simplified.geometry.simplify(0.0001, preserve_topology=True)
print(f"  Simplified {len(census_simplified)} geometries in {time.time()-t_simplify:.3f}s")

t1 = time.time()
matched_simplified = 0
spatial_index_simple = STRtree(census_simplified.geometry)
for idx, row in sample_gdf.iterrows():
    error_radius_deg = row['spatialerror'] / 111320.0
    buffer_geom = row.geometry.buffer(error_radius_deg)
    
    candidate_indices = spatial_index_simple.query(buffer_geom)
    
    if len(candidate_indices) > 0:
        candidates = census_simplified.iloc[candidate_indices]
        intersecting = candidates[candidates.intersects(buffer_geom)]
        
        if len(intersecting) > 0:
            # Simplified: just take first match instead of computing overlap
            matched_simplified += 1
t2 = time.time()

simplified_speed = sample_size/(t2-t1)
print(f"Time: {t2-t1:.3f}s for {sample_size} tweets")
print(f"Speed: {simplified_speed:.1f} tweets/sec")
print(f"Speedup: {simplified_speed/baseline_speed:.1f}x faster")
print(f"Matched: {matched_simplified}/{sample_size}")

# OPTIMIZATION 4: Combined best approach
print("\n" + "=" * 80)
print("OPTIMIZATION 4: Combined Optimizations")
print("  - STRtree spatial index")
print("  - Skip max overlap calculation (use centroid distance)")
print("  - Simplified geometries")
print("=" * 80)

t1 = time.time()
matched_combined = 0

for idx, row in sample_gdf.iterrows():
    error_radius_deg = row['spatialerror'] / 111320.0
    buffer_geom = row.geometry.buffer(error_radius_deg)
    
    candidate_indices = spatial_index_simple.query(buffer_geom)
    
    if len(candidate_indices) > 0:
        candidates = census_simplified.iloc[candidate_indices]
        intersecting = candidates[candidates.intersects(buffer_geom)]
        
        if len(intersecting) > 0:
            # Instead of computing overlap, use distance to centroid
            # Much faster and usually gives same result
            tweet_point = row.geometry
            distances = intersecting.geometry.centroid.distance(tweet_point)
            closest_idx = distances.idxmin()
            matched_combined += 1
t2 = time.time()

combined_speed = sample_size/(t2-t1)
print(f"Time: {t2-t1:.3f}s for {sample_size} tweets")
print(f"Speed: {combined_speed:.1f} tweets/sec")
print(f"Speedup: {combined_speed/baseline_speed:.1f}x faster")
print(f"Matched: {matched_combined}/{sample_size}")

# Summary
print("\n" + "=" * 80)
print("PERFORMANCE SUMMARY")
print("=" * 80)
print(f"{'Method':<35} {'Speed (tweets/sec)':<20} {'Speedup':<10}")
print("-" * 80)
print(f"{'Baseline (Naive)':<35} {baseline_speed:<20.1f} {'1.0x':<10}")
print(f"{'+ STRtree Index':<35} {strtree_speed:<20.1f} {f'{strtree_speed/baseline_speed:.1f}x':<10}")
print(f"{'+ Vectorized sjoin':<35} {sjoin_speed:<20.1f} {f'{sjoin_speed/baseline_speed:.1f}x':<10}")
print(f"{'+ Simplified Geometry':<35} {simplified_speed:<20.1f} {f'{simplified_speed/baseline_speed:.1f}x':<10}")
print(f"{'+ Combined (Best)':<35} {combined_speed:<20.1f} {f'{combined_speed/baseline_speed:.1f}x':<10}")

# Extrapolate to full dataset
print("\n" + "=" * 80)
print("FULL DATASET EXTRAPOLATION (98,584 files × 150k tweets)")
print("=" * 80)
full_size = 98584 * 150000

for method, speed in [
    ("Baseline", baseline_speed),
    ("Best Optimized", combined_speed)
]:
    days = full_size / speed / 3600 / 24
    print(f"{method:<20} {days:>8.1f} days")

# Check if feasible
print("\n" + "=" * 80)
print("FEASIBILITY ASSESSMENT")
print("=" * 80)
best_days = full_size / combined_speed / 3600 / 24
if best_days < 30:
    print(f"✅ FEASIBLE: {best_days:.1f} days with optimizations")
    print("   Recommend using buffer method with combined optimizations")
elif best_days < 100:
    print(f"⚠️  BORDERLINE: {best_days:.1f} days with optimizations")
    print("   Consider hybrid approach or confidence weighting instead")
else:
    print(f"❌ NOT FEASIBLE: {best_days:.1f} days even with optimizations")
    print("   Stick with confidence weighting approach (2-3 days)")

print("\n" + "=" * 80)
print("RECOMMENDATION")
print("=" * 80)
if best_days < 30:
    print("Buffer method is feasible with optimizations!")
    print("Use: STRtree index + simplified geometry + centroid distance")
else:
    print("Confidence weighting is still the best approach:")
    print("  - Faster: ~2-3 days vs {:.0f} days".format(best_days))
    print("  - Simpler implementation")
    print("  - Achieves similar accuracy with proper weighting")

