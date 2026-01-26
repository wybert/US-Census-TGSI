#!/usr/bin/env python
"""
Validation Spatial Representation (Optimized)
=============================================
Merges validation metrics (CR, counts) with Census Block geometry for mapping.
Optimized to process year-by-year and shift geometry once.
"""

import json
import os
import warnings

import geopandas as gpd
import pandas as pd
import polars as pl
import shapely.ops

# Suppress warnings
warnings.filterwarnings("ignore")


def shift_lon(lon):
    if lon > 0:
        return lon - 360
    else:
        return lon


def shift_geometry(geometry):
    """Apply shift_lon to every coordinate in a geometry."""

    def shift_func(x, y, z=None):
        return (shift_lon(x), y) if z is None else (shift_lon(x), y, z)

    return shapely.ops.transform(shift_func, geometry)


def main():
    # Load configuration
    with open("setting.json") as f:
        config = json.load(f)

    workspace = config["workspace"]

    # Updated paths
    # Using Blocks GeoParquet instead of iterating empty/zipped folder
    blocks_path = os.path.join(
        workspace, "data/census_data_2020/us_census_blocks_2020.geoparquet"
    )
    metrics_path = os.path.join(
        workspace, "data/all_years_tweet_count_with_pop_CR.parquet"
    )
    out_dir = os.path.join(workspace, "data", "spatial_representation")
    os.makedirs(out_dir, exist_ok=True)

    print(f"Loading Geometry from {blocks_path}...")
    try:
        # Load only necessary columns if possible, but we need geometry
        blocks = gpd.read_parquet(blocks_path)
    except Exception as e:
        print(f"Error loading blocks: {e}")
        return

    print(f"Loaded {len(blocks)} blocks. Shifting Geometry...")
    # Shift geometry ONCE for all years
    # Using simple apply as pandarallel might be overhead for just shifting if we do it once
    # But for 8M rows, parallel is good.

    try:
        from pandarallel import pandarallel

        pandarallel.initialize(progress_bar=True, verbose=0)
        blocks["geometry"] = blocks["geometry"].parallel_apply(shift_geometry)
    except ImportError:
        print("Pandarallel not found, using single core apply...")
        blocks["geometry"] = blocks["geometry"].apply(shift_geometry)

    print("Geometry shifted.")

    print(f"Loading Metrics from {metrics_path}...")
    metrics_pl = pl.read_parquet(metrics_path)

    # Get years
    if "year" in metrics_pl.columns:
        years = metrics_pl["year"].unique().sort().to_list()
    else:
        print("No 'year' column in metrics. processing as single dataset.")
        years = [None]

    for year in years:
        if year:
            print(f"Processing Year {year}...")
            year_df = metrics_pl.filter(pl.col("year") == year).to_pandas()
            out_name = f"census_blocks_merged_shifted_geo_{year}.parquet"
        else:
            print("Processing all data...")
            year_df = metrics_pl.to_pandas()
            out_name = "census_blocks_merged_shifted_geo_all.parquet"

        # Merge
        # We use left join on Blocks to ensure map covers full area (even nulls)
        # Or inner if we only want data? Usually maps need full coverage (grey out missing).
        # But 'year_df' only has populated blocks?
        # Let's use left merge on blocks.

        merged = blocks.merge(year_df, on="GEOID20", how="left")

        out_file = os.path.join(out_dir, out_name)
        print(f"  Saving to {out_file}...")
        merged.to_parquet(out_file)

    print("All years processed.")


if __name__ == "__main__":
    main()
