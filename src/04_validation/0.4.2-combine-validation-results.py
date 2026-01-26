#!/usr/bin/env python
"""
Combine Validation Metrics into Single Parquet
==============================================
Combines validation_metrics_{year}.parquet files into a single
'all_years_tweet_count_with_pop_CR.parquet' file for spatial visualization.

Input: data/validation_results/validation_metrics_{year}.parquet
Output: data/all_years_tweet_count_with_pop_CR.parquet
"""

import glob
import json
import os

import polars as pl


def main():
    # Load config
    with open("setting.json") as f:
        config = json.load(f)

    workspace = config["workspace"]
    in_dir = os.path.join(workspace, "data", "validation_results")
    out_file = os.path.join(
        workspace, "data", "all_years_tweet_count_with_pop_CR.parquet"
    )

    print(f"Searching for files in {in_dir}...")
    files = glob.glob(os.path.join(in_dir, "validation_metrics_*.parquet"))
    files.sort()

    if not files:
        print("No validation metrics files found.")
        return

    dfs = []
    for f in files:
        # Extract year from filename: validation_metrics_2012.parquet
        basename = os.path.basename(f)
        try:
            year = int(basename.split("_")[-1].split(".")[0])
        except ValueError:
            print(f"Skipping {basename} (cannot parse year)")
            continue

        print(f"Loading {basename} (Year {year})...")
        df = pl.read_parquet(f)

        # Add year column
        df = df.with_columns(pl.lit(year).alias("year"))
        dfs.append(df)

    if not dfs:
        print("No valid dataframes loaded.")
        return

    print("Concatenating...")
    combined_df = pl.concat(dfs)

    # Convert to pandas for compatibility with downstream pandas/geopandas scripts
    # or save as parquet that pandas can read. Polars to_parquet is compatible.

    print(f"Saving to {out_file}...")
    combined_df.write_parquet(out_file)
    print(f"Done. Shape: {combined_df.shape}")


if __name__ == "__main__":
    main()
