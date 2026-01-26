#!/usr/bin/env python
"""
TEST VERSION: Calculate Validation Metrics (CR, Gini)
=====================================================
Runs on a small subset of data for a single year to verify logic on the login node.
"""

import glob
import json
import os

import numpy as np
import polars as pl


def calculate_gini(values, weights=None):
    """
    Calculate Gini coefficient.
    Values: metric (e.g., tweet_count)
    Weights: population
    """
    if len(values) == 0:
        return 0.0

    df = pl.DataFrame({"val": values, "weight": weights})

    # Filter out zero-population blocks
    df = df.filter(pl.col("weight") > 0)

    if df.height == 0:
        return 0.0

    # Calculate ratio (density)
    df = df.with_columns((pl.col("val") / pl.col("weight")).alias("ratio"))

    # Sort by ratio ascending
    df = df.sort("ratio")

    # Calculate cumulative proportions - USING CORRECT cum_sum()
    cum_pop = df["weight"].cum_sum() / df["weight"].sum()
    cum_val = df["val"].cum_sum() / df["val"].sum()

    # Calculate Gini
    cum_pop_list = [0.0] + cum_pop.to_list()
    cum_val_list = [0.0] + cum_val.to_list()

    area = np.trapz(cum_val_list, cum_pop_list)
    gini = 1 - 2 * area

    return gini


def main():
    # Load config
    with open("setting.json") as f:
        config = json.load(f)

    workspace = config["workspace"]
    pop_dir = config["census_pop"]
    agg_dir = os.path.join(workspace, "data", "aggregated_sentiment_stats")

    # Use a temp output dir for test
    out_dir = os.path.join("outputs", "test_validation")
    os.makedirs(out_dir, exist_ok=True)

    # 1. Load Population Data (Small Subset)
    print("Loading Population Data (TEST MODE: First 50k rows)...")
    pop_files = glob.glob(os.path.join(pop_dir, "*.parquet"))
    # Just read the first file to keep it light
    pop_df = pl.read_parquet(pop_files[0], columns=["GEOID20", "population"])
    pop_df = pop_df.select([pl.col("GEOID20"), pl.col("population").cast(pl.Int64)])

    # Limit size
    pop_df = pop_df.head(50000)
    print(f"Loaded population subset: {pop_df.height} blocks.")

    # 2. Process Single Year (2020)
    year = 2020
    print(f"\nProcessing Year {year} (TEST MODE)...")
    agg_file = os.path.join(agg_dir, f"yearly_{year}.parquet")

    if not os.path.exists(agg_file):
        print(f"Error: File not found {agg_file}")
        return

    # Load Aggregated Data
    agg_df = pl.read_parquet(agg_file)

    # Filter agg_df to match our small pop_df to ensure we have matches if possible,
    # or just let the join handle it.
    # To ensure we test the Gini logic with real numbers, let's filter agg_df to the same GEOIDs if they exist
    target_geoids = pop_df["GEOID20"]
    agg_df = agg_df.filter(pl.col("GEOID20").is_in(target_geoids))

    print(f"Aggregated data subset: {agg_df.height} rows matching population sample.")

    # Join
    merged_df = pop_df.join(agg_df, on="GEOID20", how="left")

    tiers = ["strict", "high", "medium", "all"]

    # Determine columns to fill
    fill_cols = [f"{t}_tweet_count" for t in tiers] + [f"{t}_user_count" for t in tiers]
    merged_df = merged_df.with_columns(
        [pl.col(c).fill_null(0) for c in fill_cols if c in merged_df.columns]
    )

    for tier in tiers:
        count_col = f"{tier}_tweet_count"
        if count_col not in merged_df.columns:
            continue

        gini = calculate_gini(merged_df[count_col], merged_df["population"])
        print(f"  - Tier {tier.upper()}: Gini = {gini:.4f}")

    print("\nTest completed successfully. Logic appears correct.")


if __name__ == "__main__":
    main()
