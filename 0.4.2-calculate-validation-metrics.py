#!/usr/bin/env python
"""
Calculate Validation Metrics (CR, Gini) for All Years and Tiers
===============================================================
Calculates Coverage Ratio (CR) and Gini Coefficients for aggregated sentiment data.
Processes all confidence tiers (Strict, High, Medium, All).

Input:
- Population Data: data/census_pop/pop data/*.parquet
- Aggregated Sentiment: data/aggregated_sentiment_stats/yearly_YYYY.parquet

Output:
- validation_results/gini_summary.csv
- validation_results/lorenz_points_{year}.csv
- validation_results/validation_metrics_{year}.parquet (for mapping)
"""

import glob
import json
import os
import time

import numpy as np
import polars as pl


def calculate_gini(values, weights=None):
    """
    Calculate Gini coefficient.
    Values: metric (e.g., tweet_count)
    Weights: population (optional, but for spatial Gini we usually compare cumulative % pop vs cumulative % tweets)
    """
    # Simple Gini calculation comparing two distributions (Population vs Tweets)
    # Sort by ratio (tweets/pop)

    if len(values) == 0:
        return 0.0

    df = pl.DataFrame({"val": values, "weight": weights})

    # Filter out zero-population blocks to avoid division by zero issues in ratios
    df = df.filter(pl.col("weight") > 0)

    if df.height == 0:
        return 0.0

    # Calculate ratio (density)
    df = df.with_columns((pl.col("val") / pl.col("weight")).alias("ratio"))

    # Sort by ratio ascending (poorest to richest in terms of tweets/capita)
    df = df.sort("ratio")

    # Calculate cumulative proportions
    cum_pop = df["weight"].cum_sum() / df["weight"].sum()
    cum_val = df["val"].cum_sum() / df["val"].sum()

    # Calculate Gini
    # Area under Lorenz curve (Trapezoidal rule)
    # Gini = 1 - 2 * Area

    # Insert 0,0 point
    cum_pop_list = [0.0] + cum_pop.to_list()
    cum_val_list = [0.0] + cum_val.to_list()

    area = np.trapezoid(cum_val_list, cum_pop_list)
    gini = 1 - 2 * area

    return gini, (cum_pop_list, cum_val_list)


def main():
    # Load config
    with open("setting.json") as f:
        config = json.load(f)

    workspace = config["workspace"]
    pop_dir = config["census_pop"]
    agg_dir = os.path.join(workspace, "data", "aggregated_sentiment_stats")
    out_dir = os.path.join(workspace, "data", "validation_results")
    os.makedirs(out_dir, exist_ok=True)

    # 1. Load Population Data
    print("Loading Population Data...")
    pop_files = glob.glob(os.path.join(pop_dir, "*.parquet"))
    pop_df = pl.read_parquet(pop_files, columns=["GEOID20", "population"])
    # Ensure columns GEOID20, population
    pop_df = pop_df.select([pl.col("GEOID20"), pl.col("population").cast(pl.Int64)])

    print(f"Loaded population for {pop_df.height} blocks.")
    total_pop = pop_df["population"].sum()
    print(f"Total US Population (Census 2020 Blocks): {total_pop:,}")

    gini_results = []

    # 2. Process Years
    start_year, end_year = 2012, 2023
    tiers = ["strict", "high", "medium", "all"]

    for year in range(start_year, end_year + 1):
        print(f"\nProcessing Year {year}...")
        agg_file = os.path.join(agg_dir, f"yearly_{year}.parquet")

        if not os.path.exists(agg_file):
            print(f"Warning: File not found {agg_file}")
            continue

        # Load Aggregated Data
        agg_df = pl.read_parquet(agg_file)

        # Join with Population (Left Join on Pop to keep all blocks)
        # Blocks with no tweets will have nulls in agg columns -> fill with 0
        merged_df = pop_df.join(agg_df, on="GEOID20", how="left")

        # Determine columns to fill
        fill_cols = [f"{t}_tweet_count" for t in tiers] + [
            f"{t}_user_count" for t in tiers
        ]
        merged_df = merged_df.with_columns(
            [pl.col(c).fill_null(0) for c in fill_cols if c in merged_df.columns]
        )

        # Calculate Metrics for each Tier
        year_lorenz_points = {}

        for tier in tiers:
            count_col = f"{tier}_tweet_count"
            if count_col not in merged_df.columns:
                continue

            # Coverage Ratio (Tweet Count / Population)
            # Log2CR (Log2 of Coverage Ratio) - Handle 0/0
            # To handle log(0), usually add a small epsilon or handle separately.
            # Common practice: Log2((Count + 1) / Pop) * scalar? Or just Log2(CR).
            # Here we calculate raw CR first.

            # Gini Calculation
            gini, points = calculate_gini(merged_df[count_col], merged_df["population"])

            print(f"  - Tier {tier.upper()}: Gini = {gini:.4f}")

            gini_results.append({"year": year, "tier": tier, "gini": gini})

            year_lorenz_points[tier] = points

            # Add CR column to dataframe for saving
            merged_df = merged_df.with_columns(
                (pl.col(count_col) / pl.col("population")).alias(f"{tier}_cr")
            )

        # Save Metrics Parquet (for mapping)
        # We save only necessary columns to save space: GEOID, Pop, and CRs/Counts
        save_cols = ["GEOID20", "population"] + [
            c
            for c in merged_df.columns
            if "tweet_count" in c
            or "user_count" in c
            or "_cr" in c
            or "score_mean" in c
        ]

        out_parquet = os.path.join(out_dir, f"validation_metrics_{year}.parquet")
        merged_df.select(save_cols).write_parquet(out_parquet)

        # Save Lorenz Points (Sampling to reduce size if needed, but CSV handles it)
        # Just saving every 100th point to avoid massive CSVs?
        # Actually, let's just save the Gini summary for now. Lorenz points can be re-generated for plotting or saved if requested.

    # Save Gini Summary
    gini_df = pl.DataFrame(gini_results)
    gini_csv = os.path.join(out_dir, "gini_summary.csv")
    gini_df.write_csv(gini_csv)
    print(f"\nSaved Gini summary to {gini_csv}")


if __name__ == "__main__":
    main()
