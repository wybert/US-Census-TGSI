#!/usr/bin/env python
"""
Test Aggregation for 2022 - Census Block Sentiment
==================================================
Tests the comprehensive aggregation logic for Daily, Monthly, and Yearly granularities
with 4 confidence tiers.

Tiers:
1. Strict (Conf = 1.0)
2. High   (Conf >= 0.8)
3. Medium (Conf >= 0.5)
4. All    (All Data) + Weighted Metrics

Metrics: Count, UserCount, Mean, Std, Min, Max, Quantiles.
"""

import polars as pl
import os
import json
import glob
import time

# Configuration for Test
TEST_YEAR = 2022
INPUT_ROOT = "/n/netscratch/cga/Lab/xiaokang/US-Census-TGSI-workspace/data/tweets_with_census_blocks_confidence"
OUTPUT_ROOT = "/n/netscratch/cga/Lab/xiaokang/US-Census-TGSI-workspace/data/aggregated_test_2022"

def get_aggregation_exprs(prefix="", include_weighted=False):
    """
    Returns a list of Polars expressions for aggregation.
    """
    exprs = [
        pl.len().alias(f"{prefix}tweet_count"),
        pl.col("user_id").n_unique().alias(f"{prefix}user_count"),
        pl.col("score").mean().alias(f"{prefix}score_mean"),
        pl.col("score").std().alias(f"{prefix}score_std"),
        pl.col("score").min().alias(f"{prefix}score_min"),
        pl.col("score").max().alias(f"{prefix}score_max"),
        
        # Quantiles
        pl.col("score").quantile(0.10).alias(f"{prefix}score_10q"),
        pl.col("score").quantile(0.25).alias(f"{prefix}score_25q"),
        pl.col("score").median().alias(f"{prefix}score_50q"),
        pl.col("score").quantile(0.75).alias(f"{prefix}score_75q"),
        pl.col("score").quantile(0.90).alias(f"{prefix}score_90q"),
        
        # Confidence Stats
        pl.col("confidence").mean().alias(f"{prefix}confidence_mean")
    ]
    
    if include_weighted:
        # Weighted Mean = Sum(Score * Conf) / Sum(Conf)
        weighted_mean = (pl.col("score") * pl.col("confidence")).sum() / pl.col("confidence").sum()
        exprs.append(weighted_mean.alias(f"{prefix}weighted_score_mean"))
        
    return exprs

def process_granularity(df, group_cols, output_path, granularity_name):
    """
    Performs aggregation for a specific time granularity (Yearly/Monthly/Daily).
    """
    print(f"  > Aggregating {granularity_name}...")
    start_t = time.time()
    
    # 1. Strict (Conf == 1.0)
    print("    - Strict...")
    strict_df = (
        df.filter(pl.col("confidence") == 1.0)
          .group_by(group_cols)
          .agg(get_aggregation_exprs("strict_"))
    )
    
    # 2. High (Conf >= 0.8)
    print("    - High...")
    high_df = (
        df.filter(pl.col("confidence") >= 0.8)
          .group_by(group_cols)
          .agg(get_aggregation_exprs("high_"))
    )
    
    # 3. Medium (Conf >= 0.5)
    print("    - Medium...")
    medium_df = (
        df.filter(pl.col("confidence") >= 0.5)
          .group_by(group_cols)
          .agg(get_aggregation_exprs("medium_"))
    )
    
    # 4. All (Weighted included)
    print("    - All...")
    all_df = (
        df.group_by(group_cols)
          .agg(get_aggregation_exprs("all_", include_weighted=True))
    )
    
    # Join all branches
    # Base is All_DF because it covers everything
    print("    - Joining...")
    final_df = all_df.join(medium_df, on=group_cols, how="left") \
                     .join(high_df, on=group_cols, how="left") \
                     .join(strict_df, on=group_cols, how="left")
    
    # Sort for cleaner output
    final_df = final_df.sort(group_cols)
    
    # Save
    print(f"    - Saving to {output_path}...")
    final_df.write_parquet(output_path)
    print(f"  > Done {granularity_name} ({time.time() - start_t:.2f}s)")

def main():
    os.makedirs(OUTPUT_ROOT, exist_ok=True)
    
    print(f"Loading data for {TEST_YEAR}...")
    file_pattern = os.path.join(INPUT_ROOT, str(TEST_YEAR), "*.parquet")
    files = glob.glob(file_pattern)
    print(f"Found {len(files)} files.")
    
    # Load Data
    # Use scan_parquet for lazy evaluation optimization
    # But for joining multiple group-bys, sometimes collect() first is faster if RAM allows.
    # Given 900GB RAM node assumption, we can likely collect.
    # However, let's try Lazy first. If filtering is pushed down, it's efficient.
    # Wait, re-scanning for each branch might be IO intensive.
    # Better to read into memory ONCE since we have plenty of RAM.
    
    q = pl.scan_parquet(files)
    
    # Ensure Date parsing
    q = q.with_columns([
        pl.col("date").str.to_datetime().alias("datetime_val"), # Convert string to datetime
    ])
    q = q.with_columns([
        pl.col("datetime_val").dt.year().alias("year"),
        pl.col("datetime_val").dt.month().alias("month"),
        pl.col("datetime_val").dt.day().alias("day_num"), # avoid name collision with day string
        pl.col("datetime_val").cast(pl.Date).alias("date_val") # Ensure pure date type
    ])
    
    # Collect into memory (assuming High Mem node available, or Year 2022 fits in RAM)
    # 2022 is big. If this script runs on login node, might OOM.
    # Suggest running this via sbatch or ensuring interactive session has mem.
    print("Collecting dataframe into memory...")
    df = q.collect()
    print(f"Loaded {len(df)} rows.")
    
    # 1. Yearly Aggregation
    process_granularity(
        df, 
        ["GEOID20", "year"], 
        os.path.join(OUTPUT_ROOT, f"yearly_{TEST_YEAR}.parquet"), 
        "Yearly"
    )
    
    # 2. Monthly Aggregation
    process_granularity(
        df, 
        ["GEOID20", "year", "month"], 
        os.path.join(OUTPUT_ROOT, f"monthly_{TEST_YEAR}.parquet"), 
        "Monthly"
    )
    
    # 3. Daily Aggregation
    process_granularity(
        df, 
        ["GEOID20", "date_val"], 
        os.path.join(OUTPUT_ROOT, f"daily_{TEST_YEAR}.parquet"), 
        "Daily"
    )

if __name__ == "__main__":
    main()
