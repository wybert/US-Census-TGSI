#!/usr/bin/env python
"""
Aggregate Sentiment to Census Block Level - All Years
=====================================================
Aggregates tweet-level sentiment data to Census Block (GEOID20) level for all specified years.
Calculates rich statistics and confidence-weighted metrics.

Input: tweets_with_census_blocks_confidence/YYYY/YYYY_MM.parquet
Output: aggregated_sentiment_stats/

Metrics:
- Counts: tweet_count, user_count (unique)
- Sentiment: mean, std, min, max, quantiles
- Weighted Sentiment: sum(sentiment * confidence) / sum(confidence)
- Confidence: mean_confidence

Author: Production version of 0.4.1 test script
"""

import polars as pl
import os
import json
import glob
import argparse
import time

def get_aggregation_exprs(prefix="", include_weighted=False):
    """
    Returns a list of Polars expressions for aggregation.
    """
    exprs = [
        pl.len().alias(f"{prefix}tweet_count"),
        pl.col("user_id").n_unique().alias(f"{prefix}user_count"),
        
        # Sentiment Statistics (Raw)
        pl.col("sentiment").mean().alias(f"{prefix}sentiment_mean"),
        pl.col("sentiment").std().alias(f"{prefix}sentiment_std"),
        pl.col("sentiment").min().alias(f"{prefix}sentiment_min"),
        pl.col("sentiment").max().alias(f"{prefix}sentiment_max"),
        
        # Sentiment Quantiles
        pl.col("sentiment").quantile(0.10).alias(f"{prefix}sentiment_10q"),
        pl.col("sentiment").quantile(0.25).alias(f"{prefix}sentiment_25q"),
        pl.col("sentiment").median().alias(f"{prefix}sentiment_50q"),
        pl.col("sentiment").quantile(0.75).alias(f"{prefix}sentiment_75q"),
        pl.col("sentiment").quantile(0.90).alias(f"{prefix}sentiment_90q"),
        
        # Confidence Stats
        pl.col("confidence").mean().alias(f"{prefix}confidence_mean")
    ]
    
    if include_weighted:
        # Weighted Mean = Sum(Sentiment * Conf) / Sum(Conf)
        weighted_mean = (pl.col("sentiment") * pl.col("confidence")).sum() / pl.col("confidence").sum()
        exprs.append(weighted_mean.alias(f"{prefix}weighted_sentiment_mean"))
        
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

def load_data_for_year(year, base_path):
    """Load all parquet files for a specific year."""
    file_pattern = os.path.join(base_path, str(year), "*.parquet")
    files = glob.glob(file_pattern)
    
    if not files:
        print(f"No files found for year {year}")
        return None
        
    print(f"Loading {len(files)} files for year {year}...")
    
    # Scan parquet files (lazy loading)
    return pl.scan_parquet(files)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--start-year', type=int, default=2012)
    parser.add_argument('--end-year', type=int, default=2023)
    args = parser.parse_args()

    # Load configuration
    with open('setting.json') as f:
        config = json.load(f)

    input_path = config['tweets_with_census_blocks_confidence']
    # Production output directory
    output_dir = os.path.join(config['workspace'], "data", "aggregated_sentiment_stats")
    os.makedirs(output_dir, exist_ok=True)

    print(f"Input: {input_path}")
    print(f"Output: {output_dir}")
    print(f"Years: {args.start_year}-{args.end_year}")

    total_start = time.time()

    for year in range(args.start_year, args.end_year + 1):
        year_start = time.time()
        print(f"\n{'='*40}\nProcessing Year: {year}\n{'='*40}")
        
        try:
            q = load_data_for_year(year, input_path)
            if q is None:
                continue
            
            # Ensure Date parsing and type casting
            q = q.with_columns([
                pl.col("date").str.to_datetime().alias("datetime_val"),
                pl.col("confidence").cast(pl.Float64),
                pl.col("sentiment").cast(pl.Float64)
            ])
            q = q.with_columns([
                pl.col("datetime_val").dt.year().alias("year"),
                pl.col("datetime_val").dt.month().alias("month"),
                pl.col("datetime_val").dt.day().alias("day_num"),
                pl.col("datetime_val").cast(pl.Date).alias("date_val")
            ])
            
            # Collect into memory
            print("Collecting dataframe into memory...")
            df = q.collect()
            print(f"Loaded {len(df)} rows.")
            
            # 1. Yearly Aggregation
            process_granularity(
                df, 
                ["GEOID20", "year"], 
                os.path.join(output_dir, f"yearly_{year}.parquet"), 
                "Yearly"
            )
            
            # 2. Monthly Aggregation
            process_granularity(
                df, 
                ["GEOID20", "year", "month"], 
                os.path.join(output_dir, f"monthly_{year}.parquet"), 
                "Monthly"
            )
            
            # 3. Daily Aggregation
            process_granularity(
                df, 
                ["GEOID20", "date_val"], 
                os.path.join(output_dir, f"daily_{year}.parquet"), 
                "Daily"
            )
            
            print(f"Year {year} processed in {time.time() - year_start:.2f}s")
            
        except Exception as e:
            print(f"✗ Error processing year {year}: {e}")
            import traceback
            traceback.print_exc()

    print(f"\nTotal processing time: {time.time() - total_start:.2f}s")

if __name__ == "__main__":
    main()
