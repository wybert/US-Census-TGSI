#!/usr/bin/env python
"""
Aggregate Blocks to Tracts (Full Metrics)
=========================================
Aggregates Census Block level tweet/sentiment data to Census Tract level.
Reads original aggregation output to ensure Sentiment columns are included.

Input:
- Population: data/census_pop/pop data/*.parquet
- Sentiment: data/aggregated_sentiment_stats/yearly_YYYY.parquet

Output: data/all_years_tract_summary.parquet
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
    pop_dir = config["census_pop"]
    agg_dir = os.path.join(workspace, "data", "aggregated_sentiment_stats")
    out_file = os.path.join(workspace, "data", "all_years_tract_summary.parquet")

    print("Loading Population Data...")
    pop_files = glob.glob(os.path.join(pop_dir, "*.parquet"))
    # Load all pop files
    pop_df = pl.read_parquet(pop_files, columns=["GEOID20", "population"])
    pop_df = pop_df.select([pl.col("GEOID20"), pl.col("population").cast(pl.Int64)])

    # Create Tract ID map (Block -> Tract)
    # This avoids doing str slice repeatedly in the loop if we join
    # But doing it on 8M rows once is fine.
    pop_df = pop_df.with_columns(
        pl.col("GEOID20").str.slice(0, 11).alias("GEOID20_tract")
    )

    print(f"Population loaded: {pop_df.height} blocks.")

    # Helper to aggregate one year
    def process_year(year):
        agg_file = os.path.join(agg_dir, f"yearly_{year}.parquet")
        if not os.path.exists(agg_file):
            print(f"Skipping {year} (no file)")
            return None

        print(f"Processing {year}...")

        # Load Sentiment Data (Lazy)
        q = pl.scan_parquet(agg_file)

        # Join with Pop (Left Join on Sentiment to keep only blocks with data?
        # Or Right Join on Pop to keep all tracts?
        # Usually for correlation we need data.
        # But for map coverage we might want all tracts.
        # Let's start with inner join logic (only populated/active blocks)
        # but actually we need Population for CR.
        # So we join Pop (base) with Sentiment (left join).

        # We need to aggregate Population regardless of tweets.
        # But pop_df is 8M rows.
        # Efficient approach: Aggregate Pop to Tracts ONCE first.
        # Then Aggregate Tweets to Tracts.
        # Then Join.

        return None

    # Optimized Approach:
    # 1. Agg Pop to Tracts
    print("Aggregating Population to Tracts...")
    tract_pop = pop_df.group_by("GEOID20_tract").agg(pl.col("population").sum())

    # 2. Iterate Years, Agg Tweets to Tracts, then Join
    yearly_results = []

    tiers = ["strict", "high", "medium", "all"]

    for year in range(2012, 2024):
        agg_file = os.path.join(agg_dir, f"yearly_{year}.parquet")
        if not os.path.exists(agg_file):
            continue

        print(f"Processing Year {year}...")
        q = pl.scan_parquet(agg_file)

        # Add Tract ID
        q = q.with_columns(pl.col("GEOID20").str.slice(0, 11).alias("GEOID20_tract"))

        # Aggregate Expressions
        exprs = [pl.len().alias("block_count_with_tweets")]

        for tier in tiers:
            # Sum Counts
            exprs.append(pl.col(f"{tier}_tweet_count").sum())
            exprs.append(pl.col(f"{tier}_user_count").sum())

            # Weighted Sentiment Mean
            # Weighted Sum / Total Count
            # We assume {tier}_sentiment_mean exists.

            w_sum = (
                (pl.col(f"{tier}_sentiment_mean") * pl.col(f"{tier}_tweet_count"))
                .fill_null(0)
                .sum()
            )
            w_count = pl.col(f"{tier}_tweet_count").sum()

            exprs.append((w_sum / w_count).alias(f"{tier}_sentiment_mean"))

        # Agg Tweets
        tract_tweets = q.group_by("GEOID20_tract").agg(exprs).collect()

        # Join with Tract Pop
        # Outer join? No, we need Pop for CR. Left join on Tract Pop is safest (keeps all tracts).
        # But we only care about years where we have data?
        # If we want a complete dataset, we need all tracts.

        merged = tract_pop.join(tract_tweets, on="GEOID20_tract", how="left")

        # Fill nulls for tweet counts with 0
        fill_cols = [f"{t}_tweet_count" for t in tiers] + [
            f"{t}_user_count" for t in tiers
        ]
        merged = merged.with_columns(
            [pl.col(c).fill_null(0) for c in fill_cols if c in merged.columns]
        )

        # Calculate CR
        merged = merged.with_columns(pl.lit(year).alias("year"))

        # Add to results
        yearly_results.append(merged)

    print("Concatenating all years...")
    final_df = pl.concat(yearly_results)

    # Calculate CR (using yearly totals)
    print("Calculating Tract CRs...")
    for tier in tiers:
        t_col = f"{tier}_tweet_count"
        cr_col = f"{tier}_cr"

        final_df = final_df.with_columns(
            [
                pl.col(t_col).sum().over("year").alias(f"{tier}_total_tweets"),
                pl.col("population").sum().over("year").alias("total_pop"),
            ]
        )

        final_df = final_df.with_columns(
            (
                (pl.col(t_col) / pl.col(f"{tier}_total_tweets"))
                / (pl.col("population") / pl.col("total_pop"))
            ).alias(cr_col)
        ).drop([f"{tier}_total_tweets", "total_pop"])

    print(f"Saving to {out_file}...")
    final_df.write_parquet(out_file)
    print("Done.")


if __name__ == "__main__":
    main()
