#!/usr/bin/env python
"""
Combine Validation Metrics into Single Parquet (Optimized)
==========================================================
Combines validation_metrics_{year}.parquet files into a single
'all_years_tweet_count_with_pop_CR.parquet' file using Polars Streaming.
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

    queries = []
    for f in files:
        # Extract year from filename: validation_metrics_2012.parquet
        basename = os.path.basename(f)
        try:
            year = int(basename.split("_")[-1].split(".")[0])
        except ValueError:
            print(f"Skipping {basename} (cannot parse year)")
            continue

        # Create lazy query
        q = pl.scan_parquet(f).with_columns(pl.lit(year).alias("year"))
        queries.append(q)

    if not queries:
        print("No valid files found.")
        return

    print(f"Concatenating {len(queries)} years...")
    combined_q = pl.concat(queries)

    print(f"Streaming to {out_file}...")
    # sink_parquet uses streaming engine to write directly to disk without loading everything to RAM
    combined_q.sink_parquet(out_file)
    print("Done.")


if __name__ == "__main__":
    main()
