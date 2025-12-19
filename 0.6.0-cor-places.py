#!/usr/bin/env python
"""
Correlation with CDC PLACES (Tract Level)
=========================================
Correlates Tweet Sentiment (Strict Tier) with Mental Health outcomes (MHLTH) from CDC PLACES.
Uses aggregated Tract-level sentiment data.
Maps Release Year to correct Data Year.

Input:
- Sentiment: data/all_years_tract_summary.parquet
- Health: data/500-Cities-Places/PLACES__Census_Tract_Data__GIS_Friendly_Format___{year}_release.csv

Output:
- outputs/correlation/scatter_sent_vs_MHLTH_{year}.png
- outputs/correlation/places_correlation_summary.csv
"""

import glob
import json
import os

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import polars as pl


def weighted_corr(x, y, w):
    """Weighted Pearson Correlation"""
    mean_x = np.average(x, weights=w)
    mean_y = np.average(y, weights=w)
    cov = np.average((x - mean_x) * (y - mean_y), weights=w)
    var_x = np.average((x - mean_x) ** 2, weights=w)
    var_y = np.average((y - mean_y) ** 2, weights=w)
    return cov / np.sqrt(var_x * var_y)


def decile_curve(df, x_col, y_col, w_col, q=10):
    """Decile binning curve"""
    df = df.copy()
    df["bin"] = pd.qcut(df[x_col], q=q, duplicates="drop")

    curve = (
        df.groupby("bin", observed=True)
        .apply(
            lambda g: pd.Series(
                {
                    "x_med": g[x_col].median(),
                    "y_mean": np.average(g[y_col], weights=g[w_col]),
                }
            )
        )
        .reset_index(drop=True)
        .sort_values("x_med")
    )

    return curve


def main():
    # Load config
    with open("setting.json") as f:
        config = json.load(f)

    workspace = config["workspace"]
    places_dir = config["places_data"]
    out_dir = os.path.join(config["outputs_dir"], "correlation")
    os.makedirs(out_dir, exist_ok=True)

    # Load Sentiment Data
    sent_path = os.path.join(workspace, "data", "all_years_tract_summary.parquet")
    print(f"Loading Sentiment from {sent_path}...")
    sent_df_all = pl.read_parquet(sent_path)

    # Map Release Year -> Data Year
    # Based on standard CDC PLACES lag (2 year) or file content
    release_map = {
        2024: 2022,  # Default fallback
        2023: 2021,
        2022: 2020,
        2021: 2019,
        2020: 2018,
    }

    files = glob.glob(os.path.join(places_dir, "PLACES*_release.csv"))
    files.sort()

    results = []

    for places_file in files:
        basename = os.path.basename(places_file)
        try:
            # Extract Release Year: PLACES..._2024_release.csv
            release_year = int(basename.split("_")[-2])
        except (IndexError, ValueError):
            print(f"Skipping {basename} (cannot parse year)")
            continue

        print(f"\nProcessing Release {release_year} ({basename})...")

        # Determine Data Year
        data_year = release_map.get(release_year)

        # Special check for Year column in file (Long Format often has it)
        is_long = False

        # Read Header
        try:
            temp = pd.read_csv(places_file, nrows=1)
        except Exception as e:
            print(f"  Error reading file: {e}")
            continue

        cols = temp.columns.tolist()

        if "Year" in cols:
            # Use the year from the file if available
            file_year = int(temp["Year"].iloc[0])
            print(f"  Detected Data Year from file: {file_year}")
            data_year = file_year
            is_long = True

        if data_year is None:
            print(f"  Unknown data year for release {release_year}. Skipping.")
            continue

        print(f"  Matching with Sentiment Year: {data_year}")

        # Load PLACES Data
        if is_long or ("LocationName" in cols and "MeasureId" in cols):
            # Long Format
            print("  Format: Long")
            places_df = pd.read_csv(
                places_file,
                dtype={"LocationName": str},
                usecols=["LocationName", "MeasureId", "Data_Value", "TotalPopulation"],
            )
            # Filter for MHLTH
            places_df = places_df[places_df["MeasureId"] == "MHLTH"]

            places_df = places_df.rename(
                columns={
                    "LocationName": "GEOID20_tract",
                    "Data_Value": "mhlth",
                    "TotalPopulation": "pop",
                }
            )
            places_df = places_df[["GEOID20_tract", "mhlth", "pop"]]

        elif "TractFIPS" in cols:
            # Wide Format
            print("  Format: Wide")
            places_df = pd.read_csv(
                places_file,
                dtype={"TractFIPS": str},
                usecols=["TractFIPS", "MHLTH_CrudePrev", "TotalPopulation"],
            )
            places_df = places_df.rename(
                columns={
                    "TractFIPS": "GEOID20_tract",
                    "MHLTH_CrudePrev": "mhlth",
                    "TotalPopulation": "pop",
                }
            )
        else:
            print("  Unknown format. Skipping.")
            continue

        # Filter Sentiment for Data Year
        sent_df = sent_df_all.filter(pl.col("year") == data_year).to_pandas()

        if sent_df.empty:
            print(f"  No sentiment data for {data_year}.")
            continue

        # Merge
        # Inner join to keep only matching tracts
        merged = pd.merge(sent_df, places_df, on="GEOID20_tract", how="inner")

        # Filter valid data
        original_len = len(merged)
        merged = merged[
            (merged["strict_tweet_count"] >= 20)
            & (merged["strict_sentiment_mean"].notna())
            & (merged["mhlth"].notna())
            & (merged["pop"] > 0)
        ]

        print(f"  Matched Tracts: {len(merged)} (filtered from {original_len})")

        if len(merged) < 100:
            print("  Not enough data for correlation.")
            continue

        # Correlation
        pearson = merged["strict_sentiment_mean"].corr(merged["mhlth"])
        pearson_w = weighted_corr(
            merged["strict_sentiment_mean"], merged["mhlth"], merged["pop"]
        )

        print(f"  Pearson: {pearson:.3f}, Weighted: {pearson_w:.3f}")

        results.append(
            {
                "release_year": release_year,
                "data_year": data_year,
                "N": len(merged),
                "pearson": pearson,
                "pearson_w": pearson_w,
            }
        )

        # Plot
        curve = decile_curve(merged, "mhlth", "strict_sentiment_mean", "pop")

        fig, ax = plt.subplots(figsize=(6, 5))
        # Scatter (Sample)
        sample = merged.sample(min(len(merged), 10000))
        ax.scatter(
            sample["mhlth"],
            sample["strict_sentiment_mean"],
            alpha=0.1,
            s=2,
            color="gray",
        )

        # Curve
        ax.plot(
            curve["x_med"], curve["y_mean"], "r-", lw=2, label="Decile Mean (Weighted)"
        )

        ax.set_xlabel("Frequent Mental Distress (%)")
        ax.set_ylabel("Tweet Sentiment (Strict)")
        ax.set_title(f"Sentiment ({data_year}) vs Mental Health ({release_year} Rel)")
        ax.legend()
        ax.grid(alpha=0.3)

        out_png = os.path.join(
            out_dir, f"scatter_sent_{data_year}_vs_MHLTH_{release_year}rel.png"
        )
        fig.savefig(out_png, dpi=300)
        plt.close(fig)

    # Save Summary
    if results:
        sum_df = pd.DataFrame(results)
        sum_file = os.path.join(out_dir, "places_correlation_summary.csv")
        sum_df.to_csv(sum_file, index=False)
        print(f"\nSummary saved to {sum_file}")


if __name__ == "__main__":
    main()
