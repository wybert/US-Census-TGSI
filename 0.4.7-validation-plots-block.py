#!/usr/bin/env python
"""
Validation Plots (Block Level)
==============================
Generates Lorenz Curves and Histograms for Census Block level tweet coverage.
Adapted from 0.4.7 and 0.4.4.

Input: data/all_years_tweet_count_with_pop_CR.parquet
Output: outputs/gini/
"""

import json
import os

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import polars as pl

def plot_lorenz(df, value_col, weight_col, year, out_dir):
    """
    Plot Lorenz Curve for a specific year.
    """
    # Filter valid weights
    df = df.filter(pl.col(weight_col) > 0)

    if df.height == 0:
        print(f"No data for year {year}")
        return

    # Sort by ratio (density)
    df = df.with_columns((pl.col(value_col) / pl.col(weight_col)).alias("ratio"))
    df = df.sort("ratio")

    # Cumulative
    cum_pop = df[weight_col].cum_sum() / df[weight_col].sum()
    cum_val = df[value_col].cum_sum() / df[value_col].sum()

    # Prepend 0
    x = np.concatenate([[0.0], cum_pop.to_numpy()])
    y = np.concatenate([[0.0], cum_val.to_numpy()])

    # Gini
    area = np.trapezoid(y, x)
    gini = 1.0 - 2.0 * area

    # Plot
    fig, ax = plt.subplots(figsize=(6, 6))
    ax.plot(x, y, lw=2, label=f"Lorenz (Gini={gini:.3f})")
    ax.plot([0, 1], [0, 1], "k--", lw=1, label="Equality")
    ax.set_xlabel("Cumulative Population Share")
    ax.set_ylabel("Cumulative Tweet Share")
    ax.set_title(f"Lorenz Curve - Year {year}")
    ax.legend()
    ax.grid(alpha=0.3)

    out_file = os.path.join(out_dir, f"lorenz_curve_{year}.png")
    fig.savefig(out_file, dpi=300)
    plt.close(fig)
    print(f"Saved {out_file} (Gini: {gini:.4f})")


def plot_hist(df, cr_col, year, out_dir):
    """
    Plot Histogram of Log2(Coverage Ratio).
    """
    # Filter where we have a valid CR (count > 0 to avoid log(0))
    # Or handle log(0) by ignoring or setting to min

    # Calculate CR if not present or use existing
    # We assume 'strict_cr' is raw ratio

    # Take log2, handle zeros
    # Replace 0 with a very small number or filter?
    # Usually we plot dist of existing coverage.

    vals = df.select(pl.col(cr_col)).filter(pl.col(cr_col) > 0).to_series().log(base=2)

    if len(vals) == 0:
        return

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.hist(vals, bins=50, color="skyblue", edgecolor="black", alpha=0.7)
    ax.set_xlabel("Log2(Coverage Ratio)")
    ax.set_ylabel("Frequency (Blocks)")
    ax.set_title(f"Log2(CR) Distribution - Year {year}")
    ax.grid(alpha=0.3)

    out_file = os.path.join(out_dir, f"hist_log2cr_{year}.png")
    fig.savefig(out_file, dpi=300)
    plt.close(fig)
    print(f"Saved {out_file}")


def main():
    # Load config
    with open("setting.json") as f:
        config = json.load(f)

    workspace = config["workspace"]
    in_file = os.path.join(
        workspace, "data", "all_years_tweet_count_with_pop_CR.parquet"
    )
    out_dir = os.path.join(config["outputs_dir"], "validation")
    os.makedirs(out_dir, exist_ok=True)

    print(f"Loading {in_file}...")
    df_all = pl.read_parquet(in_file)

    # Process 2020 as primary example, or all
    years = [2020]

    for year in years:
        print(f"Processing Year {year}...")
        df_year = df_all.filter(pl.col("year") == year)

        if df_year.height == 0:
            print(f"No data for {year}")
            continue

        # Plot Lorenz (Strict Tier)
        plot_lorenz(df_year, "strict_tweet_count", "population", year, out_dir)

        # Plot Histogram (Strict Tier)
        plot_hist(df_year, "strict_cr", year, out_dir)

        # Plot Histogram (Strict Tier)
        plot_hist(df_year, "strict_cr", year, out_dir)

if __name__ == "__main__":
    main()
