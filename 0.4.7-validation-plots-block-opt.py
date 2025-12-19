#!/usr/bin/env python
"""
Validation Plots (Block Level) - Optimized
==========================================
Generates Lorenz Curves and Histograms for Census Block level tweet coverage.
Uses Polars Lazy API to avoid OOM.
"""

import json
import os

import matplotlib.pyplot as plt
import numpy as np
import polars as pl


def plot_lorenz(df_pl, value_col, weight_col, year, out_dir):
    """
    Plot Lorenz Curve for a specific year.
    df_pl: LazyFrame or DataFrame
    """
    # Collect only necessary columns
    print("  Collecting data for Lorenz...")
    df = df_pl.select([value_col, weight_col]).collect()

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


def plot_hist(df_pl, cr_col, year, out_dir):
    """
    Plot Histogram of Log2(Coverage Ratio).
    """
    print("  Collecting data for Histogram...")
    # Filter valid CR > 0 and finite before collecting
    vals = (
        df_pl.select(cr_col)
        .filter((pl.col(cr_col) > 0) & (pl.col(cr_col).is_finite()))
        .collect()
        .to_series()
        .log(base=2)
    )

    # Filter log results for finite as well (just in case)
    vals = vals.filter(vals.is_finite())

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

    print(f"Scanning {in_file}...")
    q_all = pl.scan_parquet(in_file)

    # Process all years
    years = range(2012, 2024)

    for year in years:
        print(f"Processing Year {year}...")
        q_year = q_all.filter(pl.col("year") == year)

        # Plot Lorenz (Strict Tier)
        plot_lorenz(q_year, "strict_tweet_count", "population", year, out_dir)

        # Plot Histogram (Strict Tier)
        plot_hist(q_year, "strict_cr", year, out_dir)


if __name__ == "__main__":
    main()
