#!/usr/bin/env python
"""
Generate Spatial Maps (Census Blocks) - Optimized
=================================================
Generates maps for Census Block level tweet coverage/sentiment.
Uses per-year parquet files to avoid OOM.
"""

import json
import os
import warnings

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.patches import Patch

# Suppress warnings
warnings.filterwarnings("ignore")


def main():
    # Load config
    with open("setting.json") as f:
        config = json.load(f)

    workspace = config["workspace"]
    in_dir = os.path.join(workspace, "data", "spatial_representation")
    out_dir = os.path.join(config["outputs_dir"], "validation")
    os.makedirs(out_dir, exist_ok=True)

    # Process 2020 as primary example (or loop all)
    years = [2020]

    for year in years:
        in_file = os.path.join(
            in_dir, f"census_blocks_merged_shifted_geo_{year}.parquet"
        )
        if not os.path.exists(in_file):
            print(f"Skipping {year} (No file)")
            continue

        print(f"Loading {in_file}...")
        # Load GeoDataFrame
        try:
            gdf = gpd.read_parquet(in_file)
        except Exception as e:
            print(f"Error reading {in_file}: {e}")
            continue

        print(f"Loaded {len(gdf)} blocks. Projecting...")
        # Project to Albers for mapping
        gdf = gdf.to_crs(5070)

        # Calculate Log2 CR if not present
        # We need CR column.
        # Check columns
        # strict_cr might be present.

        cr_col = "strict_cr"
        if cr_col not in gdf.columns:
            print(f"Column {cr_col} not found. Available: {gdf.columns[:10]}...")
            continue

        # Create Log2 CR
        # Handle zeros/nulls
        gdf["log2CR"] = np.log2(gdf[cr_col].replace(0, np.nan))

        # Mask low coverage (e.g. strict_tweet_count < 20)
        # If strict_tweet_count is null, it's low coverage
        count_col = "strict_tweet_count"
        if count_col in gdf.columns:
            gdf["mask"] = np.where(gdf[count_col] >= 20, 0, 1)
        else:
            gdf["mask"] = 1  # Default masked

        # Plot
        print("Plotting...")

        fig, ax = plt.subplots(figsize=(12, 8))

        # 1. Plot Background (Masked)
        masked = gdf[gdf["mask"] == 1]
        if not masked.empty:
            masked.plot(ax=ax, color="#D9D9D9", linewidth=0)

        # 2. Plot Data
        valid = gdf[gdf["mask"] == 0]
        if not valid.empty:
            # Classification
            bins = [-2, -1, -0.5, 0.5, 1, 2]
            labels = [
                "≤0.25×",
                "0.25–0.5×",
                "0.5–0.71×",
                "0.71–1.41×",
                "1.41–2×",
                "2–4×",
                ">4×",
            ]

            valid.plot(
                column="log2CR",
                cmap="Spectral_r",
                scheme="UserDefined",
                classification_kwds={"bins": bins},
                linewidth=0,
                ax=ax,
                legend=True,
                legend_kwds={
                    "title": "log\u2082(CR)\n(\u22121=0.5×, 0=1×, +1=2×)",
                    "labels": labels,
                    "frameon": False,
                    "loc": "lower right",
                },
            )

        ax.set_axis_off()
        ax.set_title(f"Tweet Coverage Representation (Block Level) - {year}")

        # Custom Legend for Masked
        # Get existing legend
        leg1 = ax.get_legend()

        masked_patch = Patch(
            facecolor="#D9D9D9", edgecolor="none", label="Masked (<20 tweets)"
        )
        # Create second legend
        leg2 = ax.legend(handles=[masked_patch], loc="lower left", frameon=False)

        if leg1:
            ax.add_artist(leg1)

        out_png = os.path.join(out_dir, f"log2CR_map_{year}.png")
        fig.savefig(out_png, dpi=300, bbox_inches="tight")
        plt.close(fig)
        print(f"Saved {out_png}")


if __name__ == "__main__":
    main()
