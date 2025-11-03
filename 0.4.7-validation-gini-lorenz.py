#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Compute Lorenz curve, Gini, and summary stats for tract-level tweet coverage.

Inputs:
  - Parquet with columns: GEOID20, T_i (tweets), P_i (population),
    CR, log2CR, mask_low_coverage  (this is your *_CR.parquet from DuckDB)

Outputs:
  - lorenz_points.csv
  - lorenz_curve.png
  - prints: covered_pop_share, Gini, P10/P50/P90 of log2CR (on unmasked tracts)
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
import json

# Load configuration
with open('setting.json') as f:
    config = json.load(f)

# ---------- paths ----------
IN = Path(config["workspace"]) / "data/all_years_tweet_count_with_pop_CR.parquet"
OUT_DIR = Path(config["outputs_dir"]) / "gini"; OUT_DIR.mkdir(exist_ok=True, parents=True)

# ---------- load ----------
df = pd.read_parquet(IN)
# keep valid population
df = df[df["P_i"] > 0].copy()

# if CR/log2CR not present, compute them
if "CR" not in df.columns or "log2CR" not in df.columns:
    T_tot = df["T_i"].sum()
    P_tot = df["P_i"].sum()
    df["CR"] = (df["T_i"]/T_tot) / (df["P_i"]/P_tot)
    df["log2CR"] = np.log2(df["CR"])

# split by mask (sample adequacy)
reportable = df[df["mask_low_coverage"] == 0].copy()
suppressed  = df[df["mask_low_coverage"] == 1].copy()

# ---------- summary stats ----------
covered_pop_share = reportable["P_i"].sum() / df["P_i"].sum()
p10, p50, p90 = np.quantile(reportable["log2CR"].dropna(), [0.10, 0.50, 0.90])

# ---------- Lorenz (on reportable tracts) ----------
# sort by per-capita tweets
reportable["t_per_cap"] = reportable["T_i"] / reportable["P_i"]
reportable = reportable.sort_values("t_per_cap", kind="mergesort")  # stable sort

T_cov = reportable["T_i"].sum()
P_cov = reportable["P_i"].sum()

x = reportable["P_i"].cumsum().to_numpy() / P_cov   # cumulative population share
y = reportable["T_i"].cumsum().to_numpy() / T_cov   # cumulative tweets share

# prepend origin (0,0) for a proper Lorenz polygon
x = np.concatenate([[0.0], x])
y = np.concatenate([[0.0], y])

# trapezoid rule for area under Lorenz; Gini = 1 - 2*area
area = np.trapz(y, x)                 # same as sum(0.5*(x_i-x_{i-1})*(y_i+y_{i-1}))
gini = 1.0 - 2.0*area

# save Lorenz points
pd.DataFrame({"cum_pop_share": x, "cum_tweet_share": y}).to_csv(OUT_DIR/"lorenz_points.csv", index=False)

# ---------- plot ----------
fig, ax = plt.subplots(figsize=(5.5, 5.0), constrained_layout=True)
ax.plot(x, y, lw=2, label="Lorenz (tweets vs population)")
ax.plot([0,1], [0,1], "k--", lw=1, label="Equality line")
ax.set_xlabel("Cumulative population share")
ax.set_ylabel("Cumulative tweets share")
ax.set_xlim(0,1); ax.set_ylim(0,1)
ax.grid(alpha=0.2)
ax.legend(loc="upper left")
ax.text(0.60, 0.08, f"Gini = {gini:.3f}", fontsize=11,
        bbox=dict(boxstyle="round,pad=0.2", fc="white", ec="0.8"))
fig.savefig(OUT_DIR/"lorenz_curve.png", dpi=300)

# ---------- print summary ----------
print("\n=== Technical Validation (coverage/representativeness) ===")
print(f"Tracts total       : {len(df):,}")
print(f"Tracts reportable  : {len(reportable):,} ({len(reportable)/len(df):.1%} of tracts)")
print(f"Covered pop. share : {covered_pop_share:.3%}")          # population still covered after masking
print(f"Gini (reportable)  : {gini:.3f}")                       # inequality of tweet allocation vs population
print(f"log2(CR) quantiles : P10={p10:.2f}, P50={p50:.2f}, P90={p90:.2f}  (reportable)")
print(f"Outputs saved to   : {OUT_DIR.resolve()}")
