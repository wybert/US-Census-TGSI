#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
For each year, join tract-level sentiment with CDC PLACES (tract GIS-friendly wide),
compute Spearman/Pearson correlations (unweighted & population-weighted),
and plot scatter + decile curve. Saves per-year PNGs and a CSV summary.

Edit the PATHS below to your files.
"""

import argparse
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from scipy import stats
import json

parser = argparse.ArgumentParser()
parser.add_argument("--tier", choices=["all", "medium", "high", "strict"], default="all",
                     help="Confidence tier of the joined tract-year data to correlate (all/medium/high/strict).")
args = parser.parse_args()
tier = args.tier
suffix = "" if tier == "all" else f"_{tier}"

# Load configuration
with open('setting.json') as f:
    config = json.load(f)

# Single source of truth: the tract-year sentiment+PLACES join produced by
# 05_prepare_correlation_tracts.py (cols: year, GEOID20_tract, tweets_year_tract,
# sent_mean_year_tract, mask_low_coverage, pop, mhlth, mammouse, release_year).
JOINED_PATH = Path(config["workspace"]) / "data" / f"sentiment_places_data_joined{suffix}.parquet"

OUT_DIR = Path(config["outputs_dir"]) / "correlation"; OUT_DIR.mkdir(parents=True, exist_ok=True)

# ========== 选用的 PLACES 指标 ==========
# 主验证：Frequent mental distress（MHLTH_CrudePrev）预期与情感均值负相关
TARGET_COL = "MHLTH_CrudePrev"
# 判别对照（可选换成 BPHIGH_CrudePrev / COLON_SCREEN_CrudePrev 等）
DISCRIM_COL = "MAMMOUSE_CrudePrev"

# ========== 工具函数 ==========
def weighted_corr(x, y, w):
    """人口加权 Pearson 相关。Drops rows where x, y, or w is NaN (np.sum on a
    pandas Series defaults to skipna=True, so without this an unfiltered NaN
    silently mismatches the numerator/denominator N instead of raising)."""
    x = pd.Series(x, dtype=float).reset_index(drop=True)
    y = pd.Series(y, dtype=float).reset_index(drop=True)
    w = pd.Series(w, dtype=float).reset_index(drop=True)
    valid = x.notna() & y.notna() & w.notna()
    x, y, w = x[valid], y[valid], w[valid]
    m_x = np.sum(w * x) / np.sum(w); m_y = np.sum(w * y) / np.sum(w)
    cov = np.sum(w * (x - m_x) * (y - m_y))
    varx = np.sum(w * (x - m_x)**2); vary = np.sum(w * (y - m_y)**2)
    return cov / np.sqrt(varx * vary)

def rank_spearman(x, y):
    """无权 Spearman（用平均秩+Pearson）。Drops NaN pairs first."""
    d = pd.DataFrame({"x": x, "y": y}).dropna()
    if len(d) < 2:
        return np.nan
    rx = d["x"].rank(method="average")
    ry = d["y"].rank(method="average")
    return np.corrcoef(rx, ry)[0, 1]

def weighted_spearman(x, y, w):
    """人口加权 Spearman（对秩做加权 Pearson）。"""
    rx = pd.Series(x).rank(method="average")
    ry = pd.Series(y).rank(method="average")
    return weighted_corr(rx, ry, w)

def decile_curve(x, y, w=None, q=10):
    """按 x 的分位数分箱，返回每箱 x 中位数与 y 的（加权）均值。"""
    df = pd.DataFrame({"x": x, "y": y})
    if w is not None:
        df["w"] = w
    # 分箱
    df = df.dropna()
    df["bin"] = pd.qcut(df["x"], q=q, duplicates="drop")
    agg = (df.groupby("bin")
           .apply(lambda g: pd.Series({
               "x_med": g["x"].median(),
               "y_mean": np.average(g["y"], weights=(g["w"] if "w" in g.columns else None))
           }))
           .sort_values("x_med")
           .reset_index(drop=True))
    return agg

# ========== 主流程 ==========
joined = pd.read_parquet(JOINED_PATH)
# Normalize to the column names the analysis below expects.
joined = joined.rename(columns={"sent_mean_year_tract": "sent_mean", "mammouse": "disc"})
years = sorted(joined["year"].unique())
rows = []

for year in years:
    print(f"\n=== {year} ===")
    df = joined[joined["year"] == year].copy()
    # 05 already applied mask_low_coverage==0 and non-null pop/mhlth; re-filter defensively.
    df = df[(df["mask_low_coverage"] == 0) & df["pop"].notna()
            & df["mhlth"].notna() & df["sent_mean"].notna()]
    if df.empty:
        print("No data after filtering; check inputs.")
        continue

    # 相关系数
    pear_u = df[["sent_mean", "mhlth"]].corr().iloc[0,1]
    spear_u = rank_spearman(df["sent_mean"], df["mhlth"])
    pear_w = weighted_corr(df["sent_mean"], df["mhlth"], df["pop"])
    spear_w = weighted_spearman(df["sent_mean"], df["mhlth"], df["pop"])

    # 判别（对照指标）
    pear_u_disc = df[["sent_mean", "disc"]].corr().iloc[0,1]
    spear_u_disc = rank_spearman(df["sent_mean"], df["disc"])
    pear_w_disc = weighted_corr(df["sent_mean"], df["disc"], df["pop"])
    spear_w_disc = weighted_spearman(df["sent_mean"], df["disc"], df["pop"])

    # Significance (unweighted) for the primary MHLTH correlation.
    r_p, p_pearson = stats.pearsonr(df["sent_mean"], df["mhlth"])
    r_s, p_spearman = stats.spearmanr(df["sent_mean"], df["mhlth"])
    # 95% CI for Pearson r via Fisher z-transform.
    n = len(df)
    z = np.arctanh(r_p); se = 1.0 / np.sqrt(n - 3)
    ci_lo, ci_hi = np.tanh(z - 1.96 * se), np.tanh(z + 1.96 * se)

    print(f"N={n:,}   Pearson={pear_u:.3f} (w={pear_w:.3f}) p={p_pearson:.2e} CI=[{ci_lo:.3f},{ci_hi:.3f}]   "
          f"Spearman={spear_u:.3f} (w={spear_w:.3f}) p={p_spearman:.2e}")
    print(f"Discriminant → Pearson={pear_u_disc:.3f} (w={pear_w_disc:.3f})   Spearman={spear_u_disc:.3f} (w={spear_w_disc:.3f})")

    rows.append({
        "year": year, "N": n,
        "pearson": pear_u, "pearson_w": pear_w,
        "pearson_p": p_pearson, "pearson_ci_lo": ci_lo, "pearson_ci_hi": ci_hi,
        "spearman": spear_u, "spearman_w": spear_w, "spearman_p": p_spearman,
        "pearson_disc": pear_u_disc, "pearson_disc_w": pear_w_disc,
        "spearman_disc": spear_u_disc, "spearman_disc_w": spear_w_disc
    })

    # —— 散点 + 十等分曲线 —— #
    # 为降低绘图负担，可抽样 100k（如需要）
    plot_df = df.sample(n=min(len(df), 100_000), random_state=42)
    curve = decile_curve(df["mhlth"], df["sent_mean"], w=df["pop"], q=10)

    fig, ax = plt.subplots(figsize=(6.2, 5.4))
    ax.scatter(plot_df["mhlth"], plot_df["sent_mean"], s=3, alpha=0.15)  # 默认配色，避免指定颜色
    ax.plot(curve["x_med"], curve["y_mean"], lw=2)                       # 十等分平滑曲线（人口加权）
    ax.set_xlabel("Frequent mental distress prevalence (%)")
    ax.set_ylabel("Tweet sentiment mean")
    ax.set_title(f"Sentiment vs. Mental Distress — {year}")
    # 在角落标注相关系数
    txt = (f"N={len(df):,}\n"
           f"Pearson: {pear_u:.2f}  (w {pear_w:.2f})\n"
           f"Spearman: {spear_u:.2f} (w {spear_w:.2f})")
    ax.text(0.02, 0.98, txt, transform=ax.transAxes, va="top",
            bbox=dict(boxstyle="round,pad=0.25", fc="white", ec="0.8", alpha=0.9), fontsize=9)
    ax.grid(alpha=0.2)
    fig.tight_layout()
    fig.savefig(OUT_DIR / f"scatter_sent_vs_MHLTH_{year}{suffix}.png", dpi=300)
    plt.close(fig)

# —— 跨年摘要表 —— #
summary = pd.DataFrame(rows).sort_values("year")
summary.to_csv(OUT_DIR / f"places_correlation_summary{suffix}.csv", index=False)
print("\nSaved:", (OUT_DIR / f'places_correlation_summary{suffix}.csv').resolve())
