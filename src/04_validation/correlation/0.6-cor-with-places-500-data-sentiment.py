#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
For each year, join tract-level sentiment with CDC PLACES (tract GIS-friendly wide),
compute Spearman/Pearson correlations (unweighted & population-weighted),
and plot scatter + decile curve. Saves per-year PNGs and a CSV summary.

Edit the PATHS below to your files.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from typing import Dict
import json

# Load configuration
with open('setting.json') as f:
    config = json.load(f)

# ========== PATHS: 修改为你的本地路径 ==========
places_data_dir = Path(config["places_data"])
PLACES_FILES: Dict[int, str] = {
    2020: str(places_data_dir / "PLACES__Census_Tract_Data_(GIS_Friendly_Format),_2020_release_20250903.csv"),
    2021: str(places_data_dir / "PLACES__Census_Tract_Data_(GIS_Friendly_Format),_2021_release_20250903.csv"),
    2022: str(places_data_dir / "PLACES__Census_Tract_Data_(GIS_Friendly_Format),_2022_release_20250903.csv"),
    2023: str(places_data_dir / "PLACES__Census_Tract_Data_(GIS_Friendly_Format),_2023_release_20250903.csv"),
    2024: str(places_data_dir / "PLACES__Census_Tract_Data_(GIS_Friendly_Format),_2024_release_20250903.csv"),
}
# 你每年的情感数据（tract 级）：文件名或模板
SENT_DIR = Path(config["sentiment_by_tract"])
SENT_PATTERN = "tract_sentiment_{year}.parquet"  # 文件需含列：GEOID20_tract, sent_mean, mask_low_coverage[, n_tweets]

OUT_DIR = Path(config["outputs_dir"]) / "correlation"; OUT_DIR.mkdir(parents=True, exist_ok=True)

# ========== 选用的 PLACES 指标 ==========
# 主验证：Frequent mental distress（MHLTH_CrudePrev）预期与情感均值负相关
TARGET_COL = "MHLTH_CrudePrev"
# 判别对照（可选换成 BPHIGH_CrudePrev / COLON_SCREEN_CrudePrev 等）
DISCRIM_COL = "MAMMOUSE_CrudePrev"

# ========== 工具函数 ==========
def load_places(path: str) -> pd.DataFrame:
    usecols = ["TractFIPS", "TotalPopulation", TARGET_COL, DISCRIM_COL]
    df = pd.read_csv(path, usecols=usecols, dtype={"TractFIPS": str})
    df.rename(columns={
        "TractFIPS": "GEOID20_tract",
        "TotalPopulation": "pop",
        TARGET_COL: "mhlth",
        DISCRIM_COL: "disc"
    }, inplace=True)
    # 规范类型
    df["GEOID20_tract"] = df["GEOID20_tract"].str.zfill(11)
    for c in ["mhlth", "disc", "pop"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def load_sentiment_tract(year: int) -> pd.DataFrame:
    """读取你准备的 'tract_sentiment_{year}.parquet'；如不存在，抛错。"""
    f = SENT_DIR / SENT_PATTERN.format(year=year)
    if not f.exists():
        raise FileNotFoundError(f"Missing {f}. If you only have block-level files, use the block→tract aggregation branch.")
    s = pd.read_parquet(f)
    # 规范列名
    rename_map = {}
    if "sent_mean_2020" in s.columns and "sent_mean" not in s.columns:
        rename_map["sent_mean_2020"] = "sent_mean"
    s = s.rename(columns=rename_map)
    # 必要列检查
    need = {"GEOID20_tract", "sent_mean", "mask_low_coverage"}
    missing = need - set(s.columns)
    if missing:
        raise ValueError(f"{f} lacks columns: {missing}")
    s["GEOID20_tract"] = s["GEOID20_tract"].astype(str).str.zfill(11)
    return s[["GEOID20_tract", "sent_mean", "mask_low_coverage"] + ([ "n_tweets"] if "n_tweets" in s.columns else [])].copy()

# ------- 可选：从 block → tract 聚合（如果你只有 15 位 block GEOID） -------
def aggregate_block_to_tract(block_df: pd.DataFrame, year: int) -> pd.DataFrame:
    """
    输入 block_df 至少包含：
    GEOID20_block(15位), sent_mean_block, mask_low_coverage[, n_tweets]
    聚合到 tract：sent_mean 用 n_tweets 加权平均（若无 n_tweets，则简单均值）
    """
    b = block_df.copy()
    b["GEOID20_tract"] = b["GEOID20_block"].astype(str).str.slice(0, 11)
    w = b["n_tweets"] if "n_tweets" in b.columns else pd.Series(1.0, index=b.index)
    grp = b.groupby("GEOID20_tract", as_index=False).apply(
        lambda g: pd.Series({
            "sent_mean": np.average(g["sent_mean_block"], weights=(g["n_tweets"] if "n_tweets" in g.columns else None)),
            "mask_low_coverage": 1 if (g["mask_low_coverage"]==1).all() else 0,   # 只要组内有可报告就保留为0
            "n_tweets": g["n_tweets"].sum() if "n_tweets" in g.columns else np.nan
        })
    ).reset_index(drop=True)
    return grp

def weighted_corr(x, y, w):
    """人口加权 Pearson 相关。"""
    x, y, w = map(lambda s: pd.Series(s, dtype=float), (x, y, w))
    m_x = np.sum(w * x) / np.sum(w); m_y = np.sum(w * y) / np.sum(w)
    cov = np.sum(w * (x - m_x) * (y - m_y))
    varx = np.sum(w * (x - m_x)**2); vary = np.sum(w * (y - m_y)**2)
    return cov / np.sqrt(varx * vary)

def rank_spearman(x, y):
    """无权 Spearman（用平均秩+Pearson）。"""
    rx = pd.Series(x).rank(method="average")
    ry = pd.Series(y).rank(method="average")
    return np.corrcoef(rx, ry)[0,1]

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
years = sorted(PLACES_FILES.keys())
rows = []

for year in years:
    print(f"\n=== {year} ===")
    places = load_places(PLACES_FILES[year])

    # 情感：如果已有 tract 文件，用 load_sentiment_tract(year)；若只有 block，请先聚合（见上）
    senti = load_sentiment_tract(year)

    # 合并与过滤
    df = senti.merge(places, on="GEOID20_tract", how="inner")
    df = df[(df["mask_low_coverage"] == 0) & df["pop"].notna() & df["mhlth"].notna() & df["sent_mean"].notna()]
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

    print(f"N={len(df):,}   Pearson={pear_u:.3f} (w={pear_w:.3f})   Spearman={spear_u:.3f} (w={spear_w:.3f})")
    print(f"Discriminant → Pearson={pear_u_disc:.3f} (w={pear_w_disc:.3f})   Spearman={spear_u_disc:.3f} (w={spear_w_disc:.3f})")

    rows.append({
        "year": year, "N": len(df),
        "pearson": pear_u, "pearson_w": pear_w,
        "spearman": spear_u, "spearman_w": spear_w,
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
    fig.savefig(OUT_DIR / f"scatter_sent_vs_MHLTH_{year}.png", dpi=300)
    plt.close(fig)

# —— 跨年摘要表 —— #
summary = pd.DataFrame(rows).sort_values("year")
summary.to_csv(OUT_DIR / "places_correlation_summary.csv", index=False)
print("\nSaved:", (OUT_DIR / 'places_correlation_summary.csv').resolve())
