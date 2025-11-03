#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd, numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from scipy import stats
from statsmodels.stats.weightstats import DescrStatsW
from pathlib import Path
import json

# Load configuration
with open('setting.json') as f:
    config = json.load(f)

IN = str(Path(config["workspace"]) / "data/sentiment_places_data_joined.parquet")
OUT_DIR = str(Path(config["outputs_dir"]) / "correlation")

# --- 读取与清洗 ---
df = pd.read_parquet(IN)
# 标准化列名（你提到有断行/空格）
df.columns = [c.strip().replace("\n","").replace(" ", "_").replace("__","_") for c in df.columns]
rename = {}
for c in df.columns:
    if "mask" in c and "coverage" in c:
        rename[c] = "mask_low_coverage"
df = df.rename(columns=rename)

# 只保留：可报告、完整行
df = df.query("mask_low_coverage == 0").copy()
df = df.dropna(subset=["sent_mean_year_tract","mhlth","pop"])
df["statefp"] = df["GEOID20_tract"].astype(str).str[:2]

# --- 相关系数（库函数版） ---
def w_pearson(x, y, w):
    # 用 statsmodels 的权重相关（也可改用 np.cov(aweights=...)）
    dsw = DescrStatsW(np.column_stack([x, y]), weights=w, ddof=0)
    return dsw.corrcoef[0,1]

def w_spearman(x, y, w):
    # 加权 Spearman = 对秩做加权 Pearson
    rx = stats.rankdata(x, method="average")
    ry = stats.rankdata(y, method="average")
    return w_pearson(rx, ry, w)

def summarize_group(g: pd.DataFrame) -> pd.Series:
    x, y, w = g["sent_mean_year_tract"].to_numpy(), g["mhlth"].to_numpy(), g["pop"].to_numpy()
    pear   = g["sent_mean_year_tract"].corr(g["mhlth"], method="pearson")   # 无权
    spear  = g["sent_mean_year_tract"].corr(g["mhlth"], method="spearman")  # 无权
    wpear  = w_pearson(x, y, w)                                             # 加权
    wspear = w_spearman(x, y, w)                                            # 加权
    return pd.Series(dict(N=len(g), pearson=pear, spearman=spear,
                          pearson_w=wpear, spearman_w=wspear))

summary = df.groupby("year", sort=True, group_keys=False).apply(summarize_group, include_groups=False).reset_index()
Path(OUT_DIR).mkdir(parents=True, exist_ok=True)
summary.to_csv(f"{OUT_DIR}/places_correlation_summary.csv", index=False)

# --- 十分位“人口加权均值”曲线（作为平滑趋势线） ---
def decile_curve_xy(g: pd.DataFrame, x_col="mhlth", y_col="sent_mean_year_tract", w_col="pop", q=10):
    d = g[[x_col, y_col, w_col]].dropna().copy()
    d["bin"] = pd.qcut(d[x_col], q=q, duplicates="drop")
    out = (d.groupby("bin")
             .apply(lambda s: pd.Series({
                 "x_med": s[x_col].median(),
                 "y_wmean": np.average(s[y_col], weights=s[w_col])
             }), include_groups=False)
             .sort_values("x_med").reset_index(drop=True))
    return out

# --- 画：每年一张（散点 + LOWESS + 十分位曲线） ---
sns.set_theme(context="talk", style="whitegrid")  # seaborn 主题与风格。:contentReference[oaicite:1]{index=1}
years = sorted(df["year"].unique())
for y in years:
    g = df[df["year"]==y]
    if g.empty:
        continue
    # 先画散点（抽样以加速；可调 max_n）
    max_n = 150_000
    gg = g.sample(n=min(len(g), max_n), random_state=42) if len(g)>max_n else g

    fig, ax = plt.subplots(figsize=(6.8,5.6))
    sns.scatterplot(data=gg, x="mhlth", y="sent_mean_year_tract",
                    s=8, alpha=0.15, ax=ax)                                   # seaborn 散点。:contentReference[oaicite:2]{index=2}
    # LOWESS 平滑线（不画散点）
    sns.regplot(data=g, x="mhlth", y="sent_mean_year_tract",
                lowess=True, scatter=False, line_kws=dict(lw=2), ax=ax)       # regplot/lowess。:contentReference[oaicite:3]{index=3}
    # 人口加权十分位均值曲线
    dc = decile_curve_xy(g)
    sns.lineplot(x="x_med", y="y_wmean", data=dc, ax=ax, linewidth=2)

    row = summary.loc[summary["year"]==y].iloc[0]
    txt = (f"N={int(row.N):,}\n"
           f"Pearson {row.pearson:+.2f} (w {row.pearson_w:+.2f})\n"
           f"Spearman {row.spearman:+.2f} (w {row.spearman_w:+.2f})")
    ax.text(0.02, 0.98, txt, transform=ax.transAxes, va="top",
            bbox=dict(boxstyle="round,pad=0.25", fc="white", ec="0.8", alpha=0.9), fontsize=10)

    ax.set_xlabel("Frequent mental distress prevalence (%)")
    ax.set_ylabel("Tweet sentiment mean")
    ax.set_title(f"Sentiment vs. Mental Distress — {y}")
    fig.tight_layout()
    fig.savefig(f"{OUT_DIR}/scatter_sent_vs_MHLTH_{y}.png", dpi=300)
    plt.close(fig)

# --- 画：FacetGrid 小多图（所有年份同一页） ---
fg = sns.FacetGrid(df, col="year", col_wrap=3, sharex=False, sharey=False, height=3.6, aspect=1.1)  # FacetGrid。:contentReference[oaicite:4]{index=4}
def _facet_scatter(data, color, **kws):
    sns.scatterplot(data=data.sample(n=min(len(data), 60_000), random_state=42),
                    x="mhlth", y="sent_mean_year_tract", s=6, alpha=0.12, color=color)
    sns.regplot(data=data, x="mhlth", y="sent_mean_year_tract",
                lowess=True, scatter=False, line_kws=dict(lw=2, alpha=0.9), color=color)
fg.map_dataframe(_facet_scatter)                                                     # FacetGrid.map_dataframe。:contentReference[oaicite:5]{index=5}
fg.set_axis_labels("Frequent mental distress (%)", "Tweet sentiment mean")
fg.fig.subplots_adjust(top=0.9)
fg.fig.suptitle("Sentiment vs. Mental Distress — All years")
fg.savefig(f"{OUT_DIR}/facet_scatter_lowess_all_years.png", dpi=300)
plt.close(fg.fig)

print("Saved:", OUT_DIR)
