#!/usr/bin/env python
"""
Technical Validation - Temporal Stability & Tier Consistency.

From the yearly block-level aggregates, compute national per-year:
  - tweet volume (sum of tweet_count)
  - tweet-weighted mean sentiment for each confidence tier (all/high/medium/strict)

Outputs:
  outputs/validation/temporal_stability_stats.csv
  outputs/validation/temporal_stability.png  (volume bars + per-tier sentiment lines)

The tier lines tracking each other shows that including lower-confidence spatial
matches ('all') does not systematically bias sentiment vs. GPS-exact ('strict').
"""
import os
import json
import duckdb
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

with open("setting.json") as f:
    config = json.load(f)

yearly_glob = os.path.join(config["aggregated_sentiment_output"], "yearly_*.parquet")
out_dir = os.path.join(config["outputs_dir"], "validation")
os.makedirs(out_dir, exist_ok=True)
csv_path = os.path.join(out_dir, "temporal_stability_stats.csv")
png_path = os.path.join(out_dir, "temporal_stability.png")

TIERS = ["all", "high", "medium", "strict"]

con = duckdb.connect()
# National tweet-weighted mean sentiment per tier per year.
sel = ["year", "SUM(all_tweet_count) AS volume"]
for t in TIERS:
    sel.append(
        f"SUM({t}_tweet_count * {t}_sentiment_mean) / NULLIF(SUM({t}_tweet_count),0) AS {t}_sentiment"
    )
    sel.append(f"SUM({t}_tweet_count) AS {t}_volume")
df = con.execute(
    f"SELECT {', '.join(sel)} FROM read_parquet('{yearly_glob}') GROUP BY year ORDER BY year"
).fetchdf()
df.to_csv(csv_path, index=False)
print("Wrote", csv_path)
print(df[["year", "volume", "all_sentiment", "strict_sentiment"]].to_string(index=False))

# ---- Figure: volume bars (left) + tier sentiment lines (right) ----
fig, ax1 = plt.subplots(figsize=(9, 5))
ax1.bar(df["year"], df["volume"] / 1e6, color="#c9d6e3", label="Tweet volume")
ax1.set_xlabel("Year")
ax1.set_ylabel("Tweet volume (millions)")
ax1.set_xticks(df["year"])
ax1.set_xticklabels(df["year"], rotation=45)

ax2 = ax1.twinx()
styles = {"all": ("#1f77b4", "o"), "high": ("#2ca02c", "s"),
          "medium": ("#ff7f0e", "^"), "strict": ("#d62728", "D")}
for t in TIERS:
    c, m = styles[t]
    ax2.plot(df["year"], df[f"{t}_sentiment"], color=c, marker=m, ms=4,
             lw=1.6, label=f"{t.capitalize()} tier")
ax2.set_ylabel("Tweet-weighted mean sentiment")

lines1, lab1 = ax1.get_legend_handles_labels()
lines2, lab2 = ax2.get_legend_handles_labels()
ax1.legend(lines1 + lines2, lab1 + lab2, loc="upper left", fontsize=8, frameon=False)
plt.title("National Tweet Volume and Mean Sentiment by Year, by Confidence Tier")
fig.tight_layout()
fig.savefig(png_path, dpi=300)
print("Wrote", png_path)

# Tier consistency: max abs difference of each tier vs 'all'
print("\nMax |tier - all| sentiment divergence across years:")
for t in ["high", "medium", "strict"]:
    div = (df[f"{t}_sentiment"] - df["all_sentiment"]).abs().max()
    print(f"  {t}: {div:.4f}")
