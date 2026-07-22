"""
Build a single cross-tier comparison table (Gini and health-correlation
results computed under each of the four confidence tiers: all/medium/
high/strict). This is the data source for the paper's tier-sensitivity
table -- see CLAUDE.md's confidence-tier design and the Technical
Validation "Confidence-Tier Sensitivity" subsection.
"""

import json
import os
import pandas as pd

with open('setting.json') as f:
    config = json.load(f)

OUT_GINI = os.path.join(config["outputs_dir"], "gini")
OUT_CORR = os.path.join(config["outputs_dir"], "correlation")
OUT_VALIDATION = os.path.join(config["outputs_dir"], "validation")
os.makedirs(OUT_VALIDATION, exist_ok=True)

TIERS = ["all", "medium", "high", "strict"]


def tier_suffix(tier):
    return "" if tier == "all" else f"_{tier}"


def tier_gini_prefix(tier):
    return "" if tier == "all" else f"{tier}_"


def read_gini_summary(path):
    stats = {}
    with open(path) as f:
        for line in f:
            if ":" not in line:
                continue
            key, val = line.strip().split(":", 1)
            stats[key.strip()] = val.strip()
    return stats


rows = []
for tier in TIERS:
    row = {"tier": tier}

    all_years_path = os.path.join(OUT_GINI, f"{tier_gini_prefix(tier)}gini-summary.txt")
    g_all = read_gini_summary(all_years_path)
    row["gini_all_years"] = float(g_all["Gini"])
    row["reportable_tracts_all_years"] = int(g_all["Tracts_reportable"])
    row["reportable_share_all_years"] = float(g_all["Reportable_share"])
    row["covered_pop_share_all_years"] = float(g_all["Covered_pop_share"])
    row["log2CR_P10_all_years"] = float(g_all["log2CR_P10"])
    row["log2CR_P50_all_years"] = float(g_all["log2CR_P50"])
    row["log2CR_P90_all_years"] = float(g_all["log2CR_P90"])

    path_2020 = os.path.join(OUT_GINI, f"2020_{tier_gini_prefix(tier)}gini-summary.txt")
    g_2020 = read_gini_summary(path_2020)
    row["gini_2020"] = float(g_2020["Gini"])
    row["reportable_tracts_2020"] = int(g_2020["Tracts_reportable"])
    row["reportable_share_2020"] = float(g_2020["Reportable_share"])
    row["covered_pop_share_2020"] = float(g_2020["Covered_pop_share"])

    corr_path = os.path.join(OUT_CORR, f"places_correlation_summary{tier_suffix(tier)}.csv")
    corr = pd.read_csv(corr_path).set_index("year")
    for year in [2020, 2021, 2022]:
        row[f"corr_N_{year}"] = int(corr.loc[year, "N"])
        row[f"corr_pearson_{year}"] = float(corr.loc[year, "pearson"])
        row[f"corr_pearson_w_{year}"] = float(corr.loc[year, "pearson_w"])

    rows.append(row)

summary = pd.DataFrame(rows).set_index("tier").loc[TIERS]
out_path = os.path.join(OUT_VALIDATION, "tier_sensitivity_summary.csv")
summary.to_csv(out_path)
print(f"Saved tier-sensitivity summary to {out_path}")
print(summary.to_string())
