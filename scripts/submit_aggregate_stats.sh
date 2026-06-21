#!/bin/bash
# Stage 04A: generate the final aggregated sentiment product (daily/monthly/yearly
# per GEOID20, confidence-stratified) from the rebuilt spatial-join output.
# Collects a full year into memory via polars, so it needs a large-memory node.
#SBATCH -J aggregate_stats
#SBATCH -p sapphire
#SBATCH -c 64
#SBATCH --mem=900G
#SBATCH -t 12:00:00
#SBATCH -o /n/home11/xiaokangfu/xiaokang/US-Census-TGSI/outputs/logs/aggregate_stats_%j.out
#SBATCH -e /n/home11/xiaokangfu/xiaokang/US-Census-TGSI/outputs/logs/aggregate_stats_%j.err

set -euo pipefail
cd /n/home11/xiaokangfu/xiaokang/US-Census-TGSI

/n/home11/xiaokangfu/.conda/envs/geo/bin/python \
    src/04_validation/aggregation/01_generate_aggregated_stats.py \
    --start-year 2010 --end-year 2023

# Mark completion so the Snakemake generate_aggregated_stats rule is satisfied.
touch /n/netscratch/cga/Lab/xiaokang/US-Census-TGSI-workspace/data/aggregated_sentiment_stats/.aggregation_complete
echo "Aggregation complete."
