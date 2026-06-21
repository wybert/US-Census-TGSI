#!/bin/bash
#SBATCH -J backfill_one_sentiment
#SBATCH -p gpu
#SBATCH --gres=gpu:1
#SBATCH -c 8
#SBATCH --mem=64G
#SBATCH -t 2:00:00
#SBATCH -o /n/home11/xiaokangfu/xiaokang/US-Census-TGSI/outputs/logs/backfill_one_%j.out
#SBATCH -e /n/home11/xiaokangfu/xiaokang/US-Census-TGSI/outputs/logs/backfill_one_%j.err

set -euo pipefail
cd /n/home11/xiaokangfu/xiaokang/US-Census-TGSI

RAW=/n/holylabs/LABS/cga/Lab/data/geo-tweets/cga-sbg-tweets/2014/2014_4_25_11.csv.gz
OUT=/n/netscratch/cga/Lab/xiaokang/US-Census-TGSI-workspace/data/geotweets_with_sentiment/2014/2014_4_25_11.parquet

/n/home11/xiaokangfu/.conda/envs/sentiment2022/bin/python scripts/backfill_one_sentiment.py "$RAW" "$OUT"
