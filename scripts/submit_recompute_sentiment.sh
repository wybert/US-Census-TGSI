#!/bin/bash
# Recompute missing BERT sentiment for one year on a GPU node.
# Usage: sbatch scripts/submit_recompute_sentiment.sh <YEAR>
# Reads outputs/missing_sentiment_files.csv, stages inputs as symlinks under
# sentiment_computing_path/input/<year>, writes bert_sentiment_*.csv.gz to
# sentiment_computing_path/output/<year>.
#SBATCH -J recompute_sentiment
#SBATCH -p gpu
#SBATCH --gres=gpu:1
#SBATCH -c 8
#SBATCH --mem=64G
#SBATCH -t 2-00:00:00
#SBATCH -o /n/home11/xiaokangfu/xiaokang/US-Census-TGSI/outputs/logs/recompute_sentiment_%j.out
#SBATCH -e /n/home11/xiaokangfu/xiaokang/US-Census-TGSI/outputs/logs/recompute_sentiment_%j.err

set -euo pipefail
YEAR="${1:?Usage: sbatch scripts/submit_recompute_sentiment.sh <YEAR>}"

cd /n/home11/xiaokangfu/xiaokang/US-Census-TGSI

echo "=== Recompute sentiment | year=$YEAR | job=$SLURM_JOB_ID | node=$SLURM_NODELIST | $(date) ==="
nvidia-smi || true

/n/home11/xiaokangfu/.conda/envs/sentiment2022/bin/python \
    src/01_data_acquisition/0.1.6-recompute-missing-sentiment.py \
    --year "$YEAR" \
    --batch_size 100 \
    --use_symlink

echo "=== Done year=$YEAR | exit=$? | $(date) ==="
