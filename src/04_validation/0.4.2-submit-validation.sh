#!/bin/bash
#SBATCH -J validation_metrics
#SBATCH -o outputs/logs/validation_metrics_%j.out
#SBATCH -e outputs/logs/validation_metrics_%j.err
#SBATCH -p sapphire
#SBATCH -t 1-00:00:00
#SBATCH -c 110
#SBATCH --mem=900G
#SBATCH --account=cga

set -e

# Create logs directory if it doesn't exist
mkdir -p outputs/logs

echo "Starting Validation Metrics Calculation (CR, Gini)"
echo "Date: $(date)"
echo "Host: $(hostname)"

# Use absolute path to python in the geo environment
/n/home11/xiaokangfu/.conda/envs/geo/bin/python 0.4.2-calculate-validation-metrics.py

echo "Job Complete"
echo "Date: $(date)"
