#!/usr/bin/env bash
#SBATCH -J val_plots
#SBATCH -o outputs/logs/val_plots_%j.out
#SBATCH -e outputs/logs/val_plots_%j.err
#SBATCH -p sapphire
#SBATCH -t 01:00:00
#SBATCH -c 8
#SBATCH --mem=32G
#SBATCH --account=cga

set -e

# Create logs directory if it doesn't exist
mkdir -p outputs/logs

echo "Starting Validation Plots Generation"
echo "Date: $(date)"
echo "Host: $(hostname)"

# Run optimized script
/n/home11/xiaokangfu/.conda/envs/geo/bin/python 0.4.7-validation-plots-block-opt.py

echo "Job Complete"
echo "Date: $(date)"
