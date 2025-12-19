#!/usr/bin/env bash
#SBATCH -J spatial_rep
#SBATCH -o outputs/logs/spatial_rep_%j.out
#SBATCH -e outputs/logs/spatial_rep_%j.err
#SBATCH -p sapphire
#SBATCH -t 04:00:00
#SBATCH -c 32
#SBATCH --mem=250G
#SBATCH --account=cga

set -e

# Create logs directory if it doesn't exist
mkdir -p outputs/logs

echo "Starting Spatial Representation (Geometry Merge)"
echo "Date: $(date)"
echo "Host: $(hostname)"

# Run optimized script
/n/home11/xiaokangfu/.conda/envs/geo/bin/python 0.4.3-validation-spatial-representation-optimized.py

echo "Job Complete"
echo "Date: $(date)"
