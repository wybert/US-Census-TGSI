#!/usr/bin/env bash
#SBATCH -J spatial_maps
#SBATCH -o outputs/logs/spatial_maps_%j.out
#SBATCH -e outputs/logs/spatial_maps_%j.err
#SBATCH -p sapphire
#SBATCH -t 02:00:00
#SBATCH -c 16
#SBATCH --mem=128G
#SBATCH --account=cga

set -e

# Create logs directory if it doesn't exist
mkdir -p outputs/logs

echo "Starting Spatial Maps Generation"
echo "Date: $(date)"
echo "Host: $(hostname)"

# Run optimized script
/n/home11/xiaokangfu/.conda/envs/geo/bin/python 0.4.5-generate-spatial-maps.py

echo "Job Complete"
echo "Date: $(date)"
