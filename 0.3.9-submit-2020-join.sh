#!/bin/bash
#SBATCH --job-name=sj-2020
#SBATCH --output=outputs/logs/spatial_join_duckdb_2020_%j.out
#SBATCH --error=outputs/logs/spatial_join_duckdb_2020_%j.err
#SBATCH --partition=sapphire
#SBATCH --nodes=1
#SBATCH -t 24:00:00
#SBATCH --mem=500G
#SBATCH -c 110

# Create logs directory if it doesn't exist
mkdir -p outputs/logs

echo "Starting 2020 Spatial Join Batch Processing"
echo "Date: $(date)"
echo "Host: $(hostname)"

# Use absolute path to python in the geo environment
/n/home11/xiaokangfu/.conda/envs/geo/bin/python 0.3.9-run-2020-spatial-join.py

echo "Job Complete"
