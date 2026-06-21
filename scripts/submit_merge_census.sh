#!/bin/bash
#SBATCH -J merge_census
#SBATCH -p shared
#SBATCH -c 4
#SBATCH --mem=64G
#SBATCH -t 60
#SBATCH -o outputs/logs/merge_census_%j.out
#SBATCH -e outputs/logs/merge_census_%j.err

/n/home11/xiaokangfu/.conda/envs/geo/bin/python src/03_spatial_join/0.3.8-merge-census-to-parquet.py
