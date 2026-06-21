#!/bin/bash
#SBATCH -p shared
#SBATCH -c 1
#SBATCH --mem=4G
#SBATCH -t 2-00:00:00
#SBATCH -o outputs/logs/snakemake_master_%j.out
#SBATCH -e outputs/logs/snakemake_master_%j.err

# Load environment
# Adjust the path to your conda installation if needed
source ~/.bashrc
conda activate geo

# Target rule (default to 'all' if not provided)
TARGET=${1:-all}

echo "============================================================"
echo "Snakemake SLURM Master Job"
echo "Job ID: $SLURM_JOB_ID"
echo "Target Rule: $TARGET"
echo "Time: $(date)"
echo "============================================================"

# Run Snakemake using the slurm profile
snakemake --profile config/slurm_profile $TARGET

echo "============================================================"
echo "Snakemake Master Job Finished at $(date)"
echo "============================================================"
