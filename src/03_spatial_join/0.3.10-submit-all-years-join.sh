#!/bin/bash
#SBATCH -J spatial-join-all-years
#SBATCH -o outputs/logs/spatial-join-all-years-%j.out
#SBATCH -e outputs/logs/spatial-join-all-years-%j.err
#SBATCH -p sapphire
#SBATCH -t 3-00:00:00  # 3 days wall time for ~70 hours of processing (14 years * ~5 hours/year)
#SBATCH -c 110
#SBATCH --mem=900G
#SBATCH --account=cga

# Create logs directory if it doesn't exist
mkdir -p outputs/logs

echo "Starting ALL YEARS (2010-2023) Spatial Join Batch Processing"
echo "Date: $(date)"
echo "Host: $(hostname)"
echo "Slurm Job ID: $SLURM_JOB_ID"
echo "Requested CPUs: $SLURM_CPUS_PER_TASK"
echo "Requested Memory: $SLURM_MEM_PER_NODE"

# Use absolute path to python in the geo environment
# The python script 0.3.9-run-2020-spatial-join.py is now generalized
/n/home11/xiaokangfu/.conda/envs/geo/bin/python 0.3.9-run-2020-spatial-join.py --start-year 2010 --end-year 2023

echo "Job Complete"
echo "Date: $(date)"
