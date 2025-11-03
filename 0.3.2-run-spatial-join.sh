#!/bin/bash
#SBATCH -c 110                # Number of cores
#SBATCH -t 03-00:00          # Runtime in D-HH:MM (3 days)
#SBATCH -p sapphire           # Partition to submit to
#SBATCH --mem=900000          # Memory pool for all cores (900GB)
#SBATCH -o outputs/logs/spatial_join_%j.out    # File to which STDOUT will be written, %j inserts jobid
#SBATCH -e outputs/logs/spatial_join_%j.err     # File to which STDERR will be written, %j inserts jobid

# Print job information
echo "=========================================="
echo "SLURM Job ID: $SLURM_JOB_ID"
echo "Running on host: $(hostname)"
echo "Starting time: $(date)"
echo "Working directory: $(pwd)"
echo "=========================================="

# Run the spatial join script
/n/home11/xiaokangfu/.conda/envs/geo/bin/python '0.3.2-xiaokang-sjoin-geopandas-us-census-script-version.py'

# Print completion information
echo "=========================================="
echo "Job completed at: $(date)"
echo "=========================================="
