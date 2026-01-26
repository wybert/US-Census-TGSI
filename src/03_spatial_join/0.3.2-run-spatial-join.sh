#!/bin/bash
#SBATCH -c 110                # Number of cores
#SBATCH -t 03-00:00          # Runtime in D-HH:MM (3 days)
#SBATCH -p sapphire           # Partition to submit to
#SBATCH --mem=900000          # Memory pool for all cores (900GB)
#SBATCH -o outputs/logs/spatial_join_%j.out    # File to which STDOUT will be written, %j inserts jobid
#SBATCH -e outputs/logs/spatial_join_%j.err     # File to which STDERR will be written, %j inserts jobid

# Parse command line arguments for year specification
# Usage: sbatch 0.3.2-run-spatial-join.sh [--year YYYY]
# Example: sbatch 0.3.2-run-spatial-join.sh --year 2010

# Print job information
echo "=========================================="
echo "Spatial Join - Full Pipeline"
echo "=========================================="
echo "SLURM Job ID: $SLURM_JOB_ID"
echo "Running on host: $(hostname)"
echo "Starting time: $(date)"
echo "Working directory: $(pwd)"
echo "Cores: $SLURM_CPUS_PER_TASK"
echo "Memory: 900GB"
echo "Arguments: $@"
echo "=========================================="
echo ""

# Run the spatial join script with any command line arguments
/n/home11/xiaokangfu/.conda/envs/geo/bin/python '0.3.2-xiaokang-sjoin-geopandas-us-census-script-version.py' "$@"

# Print completion information
echo ""
echo "=========================================="
echo "Job completed at: $(date)"
echo "Exit code: $?"
echo "=========================================="
