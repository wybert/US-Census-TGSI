#!/bin/bash
#SBATCH -J agg_test_2022
#SBATCH -o outputs/logs/agg_test_2022_%j.out
#SBATCH -e outputs/logs/agg_test_2022_%j.err
#SBATCH -p sapphire
#SBATCH -t 1-00:00:00          # 1 day runtime
#SBATCH -c 110                 # 110 CPUs
#SBATCH --mem=980G            # 980GB RAM
#SBATCH --account=cga        # Specify your Slurm account

# Create logs directory if it doesn't exist
mkdir -p outputs/logs

echo "Starting Aggregation Test for 2022"
echo "Date: $(date)"
echo "Host: $(hostname)"
echo "Slurm Job ID: $SLURM_JOB_ID"
echo "Requested CPUs: $SLURM_CPUS_PER_TASK"
echo "Requested Memory: $SLURM_MEM_PER_NODE"

# Use absolute path to python in the geo environment
/n/home11/xiaokangfu/.conda/envs/geo/bin/python 0.4.1-aggregate-test-2022.py

echo "Job Complete"
echo "Date: $(date)"
