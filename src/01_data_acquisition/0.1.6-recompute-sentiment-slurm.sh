#!/bin/bash
#SBATCH -J recompute_sentiment        # Job name
#SBATCH -o outputs/logs/recompute_sentiment_%j.out  # Output file
#SBATCH -e outputs/logs/recompute_sentiment_%j.err  # Error file
#SBATCH -p gpu                        # Partition (queue) - use GPU partition
#SBATCH -c 8                          # Number of CPU cores
#SBATCH --mem=64G                     # Memory
#SBATCH -t 72:00:00                   # Time limit (72 hours / 3 days)
#SBATCH --gres=gpu:1                  # Request 1 GPU
#SBATCH --constraint="a100|v100"      # Prefer A100 or V100 GPUs

# Script to recompute missing sentiment files using GPU
# This script processes missing sentiment files identified by 0.1.5-find-missing-sentiment-files.py

echo "=========================================="
echo "Recompute Missing Sentiment Files"
echo "=========================================="
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $SLURM_NODELIST"
echo "Start time: $(date)"
echo ""

# Load modules
module load python/3.8.12-fasrc01
module load cuda/11.3.1-fasrc01

# Activate conda environment
echo "Activating sentiment2022 conda environment..."
source ~/.bashrc
conda activate sentiment2022

# Check GPU availability
echo ""
echo "GPU Information:"
nvidia-smi
echo ""

# Get year to process from command line (optional)
YEAR=${1:-""}  # If no argument, process all years

# Create output directory
mkdir -p outputs/logs

# Run the recompute script
echo "Running sentiment recomputation..."
echo "Working directory: $(pwd)"
echo ""

if [ -z "$YEAR" ]; then
    echo "Processing all missing years..."
    python 0.1.6-recompute-missing-sentiment.py \
        --batch_size 100 \
        --use_symlink
else
    echo "Processing year: $YEAR"
    python 0.1.6-recompute-missing-sentiment.py \
        --year $YEAR \
        --batch_size 100 \
        --use_symlink
fi

EXIT_CODE=$?

echo ""
echo "=========================================="
echo "Job Complete"
echo "=========================================="
echo "End time: $(date)"
echo "Exit code: $EXIT_CODE"

if [ $EXIT_CODE -eq 0 ]; then
    echo "✓ Sentiment recomputation completed successfully"
else
    echo "✗ Sentiment recomputation failed with exit code $EXIT_CODE"
fi

exit $EXIT_CODE
