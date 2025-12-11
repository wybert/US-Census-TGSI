#!/bin/bash
#SBATCH -J test_sentiment_gpu         # Job name
#SBATCH -o outputs/logs/test_sentiment_gpu_%j.out  # Output file
#SBATCH -e outputs/logs/test_sentiment_gpu_%j.err  # Error file
#SBATCH -p gpu_test                   # Partition - GPU test queue
#SBATCH -c 4                          # Number of CPU cores
#SBATCH --mem=32G                     # Memory
#SBATCH -t 00:30:00                   # Time limit (30 minutes)
#SBATCH --gres=gpu:1                  # Request 1 GPU

# GPU Test Script for Sentiment Recomputation
# Tests with year 2020 (only 1 missing file)

echo "=========================================="
echo "GPU Test: Sentiment Recomputation"
echo "=========================================="
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $SLURM_NODELIST"
echo "Partition: $SLURM_JOB_PARTITION"
echo "Start time: $(date)"
echo ""

# Load modules
echo "Loading modules..."
module load python/3.8.12-fasrc01
module load cuda/11.3.1-fasrc01

# Activate conda environment
echo "Activating sentiment2022 conda environment..."
source ~/.bashrc
conda activate sentiment2022

# Verify conda environment
echo ""
echo "Python version:"
python --version
echo ""

# Check GPU availability
echo "GPU Information:"
nvidia-smi
echo ""

# Check if PyTorch can see GPU
echo "Checking PyTorch GPU availability:"
python -c "import torch; print(f'PyTorch version: {torch.__version__}'); print(f'CUDA available: {torch.cuda.is_available()}'); print(f'CUDA version: {torch.version.cuda}' if torch.cuda.is_available() else 'No CUDA'); print(f'GPU count: {torch.cuda.device_count()}' if torch.cuda.is_available() else '')"
echo ""

# Create output directory
mkdir -p outputs/logs

# Run the recompute script for 2020 (1 file only)
echo "Running sentiment recomputation for year 2020 (1 file)..."
echo "Working directory: $(pwd)"
echo ""

python 0.1.6-recompute-missing-sentiment.py \
    --year 2020 \
    --batch_size 50 \
    --use_symlink

EXIT_CODE=$?

echo ""
echo "=========================================="
echo "Computation Complete"
echo "=========================================="

if [ $EXIT_CODE -eq 0 ]; then
    echo "✓ Sentiment computation completed successfully"
    echo ""

    # Verify the output
    echo "Verifying output files..."
    python 0.1.8-verify-and-copy-sentiment.py --year 2020 --dry_run

    VERIFY_EXIT=$?

    if [ $VERIFY_EXIT -eq 0 ]; then
        echo ""
        echo "✓ Verification passed!"
        echo ""
        echo "Output files location:"
        echo "  /n/netscratch/cga/Lab/xiaokang/US-Census-TGSI-workspace/sentiment_computing_path/output/2020/"
        echo ""
        echo "Next steps:"
        echo "1. Review the verification output above"
        echo "2. If everything looks good, copy to main directory:"
        echo "   python 0.1.8-verify-and-copy-sentiment.py --year 2020 --copy"
        echo "3. Then submit jobs for other years:"
        echo "   bash 0.1.7-submit-recompute-sentiment-jobs.sh"
    else
        echo "⚠️  Verification had warnings. Please review the output above."
    fi
else
    echo "✗ Sentiment computation failed with exit code $EXIT_CODE"
    echo "Check the error log: outputs/logs/test_sentiment_gpu_${SLURM_JOB_ID}.err"
fi

echo ""
echo "End time: $(date)"
echo "=========================================="

exit $EXIT_CODE
