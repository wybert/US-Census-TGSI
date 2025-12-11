#!/bin/bash
#SBATCH -J validate_sentiment        # Job name
#SBATCH -o outputs/logs/validate_sentiment_%j.out  # Output file
#SBATCH -e outputs/logs/validate_sentiment_%j.err  # Error file
#SBATCH -p gpu_test                  # Partition - GPU test queue
#SBATCH -c 4                         # Number of CPU cores
#SBATCH --mem=32G                    # Memory
#SBATCH -t 00:30:00                  # Time limit (30 minutes)
#SBATCH --gres=gpu:1                 # Request 1 GPU

# Validation script to verify sentiment computation correctness
# This tests with existing files that already have sentiment scores

echo "=========================================="
echo "Sentiment Computation Validation"
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

# Verify environment
echo ""
echo "Python version:"
python --version
echo ""

# Check GPU
echo "GPU Information:"
nvidia-smi
echo ""

# Check PyTorch
echo "Checking PyTorch:"
python -c "import torch; print(f'PyTorch version: {torch.__version__}'); print(f'CUDA available: {torch.cuda.is_available()}')"
echo ""

# Create output directory
mkdir -p outputs/logs
mkdir -p outputs/validation

# Get year from command line argument, default to 2015 (year with 100% coverage)
YEAR=${1:-2015}
SAMPLE_SIZE=${2:-3}

# Run validation
echo "Running validation with year $YEAR, sample size $SAMPLE_SIZE..."
echo "Working directory: $(pwd)"
echo ""

python 0.1.10-validate-sentiment-computation.py \
    --year $YEAR \
    --sample-size $SAMPLE_SIZE \
    --batch-size 100 \
    --save-output

EXIT_CODE=$?

echo ""
echo "=========================================="
echo "Validation Complete"
echo "=========================================="
echo "End time: $(date)"
echo "Exit code: $EXIT_CODE"

if [ $EXIT_CODE -eq 0 ]; then
    echo ""
    echo "✅ VALIDATION PASSED!"
    echo "The sentiment computation method is verified to be correct."
    echo ""
    echo "Next steps:"
    echo "1. You can now safely run sentiment computation on missing files"
    echo "2. Submit batch jobs: bash 0.1.7-submit-recompute-sentiment-jobs.sh"
else
    echo ""
    echo "⚠️  VALIDATION FAILED OR HAD WARNINGS"
    echo "Please review the output above and the log files."
    echo ""
    echo "Check validation results:"
    echo "  cat outputs/validation/validation_summary.json"
fi

exit $EXIT_CODE
