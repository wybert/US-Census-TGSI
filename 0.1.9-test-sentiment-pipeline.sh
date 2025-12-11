#!/bin/bash
# Quick test script to verify the sentiment recomputation pipeline
# This tests with year 2020 which only has 1 missing file

echo "=========================================="
echo "Testing Sentiment Recomputation Pipeline"
echo "=========================================="
echo ""

# Activate conda environment
source ~/.bashrc
conda activate geo

# Step 1: Check if we have the missing files list
echo "Step 1: Checking for missing files list..."
if [ ! -f "outputs/missing_sentiment_files.csv" ]; then
    echo "  Missing files list not found. Running detection script..."
    python 0.1.5-find-missing-sentiment-files.py
else
    echo "  ✓ Missing files list found"
fi
echo ""

# Step 2: Run a small test with year 2020 (only 1 file)
echo "Step 2: Testing recomputation with year 2020 (1 file)..."
echo "  This is a dry run to verify the pipeline works"
python 0.1.6-recompute-missing-sentiment.py --year 2020 --dry_run --batch_size 50

echo ""
read -p "Dry run looks good? Continue with actual computation? (y/n) " -n 1 -r
echo ""

if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "  Running actual computation for 2020..."
    python 0.1.6-recompute-missing-sentiment.py --year 2020 --batch_size 50 --use_symlink

    if [ $? -eq 0 ]; then
        echo ""
        echo "  ✓ Computation completed successfully"
        echo ""

        # Step 3: Verify the output
        echo "Step 3: Verifying output..."
        python 0.1.8-verify-and-copy-sentiment.py --year 2020 --dry_run

        echo ""
        echo "=========================================="
        echo "Test Complete!"
        echo "=========================================="
        echo ""
        echo "If the test passed, you can now:"
        echo "1. Submit jobs for other years: bash 0.1.7-submit-recompute-sentiment-jobs.sh"
        echo "2. Or submit individual years: sbatch 0.1.6-recompute-sentiment-slurm.sh 2014"
        echo ""
    else
        echo ""
        echo "✗ Computation failed. Check the error messages above."
        echo ""
    fi
else
    echo ""
    echo "Test cancelled. No files were computed."
    echo ""
fi
