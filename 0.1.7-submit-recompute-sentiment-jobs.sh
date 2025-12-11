#!/bin/bash
# Script to submit separate SLURM jobs for each year with missing sentiment files
# This allows parallel processing of different years on different GPUs

echo "=========================================="
echo "Submit Sentiment Recomputation Jobs"
echo "=========================================="

# Years with significant missing files (based on 0.1.5 analysis)
# 2014: 3986 missing files (54.46% coverage)
# 2017: 513 missing files (94.14% coverage)
# 2020: 1 missing file (99.99% coverage)
# 2023: 510 missing files (88.19% coverage)

YEARS_TO_PROCESS=(2014 2017 2020 2023)

# Create logs directory
mkdir -p outputs/logs

echo "Years to process: ${YEARS_TO_PROCESS[@]}"
echo ""

# Submit jobs
JOB_IDS=()

for year in "${YEARS_TO_PROCESS[@]}"; do
    echo "Submitting job for year $year..."

    # Submit job and capture job ID
    JOB_OUTPUT=$(sbatch --job-name="sent_${year}" 0.1.6-recompute-sentiment-slurm.sh $year)
    JOB_ID=$(echo $JOB_OUTPUT | awk '{print $4}')

    echo "  Submitted job $JOB_ID for year $year"
    JOB_IDS+=($JOB_ID)

    # Small delay to avoid overwhelming the scheduler
    sleep 1
done

echo ""
echo "=========================================="
echo "All jobs submitted"
echo "=========================================="
echo "Job IDs: ${JOB_IDS[@]}"
echo ""
echo "To check job status:"
echo "  squeue -u $USER"
echo ""
echo "To view logs for a specific year (e.g., 2014):"
echo "  tail -f outputs/logs/recompute_sentiment_<job_id>.out"
echo ""
echo "To cancel all jobs:"
echo "  scancel ${JOB_IDS[@]}"
echo ""
