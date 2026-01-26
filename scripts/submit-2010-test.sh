#!/bin/bash
#SBATCH -c 110                # 110 cores for parallel processing
#SBATCH -t 03:00:00          # 3 hours (should be enough for 2010)
#SBATCH -p sapphire           # Sapphire partition
#SBATCH --mem=150000          # 150GB memory (less than full 900GB)
#SBATCH -o outputs/logs/spatial_join_2010_%j.out
#SBATCH -e outputs/logs/spatial_join_2010_%j.err
#SBATCH --job-name=sjoin_2010

# ========================================
# Spatial Join Test - 2010 Data Only
# ========================================
# This is a test run with 2010 data before running the full pipeline
#
# Expected:
#   - Input: 79 files from 2010
#   - Output: ~4,029 files (79 × 51 states)
#   - Time: 1-2 hours (estimate)
# ========================================

echo "=========================================="
echo "Spatial Join Test - 2010 Data Only"
echo "=========================================="
echo "SLURM Job ID: $SLURM_JOB_ID"
echo "Running on host: $(hostname)"
echo "Starting time: $(date)"
echo "Working directory: $(pwd)"
echo "Cores: $SLURM_CPUS_PER_TASK"
echo "Memory: 150GB"
echo "Python: /n/home11/xiaokangfu/.conda/envs/geo/bin/python"
echo "=========================================="
echo ""

# Run the spatial join for 2010 only
/n/home11/xiaokangfu/.conda/envs/geo/bin/python '0.3.2-xiaokang-sjoin-geopandas-us-census-script-version.py' --year 2010

# Capture exit code
EXIT_CODE=$?

echo ""
echo "=========================================="
echo "Job completed at: $(date)"
echo "Exit code: $EXIT_CODE"
echo "=========================================="

# If successful, show output summary
if [ $EXIT_CODE -eq 0 ]; then
    echo ""
    echo "✓ SUCCESS! Checking outputs..."
    OUTPUT_DIR="/n/netscratch/cga/Lab/xiaokang/US-Census-TGSI-workspace/data/tweets_with_census_blocks/2010"
    if [ -d "$OUTPUT_DIR" ]; then
        FILE_COUNT=$(ls -1 "$OUTPUT_DIR"/*.parquet 2>/dev/null | wc -l)
        echo "  Output files created: $FILE_COUNT"
        echo "  Output directory: $OUTPUT_DIR"
    fi
else
    echo ""
    echo "✗ FAILED with exit code $EXIT_CODE"
    echo "  Check error log: outputs/logs/spatial_join_2010_${SLURM_JOB_ID}.err"
fi

exit $EXIT_CODE
