#!/bin/bash
#SBATCH -J spatial_join_2020_test  # Job name
#SBATCH -c 110                      # Number of cores
#SBATCH -t 0-12:00:00              # Runtime 12 hours (should be enough for 1 year)
#SBATCH -p sapphire                 # Partition to submit to
#SBATCH --mem=900000                # Memory pool for all cores (900GB)
#SBATCH -o outputs/logs/spatial_join_2020_test_%j.out
#SBATCH -e outputs/logs/spatial_join_2020_test_%j.err

# Test run: Spatial Join with Confidence Weighting for 2020 only
# This is a test job to verify the pipeline before running full 2010-2023

echo "=========================================="
echo "Spatial Join Test: Year 2020 Only"
echo "=========================================="
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $SLURM_NODELIST"
echo "Cores: $SLURM_CPUS_PER_TASK"
echo "Memory: $SLURM_MEM_PER_NODE MB"
echo "Start time: $(date)"
echo ""

# Activate conda environment
echo "Python version:"
/n/home11/xiaokangfu/.conda/envs/geo/bin/python --version
echo ""

# Create output directories
mkdir -p outputs/logs

# Run the spatial join script for 2020 only
echo "Running spatial join with confidence calculation for year 2020..."
echo "Working directory: $(pwd)"
echo ""

/n/home11/xiaokangfu/.conda/envs/geo/bin/python \
    0.3.3-spatial-join-with-confidence.py \
    --year 2020

EXIT_CODE=$?

echo ""
echo "=========================================="
echo "Test Job Complete"
echo "=========================================="
echo "End time: $(date)"
echo "Exit code: $EXIT_CODE"

if [ $EXIT_CODE -eq 0 ]; then
    echo "✓ Spatial join test completed successfully"
    echo ""
    echo "Output location:"
    echo "  /n/netscratch/cga/Lab/xiaokang/US-Census-TGSI-workspace/data/tweets_with_census_blocks_confidence/2020/"
    echo ""
    echo "Verify results:"
    echo "  ls -lh /n/netscratch/cga/Lab/xiaokang/US-Census-TGSI-workspace/data/tweets_with_census_blocks_confidence/2020/ | head -20"
    echo ""
    echo "Check sample file:"
    echo "  /n/home11/xiaokangfu/.conda/envs/geo/bin/python -c \"import pandas as pd; df=pd.read_parquet('/n/netscratch/cga/Lab/xiaokang/US-Census-TGSI-workspace/data/tweets_with_census_blocks_confidence/2020/2020_01_01_00-tl_2020_06_tabblock20.parquet'); print('Shape:', df.shape); print('Columns:', df.columns.tolist()); print('GPS/Confidence stats:'); print(df[['GPS', 'confidence']].describe())\""
    echo ""
    echo "If test successful, submit full job:"
    echo "  sbatch 0.3.3-run-spatial-join-with-confidence.sh"
else
    echo "✗ Spatial join test failed with exit code $EXIT_CODE"
    echo "Check error log for details: outputs/logs/spatial_join_2020_test_*.err"
fi

exit $EXIT_CODE
