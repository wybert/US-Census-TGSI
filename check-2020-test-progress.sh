#!/bin/bash
# Monitor the 2020 test job progress

echo "=========================================="
echo "Spatial Join 2020 Test - Progress Monitor"
echo "=========================================="
echo ""

# Check SLURM job status
echo "📊 SLURM Job Status:"
squeue -u xiaokangfu -o "%.18i %.9P %.30j %.8u %.2t %.10M %.6D %R" | grep -E "JOBID|spatial_join_2020"
echo ""

# Find most recent log files
LATEST_OUT=$(ls -t outputs/logs/spatial_join_2020_test_*.out 2>/dev/null | head -1)
LATEST_ERR=$(ls -t outputs/logs/spatial_join_2020_test_*.err 2>/dev/null | head -1)

if [ -f "$LATEST_OUT" ]; then
    echo "📄 Latest output log: $LATEST_OUT"
    echo "Last 20 lines:"
    echo "----------------------------------------"
    tail -20 "$LATEST_OUT"
    echo ""
fi

if [ -f "$LATEST_ERR" ] && [ -s "$LATEST_ERR" ]; then
    echo "⚠️  Latest error log: $LATEST_ERR"
    echo "Last 10 lines:"
    echo "----------------------------------------"
    tail -10 "$LATEST_ERR"
    echo ""
fi

# Check output directory
OUTPUT_DIR="/n/netscratch/cga/Lab/xiaokang/US-Census-TGSI-workspace/data/tweets_with_census_blocks_confidence/2020"
if [ -d "$OUTPUT_DIR" ]; then
    FILE_COUNT=$(ls "$OUTPUT_DIR"/*.parquet 2>/dev/null | wc -l)
    echo "📁 Output files created: $FILE_COUNT"
    if [ $FILE_COUNT -gt 0 ]; then
        echo "Sample output files:"
        ls -lh "$OUTPUT_DIR"/*.parquet | head -5
        
        # Expected: 8557 input files × 51 states = ~436,407 files
        EXPECTED=436407
        PROGRESS=$(echo "scale=2; $FILE_COUNT * 100 / $EXPECTED" | bc)
        echo ""
        echo "Progress: $FILE_COUNT / $EXPECTED files (~$PROGRESS%)"
    fi
else
    echo "⏳ Output directory not yet created"
fi

echo ""
echo "=========================================="
echo "To view live log:"
echo "  tail -f $LATEST_OUT"
echo ""
echo "To check this again:"
echo "  bash check-2020-test-progress.sh"
echo "=========================================="
