# Testing Guide

## Testing Spatial Join with 2010 Data

The spatial join script (`0.3.2-xiaokang-sjoin-geopandas-us-census-script-version.py`) now supports running on a single year for testing purposes.

### Why Test with 2010?

2010 has the **smallest dataset**, making it ideal for:
- ✅ Verifying the pipeline works correctly
- ✅ Testing on smaller resources (200GB vs 900GB)
- ✅ Faster completion (hours vs days)
- ✅ Debugging issues before full run

---

## Option 1: Direct Python Execution (Local/Interactive)

```bash
# Test with 2010 only
python 0.3.2-xiaokang-sjoin-geopandas-us-census-script-version.py --year 2010

# Test with specific year range
python 0.3.2-xiaokang-sjoin-geopandas-us-census-script-version.py --start-year 2010 --end-year 2012

# Full run (all years 2010-2023)
python 0.3.2-xiaokang-sjoin-geopandas-us-census-script-version.py
```

### Help Information
```bash
python 0.3.2-xiaokang-sjoin-geopandas-us-census-script-version.py --help
```

Output:
```
usage: 0.3.2-xiaokang-sjoin-geopandas-us-census-script-version.py [-h] [--year YEAR] [--start-year START_YEAR] [--end-year END_YEAR]

Spatial join between tweets and census blocks

optional arguments:
  -h, --help            show this help message and exit
  --year YEAR           Process only specific year (e.g., 2010 for testing)
  --start-year START_YEAR
                        Start year (default: 2010)
  --end-year END_YEAR   End year (default: 2023)
```

---

## Option 2: SLURM Submission (Recommended)

### Test with 2010 Only (6 hours, 200GB)

```bash
# Create a test SLURM script
sbatch <<EOF
#!/bin/bash
#SBATCH -c 110
#SBATCH -t 06:00:00
#SBATCH -p sapphire
#SBATCH --mem=200000
#SBATCH -o outputs/logs/spatial_join_test_2010_%j.out
#SBATCH -e outputs/logs/spatial_join_test_2010_%j.err

/n/home11/xiaokangfu/.conda/envs/geo/bin/python '0.3.2-xiaokang-sjoin-geopandas-us-census-script-version.py' --year 2010
EOF
```

Or use the existing script with arguments:

```bash
# Submit with year argument
sbatch 0.3.2-run-spatial-join.sh --year 2010
```

### Full Run (All Years)

```bash
# Use the standard script without arguments
sbatch 0.3.2-run-spatial-join.sh
```

---

## Resource Requirements

| Mode | Years | Memory | Time | Cores | Partition |
|------|-------|--------|------|-------|-----------|
| **Test (2010)** | 1 year | 200GB | 6h | 110 | sapphire |
| **Small Range** | 2-3 years | 400GB | 12h | 110 | sapphire |
| **Full Pipeline** | 14 years | 900GB | 3d | 110 | sapphire |

---

## Expected Output

### Console Output

```
============================================================
TEST MODE: Processing only year 2010
============================================================

Found 365 files for year 2010

============================================================
Total files to process: 365
Years covered: [2010]
Census states to load: 51
============================================================

Processing census states: 100%|██████████| 51/51 [2:30:00<00:00]

all done!
time used: 2:30:15
```

### Output Directory

```
/tweets_with_census_blocks/2010/
├── tweets_2010_01_01-tl_2020_01_tabblock20.parquet
├── tweets_2010_01_01-tl_2020_02_tabblock20.parquet
├── ...
└── tweets_2010_12_31-tl_2020_56_tabblock20.parquet
```

Each input file generates 51 output files (one per state).

---

## Verification Steps

### 1. Check Job Status
```bash
squeue -u $USER
```

### 2. Monitor Log Files
```bash
# Live monitoring
tail -f outputs/logs/spatial_join_test_2010_*.out

# Check for errors
grep -i error outputs/logs/spatial_join_test_2010_*.err
```

### 3. Verify Output Files
```bash
# Count output files
ls /n/netscratch/cga/Lab/xiaokang/US-Census-TGSI-workspace/data/tweets_with_census_blocks/2010/*.parquet | wc -l

# Expected: ~18,615 files (365 days × 51 states)

# Check file sizes
ls -lh /n/netscratch/cga/Lab/xiaokang/US-Census-TGSI-workspace/data/tweets_with_census_blocks/2010/*.parquet | head
```

### 4. Spot Check Data Quality
```python
import pandas as pd

# Read a sample output file
df = pd.read_parquet('/path/to/output/tweets_2010_01_01-tl_2020_01_tabblock20.parquet')

print(f"Rows: {len(df)}")
print(f"Columns: {df.columns.tolist()}")
print(f"GEOID20 sample: {df['GEOID20'].head()}")

# Verify GEOID20 exists and is 15 digits
assert 'GEOID20' in df.columns, "GEOID20 column missing!"
assert df['GEOID20'].str.len().eq(15).all(), "Invalid GEOID20 format!"
```

---

## Common Issues

### Issue 1: No 2010 Data Found

**Error:**
```
ERROR: Directory /path/geotweets_with_sentiment/2010 does not exist!
```

**Solution:** Ensure the tweet-sentiment merge step (0.2.1) has completed for 2010.

### Issue 2: Memory Exceeded

**Symptom:** Job killed with exit code 137

**Solution:**
- Reduce parallelization: Edit script, change `pandarallel.initialize(nb_workers=50)`
- Increase memory: `#SBATCH --mem=300000`

### Issue 3: Missing Census Files

**Error:**
```
Census states to load: 0
```

**Solution:** Run the census download first:
```bash
python 0.1-download_cenus_data.py
```

---

## Benchmarks (Estimated)

Based on 2010 data size:

| Metric | Value |
|--------|-------|
| **Input tweets** | ~5-10M tweets |
| **Processing time** | 2-4 hours |
| **Output files** | ~18,615 parquet files |
| **Total output size** | ~50-100 GB |
| **Peak memory** | ~150-180 GB |

*Note: Actual values depend on tweet volume per day*

---

## After Successful Test

Once 2010 completes successfully:

1. **Verify Output Quality**
   - Check a few output files have GEOID20
   - Confirm spatial join worked (tweets matched to blocks)

2. **Submit Full Pipeline**
   ```bash
   sbatch 0.3.2-run-spatial-join.sh
   ```

3. **Monitor Progress**
   - Set up email notifications: `#SBATCH --mail-type=END,FAIL`
   - Periodically check logs

---

## Snakemake Integration

The Snakemake workflow doesn't yet support year-specific testing. To test via Snakemake, you'll need to:

1. Manually run the test first
2. Or modify the rule temporarily:
   ```python
   shell:
       """
       python {input.script} --year 2010 > {log} 2>&1
       touch {output.flag}
       """
   ```

---

## Quick Reference

```bash
# Test 2010
python 0.3.2-xiaokang-sjoin-geopandas-us-census-script-version.py --year 2010

# Test 2010 via SLURM
sbatch 0.3.2-run-spatial-join.sh --year 2010

# Full run
sbatch 0.3.2-run-spatial-join.sh

# Check status
squeue -u $USER

# Monitor logs
tail -f outputs/logs/spatial_join_*.out
```
