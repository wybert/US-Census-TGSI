# Spatial Join with Confidence Weighting - Implementation Log

## Date: 2025-12-05

## Summary
Implemented a new spatial join workflow that adds confidence weighting based on spatialerror and census block geometry. This creates a separate output directory to preserve the original 0.3.2 output while adding new quality metrics.

## Configuration Changes

### setting.json
Added new output path:
```json
"tweets_with_census_blocks_confidence": "/n/netscratch/cga/Lab/xiaokang/US-Census-TGSI-workspace/data/tweets_with_census_blocks_confidence"
```

**Key difference from original:**
- Original: `tweets_with_census_blocks` (from 0.3.2)
- New: `tweets_with_census_blocks_confidence` (from 0.3.3)

## Files Created

### 1. 0.3.3-spatial-join-with-confidence.py
Main spatial join script with confidence calculation:
- Input: `geotweets_with_sentiment/{year}/*.parquet`
- Output: `tweets_with_census_blocks_confidence/{year}/*-{state}.parquet`

**New output fields:**
- `GPS`: Boolean (True/False) - preserved from input data
- `confidence`: Float (0.05 - 1.0) - calculated based on spatialerror vs block size
- `block_area_m2`: Block area in square meters
- `block_diameter_m`: Block diameter in meters (calculated as 2 * sqrt(area/π))

**Confidence calculation logic:**
```
GPS data:              1.0 (highest confidence)
Error < 50m:           1.0 (GPS-level accuracy)
Error < block_radius:  0.8 (likely in correct block)
Error < block_diameter: 0.5 (might be in adjacent block)
Error < 2*diameter:    0.3 (low confidence)
Error < 1km:           0.15 (very low confidence)
Error >= 1km:          0.05 (check-in level, minimal confidence)
```

**Command-line options:**
- `--year YEAR`: Process only specific year (e.g., 2010 for testing)
- `--start-year YEAR`: Start year (default: 2010)
- `--end-year YEAR`: End year (default: 2023)
- `--dry-run`: Verify inputs without processing (safe for login node)

### 2. 0.3.3-run-spatial-join-with-confidence.sh
SLURM submission script:
- 110 cores, 900GB RAM, 3 days
- Partition: sapphire
- Processes years 2010-2023

## Files Updated

### 1. Snakefile
Updated `spatial_join` rule:
- References: `0.3.3-spatial-join-with-confidence.py`
- Output flag: `.spatial_join_confidence_complete`
- Log: `spatial_join_confidence.log`

### 2. data-pipeline-flowchart.txt
Updated Step 2 (Spatial Join):
- Changed script reference from 0.3.2 to 0.3.3
- Added confidence weighting documentation
- Documented new output fields

## Usage Examples

### 1. GPS High-Quality Subset
```python
import pandas as pd
df = pd.read_parquet('tweets_with_census_blocks_confidence/2020/2020_10_01_00-tl_2020_06_tabblock20.parquet')
gps_data = df[df['GPS'] == True]
```

### 2. High Confidence Filtering
```python
high_conf = df[df['confidence'] >= 0.8]
```

### 3. Weighted Aggregation
```python
# Weighted average sentiment by census tract
weighted_avg = (df['score'] * df['confidence']).sum() / df['confidence'].sum()
```

### 4. Confidence Threshold Sensitivity Analysis
```python
for threshold in [0.5, 0.7, 0.8, 1.0]:
    filtered = df[df['confidence'] >= threshold]
    print(f"Threshold {threshold}: {len(filtered)} tweets")
```

## Directory Structure

```
/n/netscratch/cga/Lab/xiaokang/US-Census-TGSI-workspace/data/
├── tweets_with_census_blocks/           # Original 0.3.2 output (preserved)
│   ├── 2010/
│   ├── 2011/
│   └── ...
└── tweets_with_census_blocks_confidence/  # New 0.3.3 output
    ├── 2010/
    │   ├── 2010_01_01_00-tl_2020_01_tabblock20.parquet
    │   ├── 2010_01_01_00-tl_2020_02_tabblock20.parquet
    │   └── ...
    ├── 2011/
    └── ...
```

## Next Steps

1. **Test run (single year):**
   ```bash
   conda activate geo
   python 0.3.3-spatial-join-with-confidence.py --year 2010
   ```

2. **Full production run:**
   ```bash
   sbatch 0.3.3-run-spatial-join-with-confidence.sh
   ```

3. **Via Snakemake:**
   ```bash
   snakemake spatial_join --cluster "sbatch -p {resources.partition} -c {resources.cpus} --mem={resources.mem_mb} -t {resources.time}"
   ```

## Performance Estimates

- Processing rate: ~45,000-67,000 tweets/sec (point-in-polygon with confidence calculation)
- Total files: ~93,574 input files × 51 states = ~4.77M output files
- Estimated runtime: ~40-50 hours on 110 cores with 900GB RAM

## Rationale

**Why separate output directory?**
- Preserves original 0.3.2 results for comparison
- Allows parallel workflows (GPS-only vs confidence-weighted)
- Enables validation studies comparing different spatial join strategies
- Safe rollback if issues discovered

**Why confidence weighting over filtering?**
- Retains all data (94% of tweets have >1km error)
- Enables sensitivity analysis across different quality thresholds
- Supports weighted statistical analysis
- More flexible than hard GPS-only filter
