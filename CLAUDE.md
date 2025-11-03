# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

US-Census-TGSI is a geospatial sentiment analysis research project that correlates geotagged Twitter data with US Census demographics and CDC PLACES health indicators at the census tract and block level. The pipeline processes millions of tweets across multiple years (2010-2024), merges them with sentiment scores, aggregates spatial statistics, and validates correlations with health outcomes.

## Configuration

**Central Configuration**: `setting.yaml` defines all data paths:
- `geo_tweets_archive_base_path`: Raw geotagged tweets archive
- `sentiment_file_base_path`: Pre-computed BERT sentiment scores
- `workspace`: Main working directory for intermediate/output data
- `census_pop`: Census population data location
- `500-Cities-Places`: CDC PLACES health indicator datasets
- `census_geometry`: Census geometries (blocks/tracts)

## Pipeline Architecture

The analysis follows a numbered sequential workflow:

### 0.1 - Data Acquisition
`0.1-download_cenus_data.py`: Downloads US Census TIGER/Line shapefiles (TABBLOCK20 format) for all 50 states + DC from census.gov. Uses `wget` for batch downloading.

### 0.2-0.3 - Tweet-Sentiment Merging
- `0.2-combine-geo-tweets-archive-and-sentiment.py`: Main parallel processing script that merges raw tweets with sentiment scores using `pandarallel`. Reads yearly CSV.GZ archives, joins on `message_id`, outputs `.parquet` files.
- `0.2-combine-tweets-sentiment-slurm-run.sh`: SLURM job script (110 cores, 12 hours, 100GB RAM on sapphire partition)
- `0.2.2-combine-geo-tweets-archive-and-sentiment_fault2.py`: Fault-tolerant variant for handling failed merges

Key columns after merge: `message_id`, `latitude`, `longitude`, `score` (sentiment), `date` (timestamp)

### 0.3.2 - Spatial Join with Census Blocks
Enriches sentiment tweets with census block geography through point-in-polygon spatial joins:
- `0.3.2-xiaokang-sjoin-geopandas-us-census-script-version.py`: Parallel spatial join script using GeoPandas to match tweet coordinates with US Census 2020 block geometries
  - Uses `geopandas.sjoin()` for point-in-polygon matching (EPSG:4326)
  - Implements `pandarallel` for parallel processing across all state shapefiles
  - Input: `/data/geotweets_with_sentiment/{year}/*.parquet` (~93,574 files for 2010-2024)
  - Census data: 51 state TIGER/Line shapefiles from `/data/census_data_2020/`
  - Output: `/data/tweets_with_census_blocks/{year}/*-{state}.parquet`
- `0.3.2-run-spatial-join.sh`: SLURM job script (110 cores, 900GB RAM, 3 days, sapphire partition)

**Key output columns**: Original tweet fields + `GEOID20` (15-digit block ID), `STATEFP20`, `COUNTYFP20`, `TRACTCE20`, `BLOCKCE20`, `block_population`, `block_housing_units`, `block_land_area`, `block_water_area`

**Processing scale**: Each input file generates 51 output files (one per state), resulting in ~4.77M spatial join operations for the full dataset. Estimated runtime: 40-50 hours.

### 0.4.x - Validation & Visualization
SQL and Python scripts for data quality checks:
- `0.4.1-duckdb-validate-merge-data.sql`: DuckDB aggregation queries joining tweet counts with census population
- `0.4.2-duckdb-validate-CR.sql`: Validates Coverage Ratio (CR) calculations
- `0.4.3-validation-spatial-representation.py`: GeoPandas spatial coverage validation
- `0.4.4-validation-hist.py`: Generates log2(CR) choropleth maps with missing value masking
- `0.4.5-validation-vis3-classify-customized.py`: Custom classification visualizations
- `0.4.7-validation-gini-lorenz.py`: Computes Gini coefficients and Lorenz curves for representativeness

### 0.6.x - Statistical Analysis
Correlation analysis with CDC PLACES health data:
- `0.6-cor-with-places-500-data-sentiment.py`: Main validation script computing Spearman/Pearson correlations (weighted and unweighted) between tract-level sentiment and health indicators (e.g., `MHLTH_CrudePrev` for mental distress). Generates scatter plots with decile curves.
- `0.6.1-agg-to-track-level-interactive.sql`: DuckDB aggregation from block→tract level with population weighting
- `0.6.2-cor-p-value-and-plot.py`: Statistical significance testing and publication-ready plots

## Key Metrics

**CR (Coverage Ratio)**: `tweet_count / population` at block/tract level
**log2(CR)**: Symmetrically scaled metric where 0 = proportional representation, -1 = 50% underrepresented, +1 = 200% overrepresented
**mask_low_coverage**: Binary flag filtering tracts with <20 tweets to avoid spurious correlations

## Running the Pipeline

### HPC Execution (SLURM)
```bash
# Step 1: Merge tweets with sentiment scores
sbatch 0.2.1-combine-tweets-sentiment-slurm-run.sh

# Step 2: Spatial join with census blocks
sbatch 0.3.2-run-spatial-join.sh
```

### DuckDB Interactive Analysis
```bash
duckdb -init 0.6.1-agg-to-track-level-interactive.sql
```
For non-interactive execution, use `.read` or `-c` flag with individual SQL files.

### Python Environment
Uses conda environment `geo` with dependencies:
- `pandarallel`: Parallel pandas operations
- `geopandas`: Spatial data handling
- `duckdb`: Columnar analytics
- Standard scipy/numpy/matplotlib stack

Activate: `/n/home11/xiaokangfu/.conda/envs/geo/bin/python`

## Data Flow

1. **Raw tweets** (TSV.GZ, ~TB scale) → **Sentiment scores** (TSV.GZ per file)
2. **Merged parquet** files (tweets + sentiment) → **Spatial join** with census blocks → **Tweets with GEOID20**
3. **Tweets with GEOID20** → **DuckDB aggregation** to daily/yearly by census block
4. **Census population** (parquet) joins with **tweet counts** → CR calculations
5. **Block-level** aggregates → **Tract-level** (population-weighted averaging)
6. **Tract sentiment** + **CDC PLACES** → Correlation validation

## Important Notes

- **GEOID formats**: Block IDs are 15 digits (state+county+tract+block), tract IDs are first 11 digits
- **Population weighting**: Critical for representative statistics; use `weighted_corr()` and `weighted_spearman()` functions
- **Parallel processing**: Set `pandarallel.initialize()` before using `.parallel_apply()`
- **Memory considerations**: Block-level datasets are massive; use DuckDB for out-of-core processing where possible
- **Spatial join performance**: Processing full dataset (2010-2024, ~93,574 files × 51 states) requires 900GB+ RAM and ~40-50 hours on 110 cores
- **Spatial join output format**: Each input file generates 51 output files (one per state), e.g., `2020_10_01_00-tl_2020_06_tabblock20.parquet`
- **Projection**: Use EPSG:5070 (NAD83/Conus Albers) for continental US visualizations; spatial joins use EPSG:4326 (WGS84)
- remember only create/edit the files under this very current folder.