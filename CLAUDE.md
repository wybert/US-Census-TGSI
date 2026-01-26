# US-Census-TGSI Project Guide

## Project Context
**Title:** Twitter Sentiment Geographical Index Dataset For US in Census Level (US-CT-TSGI)
**Authors:** Xiaokang Fu, Devika Jain, Jack Hayes (Harvard CGA & Wuhan University)
**Goal:** Create a high-resolution, longitudinal (2010-2023) sentiment index for the United States at the **Census Tract** level.
**Methodology:**
1.  **Input:** ~2 billion geotagged tweets (2010-2023) + BERT-based sentiment scores.
2.  **Process:** Spatial join with TIGER/Line 2020 Census Tract boundaries.
3.  **Output:** Daily, monthly, and yearly sentiment scores and tweet counts for 8.18 million US census tracts.
4.  **Validation:** Coverage Ratio (CR), Spatial Representativeness (Gini/Lorenz), and Correlation with CDC PLACES health data (Mental Health).

## Pipeline Overview
The project uses **Snakemake** for orchestration on a SLURM cluster.
1.  **01_data_acquisition**: Download Census TIGER/Line shapefiles.
2.  **02_merging**: Combine raw tweets with sentiment scores.
3.  **03_spatial_join**: Map tweets to Census Blocks/Tracts (High Compute).
4.  **04_validation**: Validation suite including:
    *   Coverage Ratio (CR) & Spatial Representativeness (Gini/Lorenz)
    *   Correlation Analysis with CDC PLACES health data (Mental Health)

## Repository Structure
- `src/`: Source code grouped by pipeline stage.
    - `01_data_acquisition/`
    - `02_merging/`
    - `03_spatial_join/`
    - `04_validation/`
        - `aggregation/`: Block/Tract aggregation logic
        - `representativeness/`: CR, Gini, Maps
        - `correlation/`: Health data correlation
        - `utils/`: Testing and helper scripts
- `scripts/`: Helper and submission scripts.
- `config/`: Configuration files (env, json).
- `docs/`: Documentation (Pipeline, setup, logs).
- `paper/`: Latex source for the manuscript "Twitter Sentiment Geographical Index Dataset...".
- `Snakefile`: Main workflow definition.
- `setting.json`: Global configuration.

## Commands
- **Dry Run:** `snakemake -n`
- **Run Local:** `snakemake -j 4`
- **Run SLURM:** `snakemake --profile config/slurm_profile`
- **Help:** `/help`

## Development Status
- **Current Task:** Consolidating paper draft with codebase and validating pipeline execution.
- **Primary Data Product:** The final dataset for publication is the **Aggregated Sentiment Statistics** (Daily/Monthly/Yearly) at the Census Tract level.
    - **Path:** `/n/netscratch/cga/Lab/xiaokang/US-Census-TGSI-workspace/data/aggregated_sentiment_stats`
    - **Format:** Parquet files (e.g., `daily_2022.parquet`, `monthly_2022.parquet`, `yearly_2022.parquet`)
- **Paper Repo:** Merged from `wybert/US-census-TGSI-paper` into `paper/`.


## Key Metrics

**CR (Coverage Ratio)**: `tweet_count / population` at block/tract level
**log2(CR)**: Symmetrically scaled metric where 0 = proportional representation, -1 = 50% underrepresented, +1 = 200% overrepresented
**mask_low_coverage**: Binary flag filtering tracts with <20 tweets to avoid spurious correlations

## Running the Pipeline

### HPC Execution (SLURM)
```bash
# Step 1: Merge tweets with sentiment scores
sbatch src/02_merging/0.2.1-combine-tweets-sentiment-slurm-run.sh

# Step 2: Spatial join with census blocks
sbatch src/03_spatial_join/0.3.2-run-spatial-join.sh
```

### DuckDB Interactive Analysis
```bash
duckdb -init src/04_validation/aggregation/0.6.1-agg-to-track-level-interactive.sql
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
- I am using geo conda env to work
- I am using snakemake to connect the pipline
- please check and keep update the Snakefile and data-pipeline-flowchart.txt if needed after you change anything
- all the script should named starting with number to show the step order of the script

## References
- **Primary Reference**: `reference/Chai et al. - 2023 - Twitter Sentiment Geographical Index Dataset.pdf` (Scientific Data, 2023) - Validation methodology and background for the global/national sentiment index.