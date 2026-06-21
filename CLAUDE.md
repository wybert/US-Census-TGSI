# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Context
**Title:** Twitter Sentiment Geographical Index Dataset For US in Census Level (US-CT-TSGI)
**Authors:** Xiaokang Fu, Devika Jain, Jack Hayes (Harvard CGA & Wuhan University)
**Goal:** Create a high-resolution, longitudinal (2010-2023) sentiment index at the **Census Tract** level from ~2 billion geotagged tweets using BERT-based sentiment scores and TIGER/Line 2020 Census Tract boundaries.

## Pipeline Overview (Snakemake on SLURM)
1. **01_data_acquisition**: Download Census TIGER/Line shapefiles; validate/recompute missing sentiment files.
2. **02_merging**: Combine raw tweets (TSV.GZ) with BERT sentiment scores via `message_id` join.
3. **03_spatial_join**: Point-in-polygon join (EPSG:4326) → assigns `GEOID20` to each tweet; 93,574 files × 51 states = ~4.77M ops.
4. **04_validation**:
   - `aggregation/`: DuckDB aggregation → daily/monthly/yearly parquet at block → tract level.
   - `representativeness/`: Coverage Ratio (CR), Gini/Lorenz, choropleth maps.
   - `correlation/`: Weighted Spearman/Pearson vs CDC PLACES health data.
   - `utils/`: Testing and helper scripts.

## Commands

### Snakemake
```bash
snakemake -n                           # Dry run
snakemake -j 4                         # Run locally
snakemake --profile config/slurm_profile  # Run on SLURM (uses config/slurm_profile/config.yaml)
snakemake --dag | dot -Tpng > outputs/pipeline_dag.png  # Visualize DAG
```

**Named target rules** (run a subset of the pipeline):
```bash
snakemake download_only          # Census TIGER/Line shapefiles only
snakemake check_data_quality     # Validate sentiment file completeness
snakemake validation_only        # CR + Gini validation only
snakemake correlation_only       # CDC PLACES correlation only
snakemake spatial_join_all       # Spatial join all years
snakemake clean                  # Remove output PNGs/CSVs/logs
snakemake clean_all              # Remove all generated data (destructive)
```

### HPC Execution (SLURM direct)
```bash
sbatch src/02_merging/0.2.1-combine-tweets-sentiment-slurm-run.sh
sbatch src/03_spatial_join/0.3.2-run-spatial-join.sh
```

### Python Environment
Conda env `geo` — activate via `/n/home11/xiaokangfu/.conda/envs/geo/bin/python`
Key packages: `geopandas`, `duckdb`, `pandarallel`, `scipy`, `matplotlib`.
Always call `pandarallel.initialize()` before `.parallel_apply()`.

## Configuration (`setting.json`)
All data paths are defined here and loaded by Snakemake (`configfile: "setting.json"`). Key paths:
- `workspace`: `/n/netscratch/cga/Lab/xiaokang/US-Census-TGSI-workspace` — intermediate data lives here
- `aggregated_sentiment_output`: `<workspace>/data/aggregated_sentiment_stats` — **final data product** (daily/monthly/yearly parquet)
- `geo_tweets_archive_base_path`, `sentiment_file_base_path`: raw inputs on holylabs
- `census_data_2020`: merged census block geoparquet
- `census_pop`: population parquet for CR calculations

## Repository Structure
- `src/`: Pipeline scripts numbered by step (e.g., `0.1-`, `0.3.9-`).
- `scripts/`: SLURM submission helpers (`0.4.3-submit-spatial-rep.sh`, etc.).
- `config/slurm_profile/config.yaml`: Default SLURM profile (sapphire partition, 900GB RAM, 1440 min).
- `docs/data-pipeline-flowchart.txt`: ASCII flowchart of full pipeline.
- `outputs/`: Local results — `validation/`, `gini/`, `correlation/`, `logs/`.
- `data/500-Cities-Places/`: CDC PLACES CSV files (2020–2024 releases).
- `paper/`: LaTeX manuscript source.
- `reference/`: Background papers.

## Key Metrics
- **CR (Coverage Ratio)**: `tweet_count / population` at block/tract level.
- **log2(CR)**: 0 = proportional, -1 = 50% underrepresented, +1 = 200% overrepresented.
- **mask_low_coverage**: Filter tracts with <20 tweets before correlation analysis.
- **Population weighting**: Use `weighted_corr()` / `weighted_spearman()` for tract-level stats.

## Important Notes
- **GEOID formats**: Block IDs = 15 digits; tract IDs = first 11 digits of GEOID20.
- **Projections**: Spatial joins use EPSG:4326 (WGS84); visualizations use EPSG:5070 (NAD83/Conus Albers).
- **DuckDB**: Use for out-of-core processing on block-level data; avoid loading full datasets into pandas.
- **Spatial join scale**: 900GB+ RAM, ~40-50 hours on 110 cores for the full 2010-2023 run.
- **Spatial join output**: Each input file → 51 per-state parquets (e.g., `2020_10_01_00-tl_2020_06_tabblock20.parquet`).
- **Script naming**: All scripts start with a number prefix indicating step order.
- **Keep updated**: After any pipeline change, update `Snakefile` and `docs/data-pipeline-flowchart.txt`.
- **Scope**: Only create or edit files within this repository directory.

## References
- `reference/Chai et al. - 2023 - Twitter Sentiment Geographical Index Dataset.pdf` (Scientific Data, 2023) — validation methodology reference.
