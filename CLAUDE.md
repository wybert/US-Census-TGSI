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
snakemake verify_recomputed_sentiment  # Verify GPU-recomputed sentiment files (verify-only, no --copy)
snakemake tier_sensitivity_all   # CR/Gini/correlation recomputed under medium/high/strict confidence tiers
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

**Secrets (`.env`, gitignored, not `setting.json`):** `CENSUS_API_KEY` — required by `download_census_population` (calls `api.census.gov`), loaded via `python-dotenv`. The TIGER/Line shapefile download (`download_census_data`, `www2.census.gov`) needs no key. Copy `.env` from a teammate or generate a new key at https://api.census.gov/data/key_signup.html; the script runs without one but gets aggressively rate-limited.

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
- **is_artifact (coordinate-artifact blocks)**: Post-2019 place-tagged (non-GPS) tweets resolve to a fixed named-place centroid; when that centroid falls inside a near-empty census block, tweets collapse onto one block nationwide (e.g. the geographic center of Texas). Flagged as `population < 50 AND tweet_count > 5,000` (2020-only scripts) or `> 50,000` (all-years scripts) and **excluded** from CR/Gini normalization totals and tract rollups — not just flagged and left in. See `03_calculate_cr_2020.py` / `03_calculate_cr_all_years.py` / `05_prepare_correlation_tracts.py`. **This `is_artifact` rule only catches the extreme (near-empty-block) case** — city-level place-tag centroids landing on blocks with ordinary (not near-zero) population slip through it entirely (confirmed: 558 blocks, 18% of the "All" tier's reportable tweet total). These are already caught correctly by the per-tweet `confidence` score (spatialerror vs. block_diameter — see below), just not by `is_artifact`'s population-based heuristic. The confidence-tier sensitivity analysis (`06_tier_comparison_summary.py`, `tier_sensitivity_all` Snakemake target) quantifies the resulting effect: Gini computed on the `All` tier is ~30% higher than on `Medium`/`High`/`Strict` (which structurally exclude these low-confidence artifacts), while Medium/High/Strict agree with each other closely. Paper reports both: `All`-tier numbers as the headline (Table 3, Figures), plus a "Confidence-Tier Sensitivity" subsection with a table showing all four tiers side by side.

## Important Notes
- **GEOID formats**: Block IDs = 15 digits; tract IDs = first 11 digits of GEOID20.
- **Projections**: Spatial joins use EPSG:4326 (WGS84); visualizations use EPSG:5070 (NAD83/Conus Albers).
- **DuckDB**: Use for out-of-core processing on block-level data; avoid loading full datasets into pandas.
- **Spatial join scale**: 900GB+ RAM, ~40-50 hours on 110 cores for the full 2010-2023 run.
- **Spatial join output**: Each input file → 51 per-state parquets (e.g., `2020_10_01_00-tl_2020_06_tabblock20.parquet`).
- **Block vs. tract Gini are NOT interchangeable**: block-level Gini is inflated by small-area sampling noise (MAUP), not a finer-grained version of the tract-level statistic. Any figure/table described as "tract-level" in the paper must be computed from tract-aggregated inputs — `gini_analysis`/`gini_analysis_2020` read `cr_data` from the tract-merged geo files, not block-level CR output.
- **Script naming**: All scripts start with a number prefix indicating step order.
- **Keep updated**: After any pipeline change, update `Snakefile` and `docs/data-pipeline-flowchart.txt`.
- **Scope**: Only create or edit files within this repository directory.

## References
- `reference/Chai et al. - 2023 - Twitter Sentiment Geographical Index Dataset.pdf` (Scientific Data, 2023) — validation methodology reference.
