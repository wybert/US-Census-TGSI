# US-Census-TGSI Pipeline Documentation

## Overview

This project uses **Snakemake** to manage the complete data processing pipeline from raw data download to final correlation analysis.

## Quick Start

### 1. Install Snakemake (if not already installed)

```bash
conda install -c conda-forge -c bioconda snakemake
# or
pip install snakemake
```

### 2. Dry Run (See what would execute)

```bash
snakemake -n
```

### 3. Run Specific Targets

```bash
# Download census data only
snakemake download_only -j 1

# Run validation analysis only
snakemake validation_only -j 4

# Run correlation analysis only
snakemake correlation_only -j 4

# Run complete pipeline
snakemake all -j 4
```

### 4. Run on SLURM Cluster

```bash
# Submit jobs to SLURM
snakemake --cluster "sbatch -p {resources.partition} -c {resources.cpus} --mem={resources.mem_mb} -t {resources.time} -o outputs/logs/{rule}_%j.out -e outputs/logs/{rule}_%j.err" -j 10

# Or use the cluster profile (recommended)
snakemake --profile slurm -j 20
```

## Pipeline Structure

### Stage 0.1: Data Acquisition
- **Rule**: `download_census_data`
- **Output**: Census TIGER/Line shapefiles for all 51 states
- **Resources**: 1 CPU, 2GB RAM, 2 hours
- **Script**: `0.1-download_cenus_data.py`

### Stage 0.2: Tweet-Sentiment Merging
- **Rule**: `merge_tweets_sentiment`
- **Input**: Raw tweets + BERT sentiment scores
- **Output**: Merged parquet files (2010-2023)
- **Resources**: 110 CPUs, 100GB RAM, 12 hours
- **Script**: `0.2.1-combine-geo-tweets-archive-and-sentiment.py`

### Stage 0.3: Spatial Join
- **Rule**: `spatial_join`
- **Input**: Merged tweets + Census blocks
- **Output**: Tweets with GEOID20 assignments
- **Resources**: 110 CPUs, 900GB RAM, 3 days
- **Script**: `0.3.2-xiaokang-sjoin-geopandas-us-census-script-version.py`

### Stage 0.4: Validation & Aggregation
1. **aggregate_tweet_counts**: DuckDB aggregation by GEOID20
2. **calculate_coverage_ratio**: CR and log2CR metrics
3. **spatial_representation**: Merge with geometries
4. **validation_histogram**: Generate visualizations
5. **validation_classification**: Classified maps
6. **gini_analysis**: Gini coefficient and Lorenz curves

### Stage 0.6: Correlation Analysis
1. **aggregate_to_tract_level**: Block → Tract aggregation + PLACES join
2. **correlation_analysis**: Sentiment vs. health indicators
3. **correlation_plots**: Enhanced visualizations with LOWESS

## Dependency Graph

Generate a visual DAG of the pipeline:

```bash
# Generate PNG (requires graphviz)
snakemake --dag | dot -Tpng > outputs/pipeline_dag.png

# Generate PDF
snakemake --dag | dot -Tpdf > outputs/pipeline_dag.pdf

# View rules DAG
snakemake --rulegraph | dot -Tpng > outputs/pipeline_rulegraph.png
```

## Common Operations

### Check What Will Run

```bash
# Dry run with reason
snakemake -n -r

# Print shell commands that would be executed
snakemake -n -p
```

### Run Specific Rules

```bash
# Run only the gini analysis
snakemake gini_analysis

# Force re-run of a specific rule
snakemake -f correlation_analysis

# Run up to a certain rule (and all dependencies)
snakemake aggregate_tweet_counts
```

### Monitor Progress

```bash
# Show detailed output
snakemake -j 4 --verbose

# Keep going even if some jobs fail
snakemake -j 4 --keep-going
```

### Clean Outputs

```bash
# Remove output plots/CSVs (safe)
snakemake clean

# Remove ALL generated data (DANGEROUS!)
snakemake clean_all
```

## SLURM Cluster Configuration

### Option 1: Command-line

```bash
snakemake \
  --cluster "sbatch -p {resources.partition} -c {resources.cpus} --mem={resources.mem_mb} -t {resources.time} -o outputs/logs/{rule}_%j.out -e outputs/logs/{rule}_%j.err" \
  --jobs 20 \
  --latency-wait 60
```

### Option 2: Cluster Profile (Recommended)

Create `~/.config/snakemake/slurm/config.yaml`:

```yaml
cluster: "sbatch -p {resources.partition} -c {resources.cpus} --mem={resources.mem_mb} -t {resources.time} -o outputs/logs/{rule}_%j.out -e outputs/logs/{rule}_%j.err"
jobs: 20
latency-wait: 60
use-conda: false
printshellcmds: true
```

Then run:
```bash
snakemake --profile slurm
```

## Resource Requirements by Rule

| Rule | CPUs | Memory | Time | Partition |
|------|------|--------|------|-----------|
| download_census_data | 1 | 2GB | 2h | shared |
| merge_tweets_sentiment | 110 | 100GB | 12h | sapphire |
| spatial_join | 110 | 900GB | 3d | sapphire |
| aggregate_tweet_counts | 4 | 32GB | 2h | shared |
| calculate_coverage_ratio | 4 | 16GB | 1h | shared |
| spatial_representation | 110 | 100GB | 4h | sapphire |
| validation_* | 4 | 32GB | 1h | shared |
| gini_analysis | 2 | 16GB | 30m | shared |
| aggregate_to_tract_level | 8 | 64GB | 2h | shared |
| correlation_analysis | 4 | 32GB | 1h | shared |

## Troubleshooting

### Issue: "MissingInputException"
**Cause**: Input file doesn't exist
**Solution**: Run the upstream rule first or check if file was deleted

### Issue: "MissingOutputException"
**Cause**: Rule completed but didn't create expected output
**Solution**: Check log file in `outputs/logs/` for errors

### Issue: "Incomplete files"
**Cause**: Job was killed or failed
**Solution**: Remove incomplete files and re-run
```bash
rm <incomplete_file>
snakemake <target> -f
```

### Issue: Jobs not submitting to SLURM
**Cause**: Cluster command syntax error
**Solution**: Test SLURM command manually:
```bash
sbatch -p shared -c 4 --mem=16000 -t 01:00:00 --wrap="echo test"
```

## Best Practices

1. **Always dry-run first**: `snakemake -n` before actual execution
2. **Use specific targets**: Don't run `all` unless you need everything
3. **Monitor logs**: Check `outputs/logs/*.log` for errors
4. **Checkpoint progress**: Snakemake automatically resumes from where it left off
5. **Version control**: Commit Snakefile changes to git

## Advanced Usage

### Parallel Processing by Year

The current Snakefile processes all years together. To parallelize by year, modify rules to use wildcards:

```python
rule merge_tweets_sentiment_by_year:
    output:
        config['geotweets_with_sentiment'] + "/{year}/.complete"
    wildcard_constraints:
        year="\d{4}"
    shell:
        "python 0.2.1-combine-geo-tweets-archive-and-sentiment.py --year {wildcards.year}"
```

### Generate Reports

```bash
# Create HTML report with statistics
snakemake --report outputs/report.html
```

### Benchmark Rules

Add to any rule:
```python
benchmark:
    "outputs/benchmarks/{rule}.txt"
```

## Pipeline Outputs

All outputs are organized in `outputs/`:

```
outputs/
├── validation/          # Validation plots
│   ├── log2CR_by_census_tract.png
│   └── log2CR_userdefined_7class.png
├── correlation/         # Correlation analysis
│   ├── places_correlation_summary.csv
│   └── scatter_sent_vs_MHLTH_*.png
├── gini/               # Representativeness metrics
│   ├── lorenz_curve.png
│   └── gini-summary.txt
└── logs/               # Execution logs
    └── *.log
```

## See Also

- [Snakemake Documentation](https://snakemake.readthedocs.io/)
- [SLURM Documentation](https://slurm.schedmd.com/)
- Project-specific documentation: `CLAUDE.md`
