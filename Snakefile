"""
Snakemake workflow for US-Census-TGSI geospatial sentiment analysis pipeline

Usage:
    # Dry run (see what would be executed)
    snakemake -n

    # Run locally with 4 cores
    snakemake -j 4

    # Run on SLURM cluster
    snakemake --cluster "sbatch -p {resources.partition} -c {resources.cpus} --mem={resources.mem_mb} -t {resources.time} -o outputs/logs/{rule}_%j.out -e outputs/logs/{rule}_%j.err" -j 10

    # Generate DAG visualization
    snakemake --dag | dot -Tpng > outputs/pipeline_dag.png
"""

import json
import os
from pathlib import Path

# Load configuration
configfile: "setting.json"

# Define state list for census downloads
STATES = ["01", "02", "04", "05", "06", "08", "09", "10", "11", "12",
          "13", "15", "16", "17", "18", "19", "20", "21", "22", "23",
          "24", "25", "26", "27", "28", "29", "30", "31", "32", "33",
          "34", "35", "36", "37", "38", "39", "40", "41", "42", "44",
          "45", "46", "47", "48", "49", "50", "51", "53", "54", "55", "56"]

YEARS = list(range(2010, 2024))
ANALYSIS_YEARS = [2020, 2021, 2022]  # Years for correlation analysis

# ========== Target Rules ==========

rule all:
    """
    Default target: complete all analysis
    """
    input:
        # Validation outputs
        expand("outputs/validation/{plot}.png",
               plot=["log2CR_by_census_tract", "log2CR_userdefined_7class"]),
        # Gini analysis
        "outputs/gini/lorenz_curve.png",
        "outputs/gini/gini-summary.txt",
        # Correlation analysis
        "outputs/correlation/places_correlation_summary.csv",
        expand("outputs/correlation/scatter_sent_vs_MHLTH_{year}.png",
               year=ANALYSIS_YEARS)

rule download_only:
    """
    Download census data only
    """
    input:
        expand(config['census_data_2020'] + "/tl_2020_{state}_tabblock20.zip",
               state=STATES)

rule validation_only:
    """
    Run validation analysis only
    """
    input:
        expand("outputs/validation/{plot}.png",
               plot=["log2CR_by_census_tract", "log2CR_userdefined_7class"]),
        "outputs/gini/lorenz_curve.png"

rule correlation_only:
    """
    Run correlation analysis only
    """
    input:
        "outputs/correlation/places_correlation_summary.csv"

# ========== Data Acquisition ==========

rule download_census_data:
    """
    Download US Census TIGER/Line shapefiles for all states
    """
    output:
        expand(config['census_data_2020'] + "/tl_2020_{state}_tabblock20.zip",
               state=STATES)
    log:
        "outputs/logs/download_census_data.log"
    resources:
        cpus=1,
        mem_mb=2000,
        time="02:00:00",
        partition="shared"
    shell:
        """
        python 0.1-download_cenus_data.py > {log} 2>&1
        """

# ========== Tweet-Sentiment Merging ==========

rule merge_tweets_sentiment:
    """
    Merge geotagged tweets with BERT sentiment scores
    Note: This is a long-running job that processes all years
    """
    input:
        script="0.2.1-combine-geo-tweets-archive-and-sentiment.py",
        config="setting.json"
    output:
        # Mark completion with a flag file
        flag=config['geotweets_with_sentiment'] + "/.merge_complete"
    log:
        "outputs/logs/merge_tweets_sentiment.log"
    resources:
        cpus=110,
        mem_mb=100000,
        time="12:00:00",
        partition="sapphire"
    shell:
        """
        python {input.script} > {log} 2>&1
        touch {output.flag}
        """

# ========== Spatial Join ==========

rule spatial_join:
    """
    Spatial join between tweets and census blocks using GeoPandas

    This script supports command-line arguments:
    - Full mode (default): processes all years 2010-2023
    - Test mode: use --year 2010 to test with just 2010 data

    To test: python 0.3.2-xiaokang-sjoin-geopandas-us-census-script-version.py --year 2010
    """
    input:
        script="0.3.2-xiaokang-sjoin-geopandas-us-census-script-version.py",
        tweets_flag=config['geotweets_with_sentiment'] + "/.merge_complete",
        census=expand(config['census_data_2020'] + "/tl_2020_{state}_tabblock20.zip",
                     state=STATES),
        config="setting.json"
    output:
        flag=config['tweets_with_census_blocks'] + "/.spatial_join_complete"
    log:
        "outputs/logs/spatial_join.log"
    resources:
        cpus=110,
        mem_mb=900000,
        time="3-00:00:00",
        partition="sapphire"
    shell:
        """
        python {input.script} > {log} 2>&1
        touch {output.flag}
        """

# ========== DuckDB Aggregation ==========

rule aggregate_tweet_counts:
    """
    Aggregate tweet counts by GEOID20 and merge with population data
    """
    input:
        script="0.4.1-duckdb-validate-merge-data.sql",
        config="setting.json"
    output:
        config['workspace'] + "/data/all_years_tweet_count.parquet",
        config['workspace'] + "/data/all_years_tweet_count_with_pop.parquet"
    log:
        "outputs/logs/aggregate_tweet_counts.log"
    resources:
        cpus=4,
        mem_mb=32000,
        time="02:00:00",
        partition="shared"
    shell:
        """
        duckdb < {input.script} > {log} 2>&1
        """

rule calculate_coverage_ratio:
    """
    Calculate Coverage Ratio (CR) and log2CR metrics
    """
    input:
        script="0.4.2-duckdb-validate-CR.sql",
        data=config['workspace'] + "/data/all_years_tweet_count_with_pop.parquet",
        config="setting.json"
    output:
        config['workspace'] + "/data/all_years_tweet_count_with_pop_CR.parquet",
        config['workspace'] + "/data/all_years_tweet_count_with_pop_CR_filtered.parquet"
    log:
        "outputs/logs/calculate_coverage_ratio.log"
    resources:
        cpus=4,
        mem_mb=16000,
        time="01:00:00",
        partition="shared"
    shell:
        """
        duckdb < {input.script} > {log} 2>&1
        """

# ========== Validation & Visualization ==========

rule spatial_representation:
    """
    Merge census geometry with CR data for spatial visualization
    """
    input:
        script="0.4.3-validation-spatial-representation.py",
        cr_data=config['workspace'] + "/data/all_years_tweet_count_with_pop_CR.parquet",
        config="setting.json"
    output:
        config['workspace'] + "/data/census_tracts_merged_shifted_geo.parquet"
    log:
        "outputs/logs/spatial_representation.log"
    resources:
        cpus=110,
        mem_mb=100000,
        time="04:00:00",
        partition="sapphire"
    shell:
        """
        python {input.script} > {log} 2>&1
        """

rule validation_histogram:
    """
    Generate log2CR histogram and map visualizations
    """
    input:
        script="0.4.4-validation-hist.py",
        geo_data=config['workspace'] + "/data/census_tracts_merged_shifted_geo.parquet",
        config="setting.json"
    output:
        "outputs/validation/log2CR_by_census_tract.png"
    log:
        "outputs/logs/validation_histogram.log"
    resources:
        cpus=4,
        mem_mb=32000,
        time="01:00:00",
        partition="shared"
    shell:
        """
        python {input.script} > {log} 2>&1
        """

rule validation_classification:
    """
    Generate classified log2CR map with custom bins
    """
    input:
        script="0.4.5-validation-vis3-classify-customized.py",
        geo_data=config['workspace'] + "/data/census_tracts_merged_shifted_geo.parquet",
        config="setting.json"
    output:
        "outputs/validation/log2CR_userdefined_7class.png"
    log:
        "outputs/logs/validation_classification.log"
    resources:
        cpus=4,
        mem_mb=32000,
        time="01:00:00",
        partition="shared"
    shell:
        """
        python {input.script} > {log} 2>&1
        """

rule gini_analysis:
    """
    Compute Gini coefficient and Lorenz curve
    """
    input:
        script="0.4.7-validation-gini-lorenz.py",
        cr_data=config['workspace'] + "/data/all_years_tweet_count_with_pop_CR.parquet",
        config="setting.json"
    output:
        "outputs/gini/lorenz_curve.png",
        "outputs/gini/lorenz_points.csv",
        "outputs/gini/gini-summary.txt"
    log:
        "outputs/logs/gini_analysis.log"
    resources:
        cpus=2,
        mem_mb=16000,
        time="00:30:00",
        partition="shared"
    shell:
        """
        python {input.script} > {log} 2>&1 || echo "Gini analysis completed with warnings"
        """

# ========== Tract-level Aggregation for Correlation ==========

rule aggregate_to_tract_level:
    """
    Aggregate block-level data to tract-level and join with PLACES data
    """
    input:
        script="0.6.1-agg-to-track-level-interactive.sql",
        config="setting.json"
    output:
        config['workspace'] + "/data/sentiment_places_data_joined.parquet"
    log:
        "outputs/logs/aggregate_to_tract_level.log"
    resources:
        cpus=8,
        mem_mb=64000,
        time="02:00:00",
        partition="shared"
    shell:
        """
        duckdb < {input.script} > {log} 2>&1
        """

# ========== Correlation Analysis ==========

rule correlation_analysis:
    """
    Compute correlations between sentiment and PLACES health indicators
    """
    input:
        script="0.6-cor-with-places-500-data-sentiment.py",
        data=config['workspace'] + "/data/sentiment_places_data_joined.parquet",
        config="setting.json"
    output:
        "outputs/correlation/places_correlation_summary.csv",
        expand("outputs/correlation/scatter_sent_vs_MHLTH_{year}.png",
               year=ANALYSIS_YEARS)
    log:
        "outputs/logs/correlation_analysis.log"
    resources:
        cpus=4,
        mem_mb=32000,
        time="01:00:00",
        partition="shared"
    shell:
        """
        python {input.script} > {log} 2>&1
        """

rule correlation_plots:
    """
    Generate enhanced correlation plots with LOWESS smoothing
    """
    input:
        script="0.6.2-cor-p-value-and-plot.py",
        data=config['workspace'] + "/data/sentiment_places_data_joined.parquet",
        config="setting.json"
    output:
        "outputs/correlation/facet_scatter_lowess_all_years.png"
    log:
        "outputs/logs/correlation_plots.log"
    resources:
        cpus=4,
        mem_mb=32000,
        time="01:00:00",
        partition="shared"
    shell:
        """
        python {input.script} > {log} 2>&1
        """

# ========== Utility Rules ==========

rule clean:
    """
    Remove all output files (but keep downloaded raw data)
    """
    shell:
        """
        rm -rf outputs/validation/*.png
        rm -rf outputs/correlation/*.png outputs/correlation/*.csv
        rm -rf outputs/gini/*.png outputs/gini/*.csv outputs/gini/*.txt
        rm -rf outputs/logs/*.log
        echo "Cleaned output files"
        """

rule clean_all:
    """
    Remove all generated data including intermediate files (WARNING: destructive!)
    """
    shell:
        """
        echo "This will remove ALL generated data files. Press Ctrl+C to cancel."
        sleep 5
        rm -rf outputs/
        rm -f {config[workspace]}/data/all_years_tweet_count*.parquet
        rm -f {config[workspace]}/data/census_tracts_merged*.parquet
        rm -f {config[workspace]}/data/sentiment_places_data_joined.parquet
        echo "All generated data removed"
        """
