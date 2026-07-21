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
MONTHS = list(range(1, 13))  # Spatial join is parallelized per (year, month)
ANALYSIS_YEARS = [2020, 2021, 2022]  # Years for correlation analysis

# ========== Target Rules ==========

rule all:
    """
    Default target: complete all analysis
    """
    input:
        # Validation outputs (All Years)
        expand("outputs/validation/{plot}.png",
               plot=["log2CR_by_census_tract", "log2CR_userdefined_7class"]),
        # Gini analysis (All Years)
        "outputs/gini/lorenz_curve.png",
        "outputs/gini/gini-summary.txt",
        # Temporal stability (confidence-tier time series)
        "outputs/validation/temporal_stability.png",
        # Validation outputs (2020 Only)
        "outputs/validation/log2CR_userdefined_7class_2020.png",
        "outputs/gini/2020_lorenz_curve.png",
        "outputs/gini/2020_gini-summary.txt",
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

rule check_data_quality:
    """
    Check data quality before processing (sentiment file validation)
    """
    input:
        config['outputs_dir'] + "/missing_sentiment_summary.txt"

rule validation_only:
    """
    Run validation analysis only
    """
    input:
        expand("outputs/validation/{plot}.png",
               plot=["log2CR_by_census_tract", "log2CR_userdefined_7class"]),
        "outputs/gini/lorenz_curve.png",
        "outputs/validation/temporal_stability.png"

rule correlation_only:
    """
    Run correlation analysis only
    """
    input:
        "outputs/correlation/places_correlation_summary.csv"

rule spatial_join_all:
    """
    Run spatial join for all years (parallelized per year-month; join is single-threaded)
    """
    input:
        expand(config['tweets_with_census_blocks_confidence'] + "/{year}/.sj_{month}_complete",
               year=YEARS, month=MONTHS)

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
        runtime=120,
        partition="shared"
    shell:
        """
        python src/01_data_acquisition/0.1-download_cenus_data.py > {log} 2>&1
        """

# ========== Data Validation ==========

rule find_missing_sentiment:
    """
    Find missing sentiment files before merging
    """
    input:
        script="src/01_data_acquisition/0.1.5-find-missing-sentiment-files.py",
        config="setting.json"
    output:
        missing=config['outputs_dir'] + "/missing_sentiment_files.csv",
        existing=config['outputs_dir'] + "/existing_sentiment_files.csv",
        stats=config['outputs_dir'] + "/sentiment_files_statistics.csv",
        summary=config['outputs_dir'] + "/missing_sentiment_summary.txt"
    log:
        "outputs/logs/find_missing_sentiment.log"
    resources:
        cpus=1,
        mem_mb=4000,
        runtime=30,
        partition="shared"
    shell:
        """
        /n/home11/xiaokangfu/.conda/envs/geo/bin/python {input.script} > {log} 2>&1
        """

# ========== Tweet-Sentiment Merging ==========

rule merge_tweets_sentiment:
    """
    Merge geotagged tweets with BERT sentiment scores
    Note: This is a long-running job that processes all years
    """
    input:
        script="src/02_merging/0.2.1-combine-geo-tweets-archive-and-sentiment.py",
        config="setting.json",
        validation=config['outputs_dir'] + "/missing_sentiment_summary.txt"
    output:
        # Mark completion with a flag file
        flag=config['geotweets_with_sentiment'] + "/.merge_complete"
    log:
        "outputs/logs/merge_tweets_sentiment.log"
    resources:
        cpus=110,
        mem_mb=100000,
        runtime=720,
        partition="sapphire"
    shell:
        """
        /n/home11/xiaokangfu/.conda/envs/geo/bin/python {input.script} > {log} 2>&1
        touch {output.flag}
        """

# ========== Spatial Join ==========

rule spatial_join:
    """
    Spatial join between tweets and census blocks with confidence weighting.
    Parallelized per (year, month): the DuckDB ST_Within join is single-threaded
    (~4.5h for a high-volume month), so many small jobs run concurrently rather
    than one 100-core job idling on serial months.
    """
    input:
        script="src/03_spatial_join/0.3.9-run-2020-spatial-join.py",
        tweets_flag=config['geotweets_with_sentiment'] + "/.merge_complete",
        census=config['census_data_2020'] + "/us_census_blocks_2020.geoparquet", # Merged census data
        config="setting.json"
    output:
        # Per-(year,month) flag file to track completion
        flag=config['tweets_with_census_blocks_confidence'] + "/{year}/.sj_{month}_complete"
    log:
        "outputs/logs/spatial_join_confidence_{year}_{month}.log"
    resources:
        cpus=8,
        mem_mb=350000,
        runtime=600, # 10h: a single high-volume month takes ~5h single-threaded
        partition="sapphire"
    shell:
        """
        DUCKDB_MEMORY_LIMIT=320GB /n/home11/xiaokangfu/.conda/envs/geo/bin/python {input.script} --start-year {wildcards.year} --end-year {wildcards.year} --month {wildcards.month} > {log} 2>&1
        touch {output.flag}
        """

# ========== DuckDB Aggregation ==========

rule generate_aggregated_stats:
    """
    Stage 04A: aggregate spatial-join output to GEOID20 (block) level at
    daily/monthly/yearly granularity, confidence-stratified. Produces the
    final data product in aggregated_sentiment_output (the daily/monthly/yearly
    parquet that downstream CR / correlation steps consume).
    """
    input:
        script="src/04_validation/aggregation/01_generate_aggregated_stats.py",
        sj=expand(config['tweets_with_census_blocks_confidence'] + "/{year}/.sj_{month}_complete",
                  year=YEARS, month=MONTHS),
        config="setting.json"
    output:
        flag=config['aggregated_sentiment_output'] + "/.aggregation_complete",
        yearly_2020=config['aggregated_sentiment_output'] + "/yearly_2020.parquet"
    log:
        "outputs/logs/generate_aggregated_stats.log"
    resources:
        cpus=64,
        mem_mb=900000,
        runtime=720,
        partition="sapphire"
    shell:
        """
        /n/home11/xiaokangfu/.conda/envs/geo/bin/python {input.script} --start-year 2010 --end-year 2023 > {log} 2>&1
        touch {output.flag}
        """

rule aggregate_tweet_counts:
    """
    Aggregate tweet counts by GEOID20 and merge with population data
    """
    input:
        script="src/04_validation/aggregation/02_merge_counts_and_pop_all_years.py",
        agg_flag=config['aggregated_sentiment_output'] + "/.aggregation_complete",
        config="setting.json"
    output:
        config['workspace'] + "/data/all_years_tweet_count.parquet",
        config['workspace'] + "/data/all_years_tweet_count_with_pop.parquet"
    log:
        "outputs/logs/aggregate_tweet_counts.log"
    resources:
        cpus=4,
        mem_mb=32000,
        runtime=120,
        partition="shared"
    shell:
        """
        /n/home11/xiaokangfu/.conda/envs/geo/bin/python {input.script} > {log} 2>&1
        """

rule calculate_coverage_ratio:
    """
    Calculate Coverage Ratio (CR) and log2CR metrics (All Years)
    """
    input:
        script="src/04_validation/aggregation/03_calculate_cr_all_years.py",
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
        runtime=60,
        partition="shared"
    shell:
        """
        /n/home11/xiaokangfu/.conda/envs/geo/bin/python {input.script} > {log} 2>&1
        """

rule calculate_coverage_ratio_2020:
    """
    Calculate Coverage Ratio (CR) and log2CR metrics for Year 2020
    """
    input:
        script="src/04_validation/aggregation/03_calculate_cr_2020.py",
        tweets=config['aggregated_sentiment_output'] + "/yearly_2020.parquet",
        config="setting.json"
    output:
        config['workspace'] + "/data/tweet_count_2020_with_pop_CR.parquet"
    log:
        "outputs/logs/calculate_coverage_ratio_2020.log"
    resources:
        cpus=4,
        mem_mb=16000,
        runtime=60,
        partition="shared"
    shell:
        """
        /n/home11/xiaokangfu/.conda/envs/geo/bin/python {input.script} > {log} 2>&1
        """

# ========== Validation & Visualization ==========

rule spatial_representation:
    """
    Merge census geometry with CR data for spatial visualization (All Years)
    """
    input:
        script="src/04_validation/representativeness/02_merge_geometry.py",
        cr_data=config['workspace'] + "/data/all_years_tweet_count_with_pop_CR.parquet",
        config="setting.json"
    output:
        config['workspace'] + "/data/census_tracts_merged_shifted_geo.parquet"
    log:
        "outputs/logs/spatial_representation.log"
    resources:
        cpus=64,
        mem_mb=450000,
        runtime=240,
        partition="sapphire"
    shell:
        """
        /n/home11/xiaokangfu/.conda/envs/geo/bin/python {input.script} > {log} 2>&1
        """

rule spatial_representation_2020:
    """
    Merge census geometry with CR data for spatial visualization (2020 Only)
    """
    input:
        script="src/04_validation/representativeness/02_merge_geometry.py",
        cr_data=config['workspace'] + "/data/tweet_count_2020_with_pop_CR.parquet",
        config="setting.json"
    output:
        config['workspace'] + "/data/census_tracts_merged_shifted_geo_2020.parquet"
    log:
        "outputs/logs/spatial_representation_2020.log"
    resources:
        cpus=64,
        mem_mb=450000,
        runtime=240,
        partition="sapphire"
    shell:
        """
        /n/home11/xiaokangfu/.conda/envs/geo/bin/python {input.script} --input {input.cr_data} --output {output} > {log} 2>&1
        """

rule validation_histogram:
    """
    Generate log2CR histogram and map visualizations
    """
    input:
        script="src/04_validation/representativeness/04_plot_histogram.py",
        geo_data=config['workspace'] + "/data/census_tracts_merged_shifted_geo.parquet",
        config="setting.json"
    output:
        "outputs/validation/log2CR_by_census_tract.png"
    log:
        "outputs/logs/validation_histogram.log"
    resources:
        cpus=4,
        mem_mb=32000,
        runtime=60,
        partition="shared"
    shell:
        """
        /n/home11/xiaokangfu/.conda/envs/geo/bin/python {input.script} > {log} 2>&1
        """

rule temporal_stability:
    """
    Technical Validation: national tweet volume and tweet-weighted mean
    sentiment by confidence tier, 2010-2023 (Figure: temporal_stability.png)
    """
    input:
        script="src/04_validation/representativeness/06_temporal_stability.py",
        agg_flag=config['aggregated_sentiment_output'] + "/.aggregation_complete",
        config="setting.json"
    output:
        png="outputs/validation/temporal_stability.png",
        csv="outputs/validation/temporal_stability_stats.csv"
    log:
        "outputs/logs/temporal_stability.log"
    resources:
        cpus=4,
        mem_mb=16000,
        runtime=30,
        partition="shared"
    shell:
        """
        /n/home11/xiaokangfu/.conda/envs/geo/bin/python {input.script} > {log} 2>&1
        """

rule validation_classification:
    """
    Generate classified log2CR map with custom bins (All Years)
    """
    input:
        script="src/04_validation/representativeness/05_plot_maps_classified.py",
        geo_data=config['workspace'] + "/data/census_tracts_merged_shifted_geo.parquet",
        config="setting.json"
    output:
        "outputs/validation/log2CR_userdefined_7class.png"
    log:
        "outputs/logs/validation_classification.log"
    resources:
        cpus=4,
        mem_mb=32000,
        runtime=60,
        partition="shared"
    shell:
        """
        /n/home11/xiaokangfu/.conda/envs/geo/bin/python {input.script} > {log} 2>&1
        """

rule validation_classification_2020:
    """
    Generate classified log2CR map with custom bins (2020 Only)
    """
    input:
        script="src/04_validation/representativeness/05_plot_maps_classified.py",
        geo_data=config['workspace'] + "/data/census_tracts_merged_shifted_geo_2020.parquet",
        config="setting.json"
    output:
        "outputs/validation/log2CR_userdefined_7class_2020.png"
    log:
        "outputs/logs/validation_classification_2020.log"
    resources:
        cpus=4,
        mem_mb=32000,
        runtime=60,
        partition="shared"
    shell:
        """
        /n/home11/xiaokangfu/.conda/envs/geo/bin/python {input.script} --input {input.geo_data} --output {output} > {log} 2>&1
        """

rule gini_analysis:
    """
    Compute Gini coefficient and Lorenz curve (All Years)
    """
    input:
        script="src/04_validation/representativeness/03_calculate_gini_lorenz.py",
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
        runtime=30,
        partition="shared"
    shell:
        """
        /n/home11/xiaokangfu/.conda/envs/geo/bin/python {input.script} > {log} 2>&1 || echo "Gini analysis completed with warnings"
        """

rule gini_analysis_2020:
    """
    Compute Gini coefficient and Lorenz curve (2020 Only)
    """
    input:
        script="src/04_validation/representativeness/03_calculate_gini_lorenz.py",
        cr_data=config['workspace'] + "/data/tweet_count_2020_with_pop_CR.parquet",
        config="setting.json"
    output:
        "outputs/gini/2020_lorenz_curve.png",
        "outputs/gini/2020_lorenz_points.csv",
        "outputs/gini/2020_gini-summary.txt"
    log:
        "outputs/logs/gini_analysis_2020.log"
    resources:
        cpus=2,
        mem_mb=16000,
        runtime=30,
        partition="shared"
    shell:
        """
        /n/home11/xiaokangfu/.conda/envs/geo/bin/python {input.script} --input {input.cr_data} --output-prefix 2020_ > {log} 2>&1 || echo "Gini analysis 2020 completed with warnings"
        """

# ========== Tract-level Aggregation for Correlation ==========

rule aggregate_to_tract_level:
    """
    Aggregate block-level data to tract-level and join with PLACES data
    """
    input:
        script="src/04_validation/aggregation/05_prepare_correlation_tracts.py",
        agg_flag=config['aggregated_sentiment_output'] + "/.aggregation_complete",
        config="setting.json"
    output:
        config['workspace'] + "/data/sentiment_places_data_joined.parquet"
    log:
        "outputs/logs/aggregate_to_tract_level.log"
    resources:
        cpus=8,
        mem_mb=64000,
        runtime=120,
        partition="shared"
    shell:
        """
        /n/home11/xiaokangfu/.conda/envs/geo/bin/python {input.script} > {log} 2>&1
        """

# ========== Correlation Analysis ==========

rule correlation_analysis:
    """
    Compute correlations between sentiment and PLACES health indicators
    """
    input:
        script="src/04_validation/correlation/0.6-cor-with-places-500-data-sentiment.py",
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
        runtime=60,
        partition="shared"
    shell:
        """
        /n/home11/xiaokangfu/.conda/envs/geo/bin/python {input.script} > {log} 2>&1
        """

rule correlation_plots:
    """
    Generate enhanced correlation plots with LOWESS smoothing
    """
    input:
        script="src/04_validation/correlation/0.6.2-cor-p-value-and-plot.py",
        data=config['workspace'] + "/data/sentiment_places_data_joined.parquet",
        config="setting.json"
    output:
        "outputs/correlation/facet_scatter_lowess_all_years.png"
    log:
        "outputs/logs/correlation_plots.log"
    resources:
        cpus=4,
        mem_mb=32000,
        runtime=60,
        partition="shared"
    shell:
        """
        /n/home11/xiaokangfu/.conda/envs/geo/bin/python {input.script} > {log} 2>&1
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
