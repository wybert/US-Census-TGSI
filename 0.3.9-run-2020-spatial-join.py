#!/usr/bin/env python
"""
Batch process 2020 tweets spatial join by month.
Based on logic from 0.3.8-test-spatial-join-example.sql.
"""

import os
import glob
import json
import argparse
import subprocess
import time

# Parse arguments
parser = argparse.ArgumentParser(description='Batch spatial join using DuckDB')
parser.add_argument('--start-year', type=int, default=2010, help='Start year')
parser.add_argument('--end-year', type=int, default=2023, help='End year')
parser.add_argument('--dry-run', action='store_true', help='Verify setup only')
args = parser.parse_args()

# Load configuration
with open('setting.json') as f:
    config = json.load(f)

# Configuration
# YEAR = 2020 # Removed hardcoded year
MONTHS = range(1, 13) # 1 to 12
BASE_INPUT_DIR = config['geotweets_with_sentiment']
BASE_OUTPUT_DIR = config['tweets_with_census_blocks_confidence']
CENSUS_FILE = config['census_data_2020'] + "/us_census_blocks_2020.geoparquet"
TEMP_DIR = config['workspace'] + "/tmp_duckdb"

# Ensure temp dir exists
os.makedirs(TEMP_DIR, exist_ok=True)
# Get thread count from SLURM or default to 8
threads = os.environ.get('SLURM_CPUS_PER_TASK', '8')

# SQL Template
SQL_TEMPLATE = """
.echo on
.timer on

-- Configuration
SET threads TO {threads};
SET memory_limit TO '800GB';
-- Use a large temporary directory for spilling to disk
SET temp_directory TO '{temp_dir}';

INSTALL spatial;
LOAD spatial;

-- Step 1: Load census blocks (Doing this every time ensures clean state, and it's fast enough)
SELECT 'Loading census blocks...';
CREATE OR REPLACE TABLE census_blocks AS
  SELECT
    GEOID20,
    STATEFP20,
    COUNTYFP20,
    TRACTCE20,
    BLOCKCE20,
    block_area_m2,
    block_diameter_m,
    geometry as geometry_4326
  FROM read_parquet('{census_file}');

CREATE INDEX census_geom_idx ON census_blocks USING RTREE(geometry_4326);

-- Step 2: Load tweets for specific month
SELECT 'Loading tweets for {year}-{month}...';
CREATE OR REPLACE TABLE tweets AS
  SELECT
    message_id,
    CAST(latitude AS DOUBLE) as latitude,
    CAST(longitude AS DOUBLE) as longitude,
    score as sentiment,
    date,
    CASE
        WHEN GPS IS NULL THEN false
        WHEN GPS = 'True' THEN true
        WHEN GPS = 'true' THEN true
        ELSE false
    END as GPS,
    COALESCE(CAST(spatialerror AS DOUBLE), 10000.0) as spatialerror,
    -- Implicitly treats coordinates as WGS84 (EPSG:4326)
    ST_Point(CAST(longitude AS DOUBLE), CAST(latitude AS DOUBLE)) as tweet_geom
  FROM read_parquet('{input_pattern}');

SELECT 'Tweets loaded:', COUNT(*) FROM tweets;

-- Step 3: Spatial join
SELECT 'Performing spatial join...';
CREATE OR REPLACE TABLE tweets_with_blocks AS
  SELECT
    t.message_id,
    t.latitude,
    t.longitude,
    t.sentiment,
    t.date,
    t.GPS,
    t.spatialerror,
    c.GEOID20,
    c.STATEFP20,
    c.COUNTYFP20,
    c.TRACTCE20,
    c.BLOCKCE20,
    c.block_area_m2,
    c.block_diameter_m,
    -- Compute confidence score (Logic from 0.3.7/0.3.8)
    CASE
        WHEN t.GPS THEN 1.0
        WHEN t.spatialerror < 50 THEN 1.0
        WHEN t.spatialerror < (c.block_diameter_m / 2) THEN 0.8
        WHEN t.spatialerror < c.block_diameter_m THEN 0.5
        WHEN t.spatialerror < (2 * c.block_diameter_m) THEN 0.3
        WHEN t.spatialerror < 1000 THEN 0.15
        ELSE 0.05
    END as confidence
  FROM tweets t
  JOIN census_blocks c
    ON ST_Within(t.tweet_geom, c.geometry_4326);

SELECT 'Tweets matched:', COUNT(*) FROM tweets_with_blocks;

-- Step 4: Save results
SELECT 'Saving results to {output_file}...';
COPY tweets_with_blocks
TO '{output_file}'
(FORMAT PARQUET, COMPRESSION SNAPPY);

SELECT 'Done.';
"""

def process_month(year, month):
    import glob

    month_str = f"{month:02d}"
    print(f"\n{'='*60}")
    print(f"Processing {year}-{month_str}")
    print(f"{'='*60}")

    # Define paths
    # Input files use single digit for months 1-9 (e.g., 2020_1_...)
    input_pattern = os.path.join(BASE_INPUT_DIR, str(year), f"{year}_{month}_*.parquet")
    
    # Check if any files match the pattern using glob
    matched_files = glob.glob(input_pattern)
    if not matched_files:
        print(f"Skipping {year}-{month_str}: No files found matching pattern '{input_pattern}'")
        return
    
    # Output file uses zero-padded month (e.g., 2020_01.parquet)
    output_dir = os.path.join(BASE_OUTPUT_DIR, str(year))
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f"{year}_{month_str}.parquet")

    # Check if output already exists
    if os.path.exists(output_file):
        print(f"Warning: Output file exists: {output_file}")
        # print("Skipping...") 
        # return # Uncomment to skip existing

    # Prepare SQL
    sql_script = SQL_TEMPLATE.format(
        census_file=CENSUS_FILE,
        input_pattern=input_pattern,
        output_file=output_file,
        year=year,
        month=month_str,
        temp_dir=TEMP_DIR,
        threads=threads
    )

    # Write temp SQL file
    temp_sql_path = f"temp_process_{year}_{month_str}.sql"
    with open(temp_sql_path, "w") as f:
        f.write(sql_script)

    # Execute DuckDB
    start_time = time.time()
    try:
        # Using subprocess to call duckdb CLI
        # Assuming 'duckdb' is in path. If not, provide full path.
        cmd = f"duckdb < {temp_sql_path}"
        subprocess.run(cmd, shell=True, check=True)
        
        duration = time.time() - start_time
        print(f"✓ Completed {year}-{month_str} in {duration:.2f} seconds")
        
    except subprocess.CalledProcessError as e:
        print(f"✗ Failed {year}-{month_str}: {e}")
    finally:
        # Cleanup
        if os.path.exists(temp_sql_path):
            os.remove(temp_sql_path)

def main():
    print(f"Starting spatial join batch processing: {args.start_year}-{args.end_year}")
    print(f"Temp Directory: {TEMP_DIR}")
    
    start_total = time.time()
    
    years = range(args.start_year, args.end_year + 1)
    months = range(1, 13)

    for year in years:
        for month in months:
            process_month(year, month)
            
    print(f"\nAll tasks finished in {time.time() - start_total:.2f} seconds")

if __name__ == "__main__":
    main()

