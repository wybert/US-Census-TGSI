-- Example: Spatial join tweets with census blocks using merged GeoParquet
-- This demonstrates the complete workflow

.echo on
.timer on

INSTALL spatial;
LOAD spatial;

-- Step 1: Load census blocks from merged GeoParquet (fast!)
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
  FROM read_parquet('/n/netscratch/cga/Lab/xiaokang/US-Census-TGSI-workspace/data/census_data_2020/us_census_blocks_2020.geoparquet');

-- Create spatial index for fast lookups
CREATE INDEX census_geom_idx ON census_blocks USING RTREE(geometry_4326);

SELECT 'Census blocks loaded:', COUNT(*) FROM census_blocks;

-- Step 2: Load sample tweets (one file from 2020 for quick testing)
SELECT 'Loading tweets...';
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
    ST_Point(CAST(longitude AS DOUBLE), CAST(latitude AS DOUBLE)) as tweet_geom
  FROM read_parquet('/n/netscratch/cga/Lab/xiaokang/US-Census-TGSI-workspace/data/geotweets_with_sentiment/2020/2020_10_*.parquet');

SELECT 'Tweets loaded:', COUNT(*) FROM tweets;
SELECT 'GPS tweets:', COUNT(*) FROM tweets WHERE GPS = true;
SELECT 'Non-GPS tweets:', COUNT(*) FROM tweets WHERE GPS = false;

-- Step 3: Spatial join - find which census block each tweet falls in
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
    -- Compute confidence score
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

SELECT 'Tweets matched to blocks:', COUNT(*) FROM tweets_with_blocks;
SELECT 'Match rate:',
  ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM tweets), 2) || '%'
FROM tweets_with_blocks;

-- Show sample results
SELECT '========================================';
SELECT 'Sample Results (first 5 matched tweets)';
SELECT '========================================';
SELECT
  message_id,
  latitude,
  longitude,
  sentiment,
  GPS,
  spatialerror,
  GEOID20,
  STATEFP20,
  ROUND(confidence, 3) as confidence
FROM tweets_with_blocks
LIMIT 5;

-- Step 4: Save results
SELECT 'Saving results...';
COPY tweets_with_blocks
TO '/n/netscratch/cga/Lab/xiaokang/US-Census-TGSI-workspace/data/tweets_with_census_blocks/test_output.parquet'
(FORMAT PARQUET, COMPRESSION SNAPPY);

SELECT 'Results saved to: test_output.parquet';

SELECT '========================================';
SELECT 'Test Complete - Census blocks ready for spatial join';
SELECT '========================================';
