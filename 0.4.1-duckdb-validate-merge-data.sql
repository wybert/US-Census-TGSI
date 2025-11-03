-- =============================================================================
-- Aggregate tweet_count by GEOID20 using JSON configuration
-- =============================================================================

-- Load configuration from JSON
CREATE OR REPLACE TABLE config AS SELECT * FROM read_json('setting.json');

-- Read all parquet files matching '*day*.parquet' in the statistic_results folder
CREATE TABLE IF NOT EXISTS geo_tweet_sum AS
SELECT GEOID20, SUM(tweet_count) AS tweet_count
FROM config, read_parquet(config.statistic_results || '/*day*.parquet')
GROUP BY GEOID20;

-- Export aggregated results to a single parquet file
COPY (SELECT * FROM geo_tweet_sum)
TO (SELECT workspace || '/data/all_years_tweet_count.parquet' FROM config)
 (FORMAT PARQUET);

-- ------------------------------------------------------------------
-- Merge population data with tweet counts
-- Census population parquet files are under the workspace 'pop data' folder
-- Aggregate population by GEO_ID (rename to GEOID20) then LEFT JOIN tweets
-- ------------------------------------------------------------------

-- Drop intermediate tables if they exist to allow re-run
DROP TABLE IF EXISTS census_pop_agg;
DROP TABLE IF EXISTS geo_tweet_with_pop;

-- Read all census parquet files and aggregate population per GEO_ID
-- The census files have GEO_ID values like '1000000US010010201...'.
-- Strip everything up to and including 'US' so the id matches tweet GEOID20 values.
CREATE TABLE census_pop_agg AS
SELECT
	regexp_replace("GEO_ID", '^.*US', '') AS GEOID20,
	CAST("P1_001N" AS BIGINT) AS population
FROM config, read_parquet(config.census_pop || '/*.parquet');

-- Create joined table: tweet counts with population
CREATE TABLE geo_tweet_with_pop AS
SELECT
	t.GEOID20,
	t.tweet_count,
	p.population
FROM geo_tweet_sum t
LEFT JOIN census_pop_agg p
	ON t.GEOID20 = p.GEOID20;

-- Export joined results to parquet
COPY (SELECT * FROM geo_tweet_with_pop)
TO (SELECT workspace || '/data/all_years_tweet_count_with_pop.parquet' FROM config)
 (FORMAT PARQUET);

