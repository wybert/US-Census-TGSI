-- ========= Interactive Version for DuckDB Shell Inspection =========
-- This version creates tables you can query and inspect before exporting
-- Run each section separately or all at once

-- Load configuration from JSON
CREATE OR REPLACE TABLE config AS SELECT * FROM read_json('setting.json');

-- ========= 1) 读"按天·block"并提取年份 =========
CREATE OR REPLACE TABLE daily AS (
  SELECT
    -- 若 day 已是 DATE/TIMESTAMP 或 'YYYY-MM-DD' 字符串，以下写法都能正确转为 DATE 再取年份
    EXTRACT('year' FROM CAST(day AS DATE))::INT AS year,      -- DuckDB 支持 EXTRACT / date_part
    GEOID20::VARCHAR                              AS GEOID20_block,   -- 15 位 block
    CAST(tweet_count AS DOUBLE)                   AS t,
    CAST(avg_score  AS DOUBLE)                    AS s
  FROM config, read_parquet(config.statistic_results || '/*day*.parquet')
);

-- Check daily data
SELECT 'Daily data loaded' AS step, COUNT(*) AS row_count FROM daily;
SELECT 'Years in daily data' AS info, MIN(year) AS min_year, MAX(year) AS max_year, COUNT(DISTINCT year) AS unique_years FROM daily;

-- ========= 2) 天→年：按年·block 的推文加权均值 =========
CREATE OR REPLACE TABLE block_year AS (
  SELECT
    year,
    GEOID20_block,
    SUM(t)                                   AS tweets_year_block,
    SUM(t * s) / NULLIF(SUM(t),0)            AS sent_mean_year_block,  -- 用推文数加权，等价于"合并所有推文再取均值"
    CASE WHEN SUM(t) < 20 THEN 1 ELSE 0 END  AS mask_lowcov_block
  FROM daily
  GROUP BY 1,2
);

-- Check block_year data
SELECT 'Block-year data aggregated' AS step, COUNT(*) AS row_count FROM block_year;
SELECT 'Low coverage blocks' AS info, SUM(mask_lowcov_block) AS low_cov_count, COUNT(*) - SUM(mask_lowcov_block) AS high_cov_count FROM block_year;

-- ========= 3) block→tract：取前 11 位聚合为"按年·tract"的加权均值 =========
CREATE OR REPLACE TABLE tract_year AS (
  SELECT
    year,
    SUBSTR(GEOID20_block, 1, 11)                   AS GEOID20_tract,   -- 11 位 tract
    SUM(tweets_year_block)                         AS tweets_year_tract,
    SUM(tweets_year_block * sent_mean_year_block)
      / NULLIF(SUM(tweets_year_block),0)           AS sent_mean_year_tract,
    CASE WHEN SUM(tweets_year_block) < 20 THEN 1 ELSE 0 END AS mask_low_coverage
  FROM block_year
  GROUP BY 1,2
);

-- Check tract_year data
SELECT 'Tract-year data aggregated' AS step, COUNT(*) AS row_count FROM tract_year;
SELECT 'High coverage tracts' AS info, COUNT(*) AS high_cov_tracts FROM tract_year WHERE mask_low_coverage = 0;

-- ========= 4) 读取所有年度的 PLACES（tract 宽表）-
CREATE OR REPLACE TABLE places_all AS (
SELECT
  CAST(regexp_extract(filename, '([0-9]{4})_release', 1) AS INT) AS release_year,
  TractFIPS::VARCHAR                                     AS GEOID20_tract,
  CAST(TotalPopulation AS DOUBLE)                         AS pop,
  CAST(MHLTH_CrudePrev AS DOUBLE)                         AS mhlth,     -- Frequent Mental Distress (%)
  CAST(MAMMOUSE_CrudePrev AS DOUBLE)                      AS mammouse   -- 判别示例
FROM config, read_csv_auto(config.places_data || '/*.csv', filename=true)
WHERE regexp_extract(filename, '([0-9]{4})_release', 1) != ''
);

-- Check PLACES data
SELECT 'PLACES data loaded' AS step, COUNT(*) AS row_count FROM places_all;
SELECT 'Years in PLACES data' AS info, MIN(release_year) AS min_year, MAX(release_year) AS max_year, COUNT(DISTINCT release_year) AS unique_years FROM places_all;

-- ========= 5) 年份重叠检查 =========
SELECT 'YEAR OVERLAP ANALYSIS' AS analysis;
SELECT 'Tweet years:' AS dataset, year, COUNT(*) AS tract_count
FROM tract_year WHERE mask_low_coverage = 0
GROUP BY year ORDER BY year;

SELECT 'PLACES years:' AS dataset, release_year AS year, COUNT(*) AS tract_count
FROM places_all
GROUP BY release_year ORDER BY release_year;

-- Check for overlapping years
SELECT 'Overlapping years' AS check, COUNT(*) AS overlap_count
FROM (
  SELECT DISTINCT year FROM tract_year WHERE mask_low_coverage = 0
  INTERSECT
  SELECT DISTINCT release_year FROM places_all
);

-- ========= 6) 原始联结（可能返回0行） =========
CREATE OR REPLACE TABLE joined_original AS (
  SELECT
    y.year,
    y.GEOID20_tract,
    y.tweets_year_tract,
    y.sent_mean_year_tract,
    y.mask_low_coverage,
    p.pop, p.mhlth, p.mammouse,
    p.release_year
  FROM tract_year y
  JOIN places_all p
    ON p.release_year = y.year
   AND p.GEOID20_tract = y.GEOID20_tract
  WHERE y.mask_low_coverage = 0
    AND p.pop IS NOT NULL AND p.mhlth IS NOT NULL
    -- only need 2020,2021,2022
    AND y.year IN (2020, 2021, 2022)
);

SELECT 'Original join result' AS step, COUNT(*) AS row_count FROM joined_original;

-- save the joined_original result to local drive as a parquet
COPY joined_original TO (SELECT workspace || '/data/sentiment_places_data_joined.parquet' FROM config) WITH (FORMAT 'PARQUET');
