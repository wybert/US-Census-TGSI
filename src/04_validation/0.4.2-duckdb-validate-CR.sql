-- =============================================================================
-- Calculate Coverage Ratio (CR) and log2CR using JSON configuration
-- =============================================================================

-- Load configuration from JSON
CREATE OR REPLACE TABLE config AS SELECT * FROM read_json('setting.json');

-- 输入：all_years_tweet_count_with_pop.parquet 只有 GEOID20, tweet_count, population
-- 输出：per-tract CR 与 log2CR
COPY (
WITH base AS (
  SELECT
    GEOID20,
    CAST(tweet_count AS DOUBLE) AS T_i,
    CAST(population  AS DOUBLE) AS P_i
  FROM config, read_parquet(config.workspace || '/data/all_years_tweet_count_with_pop.parquet')
),
tot AS (
  SELECT SUM(T_i) AS T_tot, SUM(P_i) AS P_tot FROM base
)
SELECT
  b.GEOID20,
  b.T_i,
  b.P_i,
  (b.T_i / t.T_tot) / (b.P_i / t.P_tot)          AS CR,
  ln((b.T_i / t.T_tot) / (b.P_i / t.P_tot)) / ln(2) AS log2CR,
  CASE WHEN b.T_i < 20 OR b.P_i <= 0 THEN 1 ELSE 0 END AS mask_low_coverage
FROM base b, tot t
ORDER BY CR DESC
) TO (SELECT workspace || '/data/all_years_tweet_count_with_pop_CR.parquet' FROM config) (FORMAT PARQUET);

-- 过滤掉低覆盖率的记录
COPY (
SELECT * FROM config, read_parquet(config.workspace || '/data/all_years_tweet_count_with_pop_CR.parquet')
WHERE mask_low_coverage = 0
) TO (SELECT workspace || '/data/all_years_tweet_count_with_pop_CR_filtered.parquet' FROM config) (FORMAT PARQUET);
