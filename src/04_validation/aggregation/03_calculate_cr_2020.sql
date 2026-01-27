-- =============================================================================
-- Calculate Coverage Ratio (CR) and log2CR for Year 2020
-- =============================================================================

-- Load configuration
CREATE OR REPLACE TABLE config AS SELECT * FROM read_json('setting.json');

-- 1. Load Population (All Blocks)
CREATE OR REPLACE TABLE pop AS 
SELECT GEOID20, CAST(population AS DOUBLE) AS P_i
FROM read_parquet((SELECT census_pop || '/*.parquet' FROM config));

-- 2. Load 2020 Tweets (Aggregated Stats)
-- Note: 'all_tweet_count' is the total count column in aggregated stats
CREATE OR REPLACE TABLE tweets_2020 AS 
SELECT GEOID20, CAST(all_tweet_count AS DOUBLE) AS T_i
FROM read_parquet((SELECT aggregated_sentiment_output || '/yearly_2020.parquet' FROM config));

-- 3. Join and Calculate CR
COPY (
WITH joined AS (
  SELECT 
    p.GEOID20,
    COALESCE(t.T_i, 0) AS T_i,
    p.P_i
  FROM pop p
  LEFT JOIN tweets_2020 t ON p.GEOID20 = t.GEOID20
),
tot AS (
  SELECT SUM(T_i) AS T_tot, SUM(P_i) AS P_tot FROM joined
)
SELECT
  j.GEOID20,
  j.T_i,
  j.P_i,
  (j.T_i / t.T_tot) / (j.P_i / t.P_tot)          AS CR,
  CASE 
    WHEN j.T_i = 0 OR j.P_i = 0 THEN NULL 
    ELSE ln((j.T_i / t.T_tot) / (j.P_i / t.P_tot)) / ln(2) 
  END AS log2CR,
  CASE WHEN j.T_i < 20 OR j.P_i <= 0 THEN 1 ELSE 0 END AS mask_low_coverage
FROM joined j, tot t
ORDER BY CR DESC
) TO (SELECT workspace || '/data/tweet_count_2020_with_pop_CR.parquet' FROM config) (FORMAT PARQUET);
