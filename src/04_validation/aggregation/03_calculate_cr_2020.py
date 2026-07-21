import duckdb
import json
import os

# Load configuration
with open('setting.json') as f:
    config = json.load(f)

workspace = config['workspace']
census_pop_path = os.path.join(config['census_pop'], "*.parquet")
tweets_2020_path = os.path.join(config['aggregated_sentiment_output'], "yearly_2020.parquet")
output_path = os.path.join(workspace, "data/tweet_count_2020_with_pop_CR.parquet")

print(f"Connecting to DuckDB...")
con = duckdb.connect()

print(f"Loading data and calculating CR for 2020...")
# Coordinate-artifact blocks: place-tagged (non-GPS) tweets are geocoded to a
# fixed named-place centroid; when that centroid falls inside a near-empty
# census block, the block's raw tweet count is a coordinate-collapse
# artifact, not local activity. Flag blocks with implausibly small
# population (<50) and implausibly large tweet volume (>5,000 within the
# single year), matching the scaling used in 03_calculate_cr_all_years.py
# (50,000 over the ~14x-longer 2010-2023 window).
query = f"""
WITH pop AS (
    SELECT GEOID20, CAST(population AS DOUBLE) AS P_i
    FROM read_parquet('{census_pop_path}')
),
tweets AS (
    SELECT GEOID20, CAST(all_tweet_count AS DOUBLE) AS T_i
    FROM read_parquet('{tweets_2020_path}')
),
joined AS (
    SELECT
        p.GEOID20,
        COALESCE(t.T_i, 0) AS T_i,
        p.P_i,
        CASE WHEN p.P_i < 50 AND COALESCE(t.T_i, 0) > 5000 THEN 1 ELSE 0 END AS is_artifact
    FROM pop p
    LEFT JOIN tweets t ON p.GEOID20 = t.GEOID20
),
tot AS (
    SELECT SUM(T_i) AS T_tot, SUM(P_i) AS P_tot FROM joined WHERE is_artifact = 0
)
SELECT
    j.GEOID20,
    j.T_i,
    j.P_i,
    CASE WHEN j.P_i > 0 THEN (j.T_i / t.T_tot) / (j.P_i / t.P_tot) ELSE NULL END AS CR,
    CASE
        WHEN j.T_i = 0 OR j.P_i = 0 THEN NULL
        ELSE ln((j.T_i / t.T_tot) / (j.P_i / t.P_tot)) / ln(2)
    END AS log2CR,
    CASE WHEN j.T_i < 20 OR j.P_i <= 0 OR j.is_artifact = 1 THEN 1 ELSE 0 END AS mask_low_coverage
FROM joined j, tot t
ORDER BY CR DESC
"""

con.execute(f"COPY ({query}) TO '{output_path}' (FORMAT PARQUET)")
print(f"Successfully saved results to {output_path}")
