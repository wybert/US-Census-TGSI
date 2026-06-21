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
        p.P_i
    FROM pop p
    LEFT JOIN tweets t ON p.GEOID20 = t.GEOID20
),
tot AS (
    SELECT SUM(T_i) AS T_tot, SUM(P_i) AS P_tot FROM joined
)
SELECT
    j.GEOID20,
    j.T_i,
    j.P_i,
    (j.T_i / t.T_tot) / (j.P_i / t.P_tot) AS CR,
    CASE 
        WHEN j.T_i = 0 OR j.P_i = 0 THEN NULL 
        ELSE ln((j.T_i / t.T_tot) / (j.P_i / t.P_tot)) / ln(2) 
    END AS log2CR,
    CASE WHEN j.T_i < 20 OR j.P_i <= 0 THEN 1 ELSE 0 END AS mask_low_coverage
FROM joined j, tot t
ORDER BY CR DESC
"""

con.execute(f"COPY ({query}) TO '{output_path}' (FORMAT PARQUET)")
print(f"Successfully saved results to {output_path}")
