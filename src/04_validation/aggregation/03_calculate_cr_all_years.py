import duckdb
import json
import os

# Load configuration
with open('setting.json') as f:
    config = json.load(f)

workspace = config['workspace']
input_path = os.path.join(workspace, "data/all_years_tweet_count_with_pop.parquet")
output_cr_path = os.path.join(workspace, "data/all_years_tweet_count_with_pop_CR.parquet")
output_filtered_path = os.path.join(workspace, "data/all_years_tweet_count_with_pop_CR_filtered.parquet")

print(f"Connecting to DuckDB...")
con = duckdb.connect()

print(f"Calculating Coverage Ratio (CR) for All Years...")
query = f"""
WITH base AS (
  SELECT
    GEOID20,
    CAST(tweet_count AS DOUBLE) AS T_i,
    CAST(population  AS DOUBLE) AS P_i
  FROM read_parquet('{input_path}')
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
"""

con.execute(f"COPY ({query}) TO '{output_cr_path}' (FORMAT PARQUET)")
print(f"Saved CR results to {output_cr_path}")

print(f"Filtering low coverage tracts...")
con.execute(f"COPY (SELECT * FROM read_parquet('{output_cr_path}') WHERE mask_low_coverage = 0) TO '{output_filtered_path}' (FORMAT PARQUET)")
print(f"Saved filtered results to {output_filtered_path}")
