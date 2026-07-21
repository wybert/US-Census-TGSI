import duckdb
import json
import os

# Load configuration
with open('setting.json') as f:
    config = json.load(f)

workspace = config['workspace']
# Source: freshly rebuilt per-year block aggregates from 01_generate_aggregated_stats.py
# (all_tweet_count = count across all confidence levels). Was previously the stale
# pre-purge statistic_results/*day*.parquet on holylabs.
agg_stats = os.path.join(config['aggregated_sentiment_output'], "yearly_*.parquet")
census_pop_path = os.path.join(config['census_pop'], "*.parquet")
output_counts_path = os.path.join(workspace, "data/all_years_tweet_count.parquet")
output_with_pop_path = os.path.join(workspace, "data/all_years_tweet_count_with_pop.parquet")

print(f"Connecting to DuckDB...")
con = duckdb.connect()

print(f"Aggregating tweet counts from {agg_stats}...")
con.execute(f"""
CREATE TABLE geo_tweet_sum AS
SELECT GEOID20, SUM(all_tweet_count) AS tweet_count
FROM read_parquet('{agg_stats}')
GROUP BY GEOID20;
""")

print(f"Saving counts to {output_counts_path}...")
con.execute(f"COPY (SELECT * FROM geo_tweet_sum) TO '{output_counts_path}' (FORMAT PARQUET)")

print(f"Processing Census population from {census_pop_path}...")
con.execute(f"""
CREATE TABLE census_pop_agg AS
SELECT
	GEOID20,
	CAST(population AS BIGINT) AS population
FROM read_parquet('{census_pop_path}');
""")

print(f"Merging counts with population...")
# Population-first join: start from the full 8.18M-block universe and
# COALESCE missing tweet counts to 0, so blocks with zero tweets across
# all years are retained (not silently dropped from the population total).
con.execute(f"""
CREATE TABLE geo_tweet_with_pop AS
SELECT
	p.GEOID20,
	COALESCE(t.tweet_count, 0) AS tweet_count,
	p.population
FROM census_pop_agg p
LEFT JOIN geo_tweet_sum t
	ON t.GEOID20 = p.GEOID20;
""")

print(f"Saving merged results to {output_with_pop_path}...")
con.execute(f"COPY (SELECT * FROM geo_tweet_with_pop) TO '{output_with_pop_path}' (FORMAT PARQUET)")
print("Done.")
