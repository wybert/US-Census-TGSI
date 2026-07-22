import duckdb
import json
import os
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--tier", choices=["all", "medium", "high", "strict"], default="all",
                     help="Confidence tier the tweet counts/sentiment are drawn from (all/medium/high/strict).")
args = parser.parse_args()
tier = args.tier
suffix = "" if tier == "all" else f"_{tier}"

# Load configuration
with open('setting.json') as f:
    config = json.load(f)

workspace = config['workspace']
# Source: freshly rebuilt daily block aggregates from 01_generate_aggregated_stats.py.
# Column mapping vs. legacy statistic_results: day->date_val, tweet_count->{tier}_tweet_count,
# avg_score->{tier}_sentiment_mean. Was previously the stale pre-purge holylabs files.
# Only the years this correlation uses (tweet years 2020-2022). Reading all years'
# daily files (~470M block-day rows) is unnecessary here and was exhausting memory.
agg_stats = os.path.join(config['aggregated_sentiment_output'], "daily_202[0-2].parquet")
# Only the GIS-friendly wide releases this analysis uses (tweet years 2020-2022).
# Excludes the 2024 file, which is long-format (different schema) and would break read_csv_auto.
places_data = os.path.join(config['places_data'], "*202[0-2]_release.csv")
census_pop_path = os.path.join(config['census_pop'], "*.parquet")
output_path = os.path.join(workspace, f"data/sentiment_places_data_joined{suffix}.parquet")

print(f"Connecting to DuckDB...")
con = duckdb.connect()
# Spill to large netscratch temp dir (not a small node /tmp) and cap memory.
_tmp = os.path.join(workspace, "tmp_duckdb")
os.makedirs(_tmp, exist_ok=True)
con.execute(f"SET temp_directory='{_tmp}'; SET memory_limit='55GB';")

print(f"Loading daily data from {agg_stats} (tier={tier})...")
con.execute(f"""
CREATE OR REPLACE TABLE daily AS (
  SELECT
    EXTRACT('year' FROM CAST(date_val AS DATE))::INT AS year,
    GEOID20::VARCHAR                                  AS GEOID20_block,
    CAST({tier}_tweet_count AS DOUBLE)                AS t,
    CAST({tier}_sentiment_mean AS DOUBLE)             AS s
  FROM read_parquet('{agg_stats}')
);
""")

print(f"Loading census population (for coordinate-artifact detection)...")
con.execute(f"""
CREATE OR REPLACE TABLE pop AS (
  SELECT GEOID20::VARCHAR AS GEOID20_block, CAST(population AS DOUBLE) AS P_i
  FROM read_parquet('{census_pop_path}')
);
""")

print(f"Aggregating block-year stats...")
# Coordinate-artifact blocks (place-tagged tweets collapsed onto a fixed
# named-place centroid that falls inside a near-empty block; see
# 03_calculate_cr_2020.py) are excluded from the tract sum below, not just
# flagged, so they cannot inflate a tract's sentiment/tweet total.
con.execute("""
CREATE OR REPLACE TABLE block_year AS (
  SELECT
    d.year,
    d.GEOID20_block,
    SUM(d.t)                                   AS tweets_year_block,
    SUM(d.t * d.s) / NULLIF(SUM(d.t),0)        AS sent_mean_year_block,
    CASE WHEN SUM(d.t) < 20 THEN 1 ELSE 0 END  AS mask_lowcov_block,
    CASE WHEN MAX(p.P_i) < 50 AND SUM(d.t) > 5000 THEN 1 ELSE 0 END AS is_artifact
  FROM daily d
  LEFT JOIN pop p ON p.GEOID20_block = d.GEOID20_block
  GROUP BY 1,2
);
""")

print(f"Aggregating tract-year stats...")
con.execute("""
CREATE OR REPLACE TABLE tract_year AS (
  SELECT
    year,
    SUBSTR(GEOID20_block, 1, 11)                   AS GEOID20_tract,
    SUM(tweets_year_block)                         AS tweets_year_tract,
    SUM(tweets_year_block * sent_mean_year_block)
      / NULLIF(SUM(tweets_year_block),0)           AS sent_mean_year_tract,
    CASE WHEN SUM(tweets_year_block) < 20 THEN 1 ELSE 0 END AS mask_low_coverage
  FROM block_year
  WHERE is_artifact = 0
  GROUP BY 1,2
);
""")

print(f"Loading PLACES health data from {places_data}...")
con.execute(f"""
CREATE OR REPLACE TABLE places_all AS (
SELECT
  CAST(regexp_extract(filename, '([0-9]{{4}})_release', 1) AS INT) AS release_year,
  TractFIPS::VARCHAR                                     AS GEOID20_tract,
  CAST(TotalPopulation AS DOUBLE)                         AS pop,
  CAST(MHLTH_CrudePrev AS DOUBLE)                         AS mhlth,
  CAST(MAMMOUSE_CrudePrev AS DOUBLE)                      AS mammouse
FROM read_csv_auto('{places_data}', filename=true)
WHERE regexp_extract(filename, '([0-9]{{4}})_release', 1) != ''
);
""")

print(f"Joining tweets with health indicators (2020-2022)...")
con.execute(f"""
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
    AND y.year IN (2020, 2021, 2022)
);
""")

print(f"Saving joined result to {output_path}...")
con.execute(f"COPY joined_original TO '{output_path}' (FORMAT PARQUET)")
print("Done.")
