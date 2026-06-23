# US Census-Tract Twitter Sentiment Geographical Index (US-CT-TSGI)

Aggregated sentiment statistics derived from ~2.8 billion geotagged tweets in the
United States (2010–2023), resolved to the US **Census Block** level (2020 vintage,
GEOID20) at **daily, monthly, and yearly** temporal granularity. Sentiment is scored
with the BERT-based model of Chai et al. (2023); tweets are assigned to census blocks
by point-in-polygon spatial join against TIGER/Line 2020 boundaries.

This deposit contains the **aggregated statistics only** (counts and sentiment
summaries per block-period). It contains **no tweet text, user identifiers, or
coordinates**, and is therefore consistent with the Twitter/X Developer Terms.

## Files

42 Apache Parquet files: `{granularity}_{year}.parquet` for
`granularity ∈ {daily, monthly, yearly}` and `year ∈ 2010..2023`.

| Granularity | Temporal key | Files | Approx. size |
|-------------|--------------|-------|--------------|
| yearly  | `GEOID20, year`        | 14 | 5.3 GB |
| monthly | `GEOID20, year, month` | 14 | 23 GB |
| daily   | `GEOID20, date_val`    | 14 | 94 GB |

Each file provides, for four spatial-confidence tiers (`all`, `medium`, `high`,
`strict`), the tweet count, unique-user count, and sentiment distribution
(mean, sd, min, max, and 10/25/50/75/90th percentiles), plus a confidence-weighted
mean sentiment. See `data_dictionary.csv` for the full variable list (53 fields).

## Confidence tiers

Each tweet receives a spatial-join `confidence` (1.0 = GPS-exact; lower for
place/POI-centroid matches). Tiers are cumulative: `strict` (=1.0) ⊆ `high` (≥0.8)
⊆ `medium` (≥0.5) ⊆ `all`. **Note:** following Twitter's June 2019 removal of
precise geotagging (Hu & Wang 2020), GPS-exact tweets fall below ~11% of volume;
for series spanning that transition, prefer the confidence-weighted `all` tier.

## Usage

```python
import duckdb
duckdb.sql("""
  SELECT substr(GEOID20,1,11) AS tract,
         SUM(all_tweet_count) AS tweets,
         SUM(all_tweet_count*all_sentiment_mean)/SUM(all_tweet_count) AS sentiment
  FROM 'yearly_2020.parquet' GROUP BY 1
""")
```

Parquet is column-oriented and queryable out-of-core with DuckDB, pandas, or polars.
Aggregate to tract/county/state via the GEOID20 prefix. Recompute coverage ratios as
`tweet_count / population` (2020 Decennial block population). Mask low-coverage units
(< 20 tweets) for analyses sensitive to sparse sampling.

## Known limitations

Tweet volume is far more spatially concentrated than population (Gini ≈ 0.79) and the
posting population is not demographically representative; annual volume varies with the
source archive's collection coverage; 2010–2011 are too sparse for reliable estimates.
Treat the index as an aggregate, relative signal. See the Data Descriptor for details.

## Citation

Fu, X., Jain, D., Hayes, J. *Twitter Sentiment Geographical Index Dataset for the US at
Census Level.* (Data Descriptor, under review.) Code: https://github.com/wybert/US-Census-TGSI

## License

Creative Commons Attribution 4.0 International (CC BY 4.0). See `LICENSE.txt`.
