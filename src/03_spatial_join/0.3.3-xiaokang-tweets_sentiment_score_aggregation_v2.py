import polars as pl
import os

def load_yearly_data(year, census_base_path):
    # Define paths for the year-specific files
    census_path = os.path.join(census_base_path, str(year), "*.parquet")

    df = pl.read_parquet(census_path,columns = ["message_id",
                                                "text","user_id",
                                                "GEOID20",
                                                "date",
                                                "score"
                                                ]).lazy()


    # Add temporal columns
    df = df.with_columns(
        pl.col("date").dt.year().alias("year"),
        pl.col("date").dt.month().alias("month"),
        pl.col("date").dt.strftime('%Y-%m-%d').alias("day"),
        pl.col("date").dt.strftime('%Y-%m').alias("year_month")
    )
    return df

def filter_by_keywords(df, keywords=None):
    # Filter DataFrame based on keywords, if provided
    if keywords:
        df = df.filter(pl.col("text").str.contains(keywords))
    return df

def compute_statistics(df, group_by_vars=["day", "census_id"], aggregate_var="score"):
    # Define standard aggregation metrics
    basic_aggregations = [
        pl.count("message_id").alias("tweet_count"),
        pl.count("user_id").alias("user_count"),
        pl.mean(aggregate_var).alias(f"avg_{aggregate_var}"),
        pl.max(aggregate_var).alias(f"max_{aggregate_var}"),
        pl.min(aggregate_var).alias(f"min_{aggregate_var}"),
        pl.std(aggregate_var).alias(f"std_{aggregate_var}"),
        pl.col(aggregate_var).quantile(0.10).alias(f"{aggregate_var}_10q"),
        pl.col(aggregate_var).quantile(0.25).alias(f"{aggregate_var}_25q"),
        pl.col(aggregate_var).quantile(0.50).alias(f"{aggregate_var}_50q"),
        pl.col(aggregate_var).quantile(0.75).alias(f"{aggregate_var}_75q"),
        pl.col(aggregate_var).quantile(0.90).alias(f"{aggregate_var}_90q")
    ]
    stats_results = df.group_by(group_by_vars).agg(basic_aggregations).collect()
    return stats_results

def main( census_base_path, out_path, start_year=2022, end_year=2023):
    # Create output directory if it doesn't exist
    os.makedirs(out_path, exist_ok=True)
    # Define keyword groups and group by variables
    # keywords_group = [None, "covid|疫情|新冠|Covid|COVID"]
    keywords_group = [None]
    aggregate_var = "score"
    group_by_vars_list = [["day", "GEOID20"], ["year", "month", "GEOID20"],["year","GEOID20"]]
    # Process each year
    for year in range(start_year, end_year + 1):
        print(f"Processing year: {year}")

        # Load data for the specific year
        df = load_yearly_data(year, census_base_path)
        # Process and compute statistics for each keyword group
        for keywords in keywords_group:
            print(f"Processing keywords: {keywords}")
            df_filtered = filter_by_keywords(df, keywords=keywords)
            name_suffix = "_topic" if keywords else "_no_topic"
            # Compute and save statistics for each grouping level
            for group_by_vars in group_by_vars_list:
                prefix = "_".join(group_by_vars)
                suffix = f"{year}_{prefix}_{name_suffix}"
                print(f"Processing group by: {suffix}")
                stats_result = compute_statistics(df_filtered, group_by_vars, aggregate_var)
                stats_result.write_parquet(out_path + f"statistics-{suffix}.parquet")
                # write to csv
                stats_result.write_csv(out_path + f"statistics-{suffix}.csv")
    print("Processing complete.")

# Run the main function with specified paths and year range
if __name__ == "__main__":
    census_base_path = "/n/netscratch/cga/Lab/xiaokang/merged_sentiments_tweets_convert_types/"
    out_path = "/n/netscratch/cga/Lab/xiaokang/tweets_us_census_sentiment_stastic/"
    main(census_base_path, out_path)
