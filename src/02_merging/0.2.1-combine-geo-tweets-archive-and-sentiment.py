import os
import json
import pandas as pd
from pandarallel import pandarallel
pandarallel.initialize()

# Load configuration
with open('setting.json') as f:
    config = json.load(f)

workspace = config["workspace"]
geo_tweets_archive_base_path = config["geo_tweets_archive_base_path"]
sentiment_file_base_path = config["sentiment_file_base_path"]
sentiment_computing_path = config.get("sentiment_computing_path", None)  # Fallback directory for recomputed sentiment
output_data_path = os.path.join(workspace, "data/geotweets_with_sentiment")
print(output_data_path)
os.makedirs(output_data_path, exist_ok=True)

print("=" * 80)
print("Configuration:")
print(f"  Tweets archive: {geo_tweets_archive_base_path}")
print(f"  Sentiment base: {sentiment_file_base_path}")
print(f"  Sentiment computing (fallback): {sentiment_computing_path}")
print(f"  Output: {output_data_path}")
print("=" * 80)


# Initialize files_df, which stores information about tweets path, sentiment file path, and output file path

files_df = pd.DataFrame()
for year in range(2010, 2024):
    print(year)
    tweets_path_year = os.path.join(geo_tweets_archive_base_path, str(year))
    tweets_sentiment_output_path = os.path.join(output_data_path, str(year))
    print(tweets_sentiment_output_path)
    os.makedirs(tweets_sentiment_output_path, exist_ok=True)

    tweets_path = [os.path.join(tweets_path_year, file) for file in os.listdir(tweets_path_year) if file.endswith(".csv.gz")]
    file_names = [file.split("/")[-1] for file in tweets_path]

    sentiment_file_path = [os.path.join(sentiment_file_base_path, str(year), "bert_sentiment_" + file_name) for file_name in file_names]

    output_file_names = [os.path.join(tweets_sentiment_output_path, file.split(".csv.gz")[0] + ".parquet") for file in file_names]
    files_df = pd.concat([files_df, pd.DataFrame({"tweets_path": tweets_path, "sentiment_file_path": sentiment_file_path, "output_file": output_file_names,"file_name": file_names, "year": year})])

# files_df.to_csv(os.path.join(output_data_path, "files.csv"), index=False)

# merge tweets and sentiment data

def merge_tweets_and_sentiment(row):

    try:
        # Skip if output file already exists
        if os.path.exists(row["output_file"]):
            # print(f"  Skipping {row['file_name']} (already processed)")
            return "skipped"

        tweets = pd.read_csv(row["tweets_path"], sep = "\t", lineterminator="\n", dtype="unicode", index_col=None,  compression = "gzip")

        # Try to read sentiment file from primary location
        sentiment_path = row["sentiment_file_path"]
        sentiment = None
        used_fallback = False

        if os.path.exists(sentiment_path):
            # Primary location exists
            sentiment = pd.read_csv(sentiment_path, compression='gzip', sep = "\t", dtype={'message_id': str, 'score': float})
        elif sentiment_computing_path:
            # Try fallback location (recomputed sentiment)
            fallback_path = os.path.join(sentiment_computing_path, "output", str(row["year"]), "bert_sentiment_" + row["file_name"])
            if os.path.exists(fallback_path):
                print(f"  Using recomputed sentiment for {row['file_name']} (year {row['year']})")
                sentiment = pd.read_csv(fallback_path, compression='gzip', sep = "\t", dtype={'message_id': str, 'score': float})
                used_fallback = True
            else:
                raise FileNotFoundError(f"Sentiment file not found in primary or fallback location: {sentiment_path}")
        else:
            raise FileNotFoundError(f"Sentiment file not found: {sentiment_path}")

        # Merge tweets and sentiment
        merged_df = pd.merge(tweets, sentiment, on=['message_id'])
        merged_df.to_parquet(row["output_file"], index=False)
        return "success_fallback" if used_fallback else "success"
    except Exception as e:
        print(f"Error merging {row['file_name']}: {e}")
        return "failed"


import datetime
t1 = datetime.datetime.now()
print("\nStarting merge process...")
print(f"Total files to process: {len(files_df)}")
print("=" * 80)

# the following for a test
# test_df = files_df.head(5)
# test_df["merge_status"] = test_df.parallel_apply(merge_tweets_and_sentiment, axis=1)

# the following for the run
files_df["merge_status"] = files_df.parallel_apply(merge_tweets_and_sentiment, axis=1)
files_df.to_csv(os.path.join(output_data_path, "results_records.csv"), index=False)

# Print statistics
t2 = datetime.datetime.now()
print("\n" + "=" * 80)
print("MERGE COMPLETE")
print("=" * 80)
print(f"Time taken: {t2 - t1}")
print(f"\nStatistics:")
print(f"  Total files: {len(files_df)}")
print(f"  Skipped (already processed): {(files_df['merge_status'] == 'skipped').sum()}")
print(f"  Successfully merged (primary): {(files_df['merge_status'] == 'success').sum()}")
print(f"  Successfully merged (fallback/recomputed): {(files_df['merge_status'] == 'success_fallback').sum()}")
print(f"  Failed: {(files_df['merge_status'] == 'failed').sum()}")

# Show breakdown by year
print(f"\nBreakdown by year:")
for year in sorted(files_df['year'].unique()):
    year_df = files_df[files_df['year'] == year]
    fallback_count = (year_df['merge_status'] == 'success_fallback').sum()
    if fallback_count > 0:
        print(f"  {year}: {fallback_count} files used recomputed sentiment")

print("\nResults saved to:", os.path.join(output_data_path, "results_records.csv"))
print("=" * 80)
print("All done!")
