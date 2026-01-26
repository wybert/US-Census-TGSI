import os
import json
import pandas as pd
from tqdm import tqdm
from pandarallel import pandarallel
pandarallel.initialize()
tqdm.pandas()

# Load configuration
with open('setting.json') as f:
    config = json.load(f)

workspace = config["workspace"]
geo_tweets_archive_base_path = config["geo_tweets_archive_base_path"]
sentiment_file_base_path = config["sentiment_file_base_path"]
output_data_path = os.path.join(workspace, "data/geotweets_with_sentiment")
print(output_data_path)
os.makedirs(output_data_path, exist_ok=True)


# Initialize files_df, which stores information about tweets path, sentiment file path, and output file path

# files_df = pd.DataFrame()
# for year in range(2010, 2024):
#     print(year)
#     tweets_path_year = os.path.join(geo_tweets_archive_base_path, str(year))
#     tweets_sentiment_output_path = os.path.join(output_data_path, str(year))
#     print(tweets_sentiment_output_path)
#     os.makedirs(tweets_sentiment_output_path, exist_ok=True)

#     tweets_path = [os.path.join(tweets_path_year, file) for file in os.listdir(tweets_path_year) if file.endswith(".csv.gz")]
#     file_names = [file.split("/")[-1] for file in tweets_path]

#     sentiment_file_path = [os.path.join(sentiment_file_base_path, str(year), "bert_sentiment_" + file_name) for file_name in file_names]

#     output_file_names = [os.path.join(tweets_sentiment_output_path, file.split(".csv.gz")[0] + ".parquet") for file in file_names]
#     files_df = pd.concat([files_df, pd.DataFrame({"tweets_path": tweets_path, "sentiment_file_path": sentiment_file_path, "output_file": output_file_names,"file_name": file_names, "year": year})])

# files_df.to_csv(os.path.join(output_data_path, "files.csv"), index=False)

files_df = pd.read_csv(os.path.join(output_data_path, "results_records.csv"))
remain_df = files_df[files_df['if_merged_success'] == False]
# merge tweets and sentiment data

def merge_tweets_and_sentiment(row):

    try:
        tweets = pd.read_csv(row["tweets_path"], sep = "\t", lineterminator="\n", dtype="unicode", index_col=None,  compression = "gzip")
        # print(tweets.dtypes)
        sentiment = pd.read_csv(row["sentiment_file_path"], compression='gzip', sep = "\t", dtype={'message_id': str, 'score': float})
        # print(sentiment.dtypes)
        merged_df = pd.merge(tweets, sentiment, on=['message_id'])
        merged_df.to_parquet(row["output_file"], index=False)
        return True
    except Exception as e:
        print(f"Error merging {row['file_name']}: {e}")
        return False


import datetime
t1 = datetime.datetime.now()
print("start..")
# the following for a test
# test_df = files_df.head(5)
# test_df["if_merged_success"] = test_df.parallel_apply(merge_tweets_and_sentiment, axis=1)

# the following for the run
remain_df["if_merged_success"] = remain_df.progress_apply(merge_tweets_and_sentiment, axis=1)
remain_df.to_csv(os.path.join(output_data_path, "results_records_fault2.csv"), index=False)
t2 = datetime.datetime.now()
print(f"Time taken: {t2 - t1}")
print("all done")
