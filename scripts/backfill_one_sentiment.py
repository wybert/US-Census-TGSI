#!/usr/bin/env python
"""
Backfill a single geotweets_with_sentiment parquet whose backup was empty
(2014_4_25_11). Recomputes BERT sentiment for one raw tweet file and merges
it with the tweets, mirroring the tested logic in:
  - src/01_data_acquisition/0.1.6-recompute-missing-sentiment.py (imputer)
  - src/02_merging/0.2.1-combine-geo-tweets-archive-and-sentiment.py (merge)

Usage: python scripts/backfill_one_sentiment.py <raw_csv_gz> <output_parquet>
"""
import os
import sys
import pandas as pd

sys.path.insert(0, '/n/home11/xiaokangfu/xiaokang/geotweet-sentiment-geography/src')
import torch
from utils.emb_sentiment_imputer import embedding_imputation

EMB = "/n/home11/xiaokangfu/xiaokang/geotweet-sentiment-geography/training_model/emb.pkl"
CLF = "/n/home11/xiaokangfu/xiaokang/geotweet-sentiment-geography/training_model/clf.pkl"

raw_path, out_path = sys.argv[1], sys.argv[2]
data_path, fname = os.path.dirname(raw_path), os.path.basename(raw_path)

print(f"Backfill: {fname} -> {out_path}")
assert torch.cuda.is_available(), "GPU required"
print("GPU:", torch.cuda.get_device_name(0))

emb_model = torch.load(EMB)
clf_model = torch.load(CLF)

class Args:
    batch_size = 100
    score_digits = 6
    data_path = os.path.dirname(raw_path)
    max_rows = 2500000
sa = Args(); sa.emb_model = emb_model; sa.clf_model = clf_model

# 1) sentiment (message_id, score) -- same imputer as the bulk recompute
sentiment = embedding_imputation(fname, sa)
sentiment['message_id'] = sentiment['message_id'].astype(str)
print(f"sentiment rows: {len(sentiment)}")

# 2) merge with full tweets -- identical to 0.2.1 merge_tweets_and_sentiment
tweets = pd.read_csv(raw_path, sep="\t", lineterminator="\n", dtype="unicode",
                     index_col=None, compression="gzip")
merged = pd.merge(tweets, sentiment.astype({'message_id': str}), on=['message_id'])
os.makedirs(os.path.dirname(out_path), exist_ok=True)
merged.to_parquet(out_path, index=False)
print(f"WROTE {out_path}: {len(merged)} rows, {len(merged.columns)} cols")
