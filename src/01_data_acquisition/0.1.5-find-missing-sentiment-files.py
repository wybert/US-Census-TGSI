#!/usr/bin/env python
"""
Script to identify missing sentiment files before tweet-sentiment merge

This script scans all geotagged tweet files and checks if corresponding
BERT sentiment files exist. Missing sentiment files are saved to CSV for
further investigation or re-processing.

Usage:
    python 0.1.5-find-missing-sentiment-files.py

Output:
    - outputs/missing_sentiment_files.csv: List of missing sentiment files
    - outputs/missing_sentiment_summary.txt: Summary statistics
"""

import os
import json
import pandas as pd
from pathlib import Path
from datetime import datetime

# Load configuration
with open('setting.json') as f:
    config = json.load(f)

geo_tweets_archive_base_path = config["geo_tweets_archive_base_path"]
sentiment_file_base_path = config["sentiment_file_base_path"]
outputs_dir = config["outputs_dir"]

# Create outputs directory if it doesn't exist
os.makedirs(outputs_dir, exist_ok=True)

print("=" * 80)
print("Finding Missing Sentiment Files")
print("=" * 80)
print(f"Geo tweets path: {geo_tweets_archive_base_path}")
print(f"Sentiment files path: {sentiment_file_base_path}")
print(f"Output directory: {outputs_dir}")
print()

# Initialize lists to store results
missing_files = []
existing_files = []
tweet_file_stats = []

# Scan all years (2010-2024)
for year in range(2010, 2025):
    print(f"\nProcessing year {year}...")

    tweets_path_year = os.path.join(geo_tweets_archive_base_path, str(year))
    sentiment_path_year = os.path.join(sentiment_file_base_path, str(year))

    # Check if tweet directory exists
    if not os.path.exists(tweets_path_year):
        print(f"  ⚠️  Tweet directory does not exist: {tweets_path_year}")
        continue

    # Check if sentiment directory exists
    if not os.path.exists(sentiment_path_year):
        print(f"  ⚠️  Sentiment directory does not exist: {sentiment_path_year}")
        # Mark all tweet files as missing sentiment
        tweet_files = [f for f in os.listdir(tweets_path_year) if f.endswith(".csv.gz")]
        for tweet_file in tweet_files:
            missing_files.append({
                'year': year,
                'tweet_file': tweet_file,
                'tweet_file_path': os.path.join(tweets_path_year, tweet_file),
                'expected_sentiment_file': f"bert_sentiment_{tweet_file}",
                'expected_sentiment_path': os.path.join(sentiment_path_year, f"bert_sentiment_{tweet_file}"),
                'reason': 'sentiment_directory_missing'
            })
        continue

    # Get all tweet files in this year
    tweet_files = [f for f in os.listdir(tweets_path_year) if f.endswith(".csv.gz")]
    print(f"  Found {len(tweet_files)} tweet files")

    # Check each tweet file for corresponding sentiment file
    year_missing = 0
    year_existing = 0

    for tweet_file in tweet_files:
        expected_sentiment_file = f"bert_sentiment_{tweet_file}"
        sentiment_file_path = os.path.join(sentiment_path_year, expected_sentiment_file)
        tweet_file_path = os.path.join(tweets_path_year, tweet_file)

        if os.path.exists(sentiment_file_path):
            year_existing += 1
            existing_files.append({
                'year': year,
                'tweet_file': tweet_file,
                'sentiment_file': expected_sentiment_file,
                'tweet_file_path': tweet_file_path,
                'sentiment_file_path': sentiment_file_path
            })
        else:
            year_missing += 1
            missing_files.append({
                'year': year,
                'tweet_file': tweet_file,
                'tweet_file_path': tweet_file_path,
                'expected_sentiment_file': expected_sentiment_file,
                'expected_sentiment_path': sentiment_file_path,
                'reason': 'file_not_found'
            })

    print(f"  ✓ Existing: {year_existing}")
    print(f"  ✗ Missing: {year_missing}")

    # Store year statistics
    tweet_file_stats.append({
        'year': year,
        'total_tweet_files': len(tweet_files),
        'sentiment_files_exist': year_existing,
        'sentiment_files_missing': year_missing,
        'coverage_percentage': (year_existing / len(tweet_files) * 100) if len(tweet_files) > 0 else 0
    })

# Convert to DataFrames
missing_df = pd.DataFrame(missing_files)
existing_df = pd.DataFrame(existing_files)
stats_df = pd.DataFrame(tweet_file_stats)

# Save missing files to CSV
missing_output_path = os.path.join(outputs_dir, "missing_sentiment_files.csv")
missing_df.to_csv(missing_output_path, index=False)
print(f"\n✓ Saved missing files list to: {missing_output_path}")

# Save existing files for reference
existing_output_path = os.path.join(outputs_dir, "existing_sentiment_files.csv")
existing_df.to_csv(existing_output_path, index=False)
print(f"✓ Saved existing files list to: {existing_output_path}")

# Save statistics
stats_output_path = os.path.join(outputs_dir, "sentiment_files_statistics.csv")
stats_df.to_csv(stats_output_path, index=False)
print(f"✓ Saved statistics to: {stats_output_path}")

# Generate summary report
summary_path = os.path.join(outputs_dir, "missing_sentiment_summary.txt")
with open(summary_path, 'w') as f:
    f.write("=" * 80 + "\n")
    f.write("Missing Sentiment Files Summary Report\n")
    f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    f.write("=" * 80 + "\n\n")

    f.write("OVERALL STATISTICS\n")
    f.write("-" * 80 + "\n")
    total_tweets = len(missing_files) + len(existing_files)
    f.write(f"Total tweet files scanned: {total_tweets:,}\n")
    f.write(f"Sentiment files exist: {len(existing_files):,} ({len(existing_files)/total_tweets*100:.2f}%)\n")
    f.write(f"Sentiment files missing: {len(missing_files):,} ({len(missing_files)/total_tweets*100:.2f}%)\n")
    f.write("\n")

    f.write("YEAR-BY-YEAR BREAKDOWN\n")
    f.write("-" * 80 + "\n")
    f.write(f"{'Year':<8} {'Total':<10} {'Existing':<10} {'Missing':<10} {'Coverage':<12}\n")
    f.write("-" * 80 + "\n")
    for _, row in stats_df.iterrows():
        f.write(f"{row['year']:<8} {row['total_tweet_files']:<10} {row['sentiment_files_exist']:<10} "
                f"{row['sentiment_files_missing']:<10} {row['coverage_percentage']:<11.2f}%\n")

    f.write("\n")
    f.write("MISSING FILES BY REASON\n")
    f.write("-" * 80 + "\n")
    if len(missing_files) > 0:
        reason_counts = missing_df['reason'].value_counts()
        for reason, count in reason_counts.items():
            f.write(f"{reason}: {count:,} files\n")
    else:
        f.write("No missing files found!\n")

    f.write("\n")
    f.write("TOP 10 MISSING FILES (by year)\n")
    f.write("-" * 80 + "\n")
    if len(missing_files) > 0:
        for _, row in missing_df.head(10).iterrows():
            f.write(f"Year {row['year']}: {row['tweet_file']}\n")
            f.write(f"  Expected: {row['expected_sentiment_file']}\n")
            f.write(f"  Reason: {row['reason']}\n\n")
    else:
        f.write("No missing files to display.\n")

print(f"✓ Saved summary report to: {summary_path}")

# Print summary to console
print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)
total_tweets = len(missing_files) + len(existing_files)
print(f"Total tweet files scanned: {total_tweets:,}")
print(f"Sentiment files exist: {len(existing_files):,} ({len(existing_files)/total_tweets*100:.2f}%)")
print(f"Sentiment files missing: {len(missing_files):,} ({len(missing_files)/total_tweets*100:.2f}%)")
print()

if len(missing_files) > 0:
    print("⚠️  WARNING: Missing sentiment files detected!")
    print(f"   Please review: {missing_output_path}")
else:
    print("✓ All sentiment files are present!")

print("\nDone!")
