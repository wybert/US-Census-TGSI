#!/usr/bin/env python
"""
Validation script to verify sentiment computation correctness

This script:
1. Takes an existing tweet file that already has sentiment scores
2. Recomputes sentiment using our method
3. Compares the results to verify correctness

Usage:
    python 0.1.10-validate-sentiment-computation.py --year 2015 --sample-size 10
"""

import os
import json
import pandas as pd
import argparse
import sys
import numpy as np
from pathlib import Path

# Add the geotweet-sentiment-geography repo to path
sys.path.insert(0, '/n/home11/xiaokangfu/xiaokang/geotweet-sentiment-geography/src')

def find_existing_sentiment_file(config, year, limit=10):
    """
    Find existing sentiment files for a given year

    Returns: list of (tweet_file, sentiment_file) tuples
    """

    tweet_dir = os.path.join(config['geo_tweets_archive_base_path'], str(year))
    sentiment_dir = os.path.join(config['sentiment_file_base_path'], str(year))

    if not os.path.exists(tweet_dir) or not os.path.exists(sentiment_dir):
        return []

    # Get list of existing sentiment files
    sentiment_files = [f for f in os.listdir(sentiment_dir) if f.startswith('bert_sentiment_')]

    pairs = []
    for sentiment_file in sentiment_files[:limit]:
        # Get corresponding tweet file name
        tweet_file = sentiment_file.replace('bert_sentiment_', '')
        tweet_path = os.path.join(tweet_dir, tweet_file)
        sentiment_path = os.path.join(sentiment_dir, sentiment_file)

        if os.path.exists(tweet_path) and os.path.exists(sentiment_path):
            pairs.append({
                'tweet_file': tweet_file,
                'tweet_path': tweet_path,
                'sentiment_file': sentiment_file,
                'sentiment_path': sentiment_path
            })

    return pairs


def recompute_sentiment(tweet_path, output_dir, args):
    """
    Recompute sentiment for a single file
    """

    from utils.emb_sentiment_imputer import embedding_imputation
    import torch

    # Load models
    embedding_path = "/n/home11/xiaokangfu/xiaokang/geotweet-sentiment-geography/training_model/emb.pkl"
    classifier_path = "/n/home11/xiaokangfu/xiaokang/geotweet-sentiment-geography/training_model/clf.pkl"

    print("\nLoading BERT models...")
    if torch.cuda.is_available():
        print(f"✓ GPU available: {torch.cuda.get_device_name(0)}")
        emb_model = torch.load(embedding_path)
        clf_model = torch.load(classifier_path)
    else:
        print("⚠️  Running on CPU")
        emb_model = torch.load(embedding_path, map_location=torch.device('cpu'))
        emb_model._target_device = torch.device(type='cpu')
        clf_model = torch.load(classifier_path, map_location=torch.device('cpu'))
        clf_model._target_device = torch.device(type='cpu')

    print("✓ Models loaded successfully")

    # Create args object for the imputer
    class SentimentArgs:
        def __init__(self, batch_size, data_path):
            self.batch_size = batch_size
            self.emb_model = emb_model
            self.clf_model = clf_model
            self.score_digits = 6
            self.data_path = data_path
            self.max_rows = 2500000

    file_name = os.path.basename(tweet_path)
    data_path = os.path.dirname(tweet_path)

    sentiment_args = SentimentArgs(args.batch_size, data_path)

    # Run sentiment imputation
    print(f"\nRecomputing sentiment for: {file_name}")
    df = embedding_imputation(file_name, sentiment_args)

    return df


def compare_results(original_df, recomputed_df, tolerance=0.001):
    """
    Compare original and recomputed sentiment scores
    """

    print(f"\n{'='*80}")
    print("Comparison Results")
    print(f"{'='*80}")

    # Merge on message_id
    merged = pd.merge(
        original_df[['message_id', 'score']],
        recomputed_df[['message_id', 'score']],
        on='message_id',
        suffixes=('_original', '_recomputed')
    )

    print(f"\nTotal messages:")
    print(f"  Original: {len(original_df)}")
    print(f"  Recomputed: {len(recomputed_df)}")
    print(f"  Matched: {len(merged)}")

    if len(merged) == 0:
        print("\n✗ No matching messages found!")
        return False

    # Calculate differences
    merged['diff'] = abs(merged['score_original'] - merged['score_recomputed'])
    merged['diff_pct'] = (merged['diff'] / merged['score_original']) * 100

    print(f"\nScore Statistics:")
    print(f"  Original mean: {merged['score_original'].mean():.6f}")
    print(f"  Recomputed mean: {merged['score_recomputed'].mean():.6f}")
    print(f"  Original std: {merged['score_original'].std():.6f}")
    print(f"  Recomputed std: {merged['score_recomputed'].std():.6f}")

    print(f"\nDifference Statistics:")
    print(f"  Mean absolute difference: {merged['diff'].mean():.6f}")
    print(f"  Max absolute difference: {merged['diff'].max():.6f}")
    print(f"  Median absolute difference: {merged['diff'].median():.6f}")

    # Count matches within tolerance
    exact_matches = (merged['diff'] == 0).sum()
    close_matches = (merged['diff'] <= tolerance).sum()

    print(f"\nMatch Analysis:")
    print(f"  Exact matches: {exact_matches} ({exact_matches/len(merged)*100:.2f}%)")
    print(f"  Within {tolerance} tolerance: {close_matches} ({close_matches/len(merged)*100:.2f}%)")

    # Show some examples
    print(f"\nSample Comparisons (first 10):")
    print(merged[['message_id', 'score_original', 'score_recomputed', 'diff']].head(10).to_string())

    # Correlation
    correlation = merged['score_original'].corr(merged['score_recomputed'])
    print(f"\nCorrelation: {correlation:.6f}")

    # Verdict
    print(f"\n{'='*80}")
    if correlation > 0.99 and merged['diff'].mean() < tolerance:
        print("✅ VALIDATION PASSED: Results are highly consistent!")
        return True
    elif correlation > 0.95:
        print("⚠️  VALIDATION WARNING: Results are similar but have some differences")
        return True
    else:
        print("❌ VALIDATION FAILED: Results are significantly different!")
        return False


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Validate sentiment computation')
    parser.add_argument('--year', type=int, default=2015,
                        help='Year to test with (use a year with 100% coverage, e.g., 2015)')
    parser.add_argument('--sample-size', type=int, default=5,
                        help='Number of files to test')
    parser.add_argument('--batch-size', type=int, default=100,
                        help='Batch size for BERT processing')
    parser.add_argument('--save-output', action='store_true',
                        help='Save recomputed results for inspection')

    args = parser.parse_args()

    # Load configuration
    with open('setting.json') as f:
        config = json.load(f)

    print("=" * 80)
    print("Sentiment Computation Validation")
    print("=" * 80)
    print(f"Year: {args.year}")
    print(f"Sample size: {args.sample_size}")
    print(f"Batch size: {args.batch_size}")

    # Find existing sentiment files
    print(f"\nSearching for existing sentiment files in year {args.year}...")
    file_pairs = find_existing_sentiment_file(config, args.year, limit=args.sample_size)

    if len(file_pairs) == 0:
        print(f"✗ No existing sentiment files found for year {args.year}")
        print("Try a different year (e.g., 2015, 2016, 2018, 2019)")
        sys.exit(1)

    print(f"✓ Found {len(file_pairs)} file pairs to test")

    # Create output directory for validation
    validation_dir = os.path.join(config['outputs_dir'], 'validation', str(args.year))
    os.makedirs(validation_dir, exist_ok=True)

    # Test each file
    all_passed = True
    results = []

    for i, pair in enumerate(file_pairs, 1):
        print(f"\n{'='*80}")
        print(f"Test {i}/{len(file_pairs)}: {pair['tweet_file']}")
        print(f"{'='*80}")

        try:
            # Load original sentiment
            print("\nLoading original sentiment...")
            original_df = pd.read_csv(pair['sentiment_path'], sep='\t', compression='gzip')
            print(f"✓ Loaded {len(original_df)} records")

            # Recompute sentiment
            recomputed_df = recompute_sentiment(pair['tweet_path'], validation_dir, args)
            print(f"✓ Recomputed {len(recomputed_df)} records")

            # Save recomputed results if requested
            if args.save_output:
                output_path = os.path.join(validation_dir, f"recomputed_{pair['sentiment_file']}")
                recomputed_df.to_csv(output_path, sep='\t', index=False, compression='gzip')
                print(f"✓ Saved recomputed results to: {output_path}")

            # Compare results
            passed = compare_results(original_df, recomputed_df)

            results.append({
                'file': pair['tweet_file'],
                'passed': passed,
                'original_count': len(original_df),
                'recomputed_count': len(recomputed_df)
            })

            if not passed:
                all_passed = False

        except Exception as e:
            print(f"\n✗ Error processing {pair['tweet_file']}: {e}")
            import traceback
            traceback.print_exc()
            all_passed = False
            results.append({
                'file': pair['tweet_file'],
                'passed': False,
                'error': str(e)
            })

    # Final summary
    print(f"\n{'='*80}")
    print("VALIDATION SUMMARY")
    print(f"{'='*80}")

    passed_count = sum(1 for r in results if r.get('passed', False))
    print(f"Tests passed: {passed_count}/{len(results)}")

    for r in results:
        status = "✓ PASS" if r.get('passed', False) else "✗ FAIL"
        print(f"  {status}: {r['file']}")

    # Save summary
    summary_path = os.path.join(config['outputs_dir'], 'validation', 'validation_summary.json')
    with open(summary_path, 'w') as f:
        json.dump({
            'year': args.year,
            'sample_size': len(file_pairs),
            'passed': passed_count,
            'failed': len(results) - passed_count,
            'all_passed': all_passed,
            'results': results
        }, f, indent=2)
    print(f"\n✓ Validation summary saved to: {summary_path}")

    if all_passed:
        print("\n✅ ALL VALIDATIONS PASSED! The computation method is correct.")
        sys.exit(0)
    else:
        print("\n⚠️  Some validations failed. Please review the results.")
        sys.exit(1)
