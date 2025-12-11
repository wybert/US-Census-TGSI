#!/usr/bin/env python
"""
Script to recompute missing sentiment files using BERT model

This script reads the missing sentiment files list and prepares them for
sentiment analysis using the geotweet-sentiment-geography pipeline.

Usage:
    python 0.1.6-recompute-missing-sentiment.py --year 2014 --batch_size 100

Output:
    Sentiment scores saved to sentiment_computing_path organized by year
"""

import os
import json
import pandas as pd
import argparse
import shutil
from pathlib import Path
import sys

# Add the geotweet-sentiment-geography repo to path
sys.path.insert(0, '/n/home11/xiaokangfu/xiaokang/geotweet-sentiment-geography/src')

def prepare_data_for_year(config, year, missing_df, args):
    """
    Prepare missing tweet files for a specific year for sentiment computation

    Args:
        config: Configuration dictionary
        year: Year to process
        missing_df: DataFrame with missing files information
        args: Command line arguments
    """

    print(f"\n{'='*80}")
    print(f"Processing Year {year}")
    print(f"{'='*80}")

    # Filter missing files for this year
    year_missing = missing_df[missing_df['year'] == year]

    if len(year_missing) == 0:
        print(f"No missing files for year {year}")
        return

    print(f"Found {len(year_missing)} missing sentiment files for {year}")

    # Create temporary input directory for this year
    year_input_dir = os.path.join(config['sentiment_computing_path'], 'input', str(year))
    year_output_dir = os.path.join(config['sentiment_computing_path'], 'output', str(year))

    os.makedirs(year_input_dir, exist_ok=True)
    os.makedirs(year_output_dir, exist_ok=True)

    print(f"Input directory: {year_input_dir}")
    print(f"Output directory: {year_output_dir}")

    # Copy missing tweet files to input directory
    print("\nCopying tweet files to input directory...")
    copied_count = 0

    for idx, row in year_missing.iterrows():
        source_file = row['tweet_file_path']
        dest_file = os.path.join(year_input_dir, row['tweet_file'])

        if os.path.exists(source_file):
            try:
                # Create symlink instead of copying to save space
                if args.use_symlink:
                    if os.path.exists(dest_file):
                        os.remove(dest_file)
                    os.symlink(source_file, dest_file)
                else:
                    shutil.copy2(source_file, dest_file)
                copied_count += 1

                if copied_count % 100 == 0:
                    print(f"  Copied {copied_count}/{len(year_missing)} files...")
            except Exception as e:
                print(f"  Error copying {source_file}: {e}")
        else:
            print(f"  Warning: Source file not found: {source_file}")

    print(f"\n✓ Successfully prepared {copied_count} files for sentiment computation")

    # Create a file list for processing
    file_list_path = os.path.join(config['sentiment_computing_path'], f'file_list_{year}.txt')
    with open(file_list_path, 'w') as f:
        for _, row in year_missing.iterrows():
            f.write(f"{row['tweet_file']}\n")

    print(f"✓ Saved file list to: {file_list_path}")

    return year_input_dir, year_output_dir


def run_sentiment_analysis(year_input_dir, year_output_dir, args):
    """
    Run sentiment analysis using the geotweet-sentiment-geography pipeline

    Args:
        year_input_dir: Input directory with tweet files
        year_output_dir: Output directory for sentiment files
        args: Command line arguments
    """

    print(f"\n{'='*80}")
    print("Running Sentiment Analysis")
    print(f"{'='*80}")

    # Import the sentiment imputer
    try:
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
            print("⚠️  WARNING: Running on CPU (will be slower)")
            emb_model = torch.load(embedding_path, map_location=torch.device('cpu'))
            emb_model._target_device = torch.device(type='cpu')
            clf_model = torch.load(classifier_path, map_location=torch.device('cpu'))
            clf_model._target_device = torch.device(type='cpu')

        print("✓ Models loaded successfully")

        # Process each file
        files = [f for f in os.listdir(year_input_dir) if f.endswith('.csv.gz')]
        print(f"\nProcessing {len(files)} files...")

        # Create args object for the imputer
        class SentimentArgs:
            def __init__(self, batch_size):
                self.batch_size = batch_size
                self.emb_model = emb_model
                self.clf_model = clf_model
                self.score_digits = 6
                self.data_path = year_input_dir
                self.max_rows = 2500000  # Process in chunks
                self.output_path = year_output_dir

        sentiment_args = SentimentArgs(args.batch_size)

        success_count = 0
        error_count = 0
        skipped_count = 0

        for i, file in enumerate(files, 1):
            try:
                # Check if output file already exists
                output_file = os.path.join(year_output_dir, f'bert_sentiment_{file}')
                if os.path.exists(output_file):
                    print(f"\n[{i}/{len(files)}] Skipping (already exists): {file}")
                    skipped_count += 1
                    success_count += 1  # Count as success
                    continue

                print(f"\n[{i}/{len(files)}] Processing: {file}")

                # Run sentiment imputation
                df = embedding_imputation(file, sentiment_args)

                # Save output with 'bert_sentiment_' prefix
                df.to_csv(output_file, sep='\t', index=False, compression='gzip')

                print(f"  ✓ Saved {len(df)} sentiment scores")
                success_count += 1

                del df

            except Exception as e:
                print(f"  ✗ Error processing {file}: {e}")
                error_count += 1
                import traceback
                traceback.print_exc()

        print(f"\n{'='*80}")
        print("Sentiment Analysis Complete")
        print(f"{'='*80}")
        print(f"Successfully processed: {success_count} files")
        print(f"Skipped (already existed): {skipped_count} files")
        print(f"Errors: {error_count} files")

    except Exception as e:
        print(f"\n✗ Error during sentiment analysis: {e}")
        import traceback
        traceback.print_exc()
        return False

    return True


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Recompute missing sentiment files')
    parser.add_argument('--year', type=int, default=None,
                        help='Specific year to process (if not specified, process all missing years)')
    parser.add_argument('--batch_size', type=int, default=100,
                        help='Batch size for BERT processing (default: 100)')
    parser.add_argument('--use_symlink', action='store_true',
                        help='Use symbolic links instead of copying files (saves disk space)')
    parser.add_argument('--dry_run', action='store_true',
                        help='Show what would be done without actually processing')

    args = parser.parse_args()

    # Load configuration
    with open('setting.json') as f:
        config = json.load(f)

    # Read missing files list
    missing_files_path = os.path.join(config['outputs_dir'], 'missing_sentiment_files.csv')

    if not os.path.exists(missing_files_path):
        print(f"✗ Missing files list not found: {missing_files_path}")
        print("Please run 0.1.5-find-missing-sentiment-files.py first")
        sys.exit(1)

    print("=" * 80)
    print("Recompute Missing Sentiment Files")
    print("=" * 80)
    print(f"Configuration file: setting.json")
    print(f"Missing files list: {missing_files_path}")
    print(f"Output directory: {config['sentiment_computing_path']}")
    print(f"Batch size: {args.batch_size}")
    print(f"Use symlinks: {args.use_symlink}")
    print(f"Dry run: {args.dry_run}")

    # Read missing files
    missing_df = pd.read_csv(missing_files_path)
    print(f"\nTotal missing files: {len(missing_df)}")

    # Get years to process
    if args.year:
        years_to_process = [args.year]
        year_missing_df = missing_df[missing_df['year'] == args.year]
        if len(year_missing_df) == 0:
            print(f"\n✗ No missing files found for year {args.year}")
            sys.exit(0)
    else:
        years_to_process = sorted(missing_df['year'].unique())

    print(f"Years to process: {years_to_process}")

    # Show summary by year
    print("\nMissing files by year:")
    for year in years_to_process:
        count = len(missing_df[missing_df['year'] == year])
        print(f"  {year}: {count} files")

    if args.dry_run:
        print("\n[DRY RUN] Would process the above files. Use without --dry_run to actually run.")
        sys.exit(0)

    # Create base directories
    os.makedirs(config['sentiment_computing_path'], exist_ok=True)
    os.makedirs(os.path.join(config['sentiment_computing_path'], 'input'), exist_ok=True)
    os.makedirs(os.path.join(config['sentiment_computing_path'], 'output'), exist_ok=True)

    # Process each year
    for year in years_to_process:
        try:
            year_input_dir, year_output_dir = prepare_data_for_year(
                config, year, missing_df, args
            )

            if not args.dry_run:
                success = run_sentiment_analysis(year_input_dir, year_output_dir, args)

                if not success:
                    print(f"\n⚠️  Warning: Sentiment analysis failed for year {year}")

        except Exception as e:
            print(f"\n✗ Error processing year {year}: {e}")
            import traceback
            traceback.print_exc()

    print("\n" + "=" * 80)
    print("All processing complete!")
    print("=" * 80)
    print(f"\nResults saved to: {config['sentiment_computing_path']}/output/")
    print("\nNext steps:")
    print("1. Verify the output files in the sentiment_computing_path")
    print("2. If everything looks good, copy them to the main sentiment directory")
    print("3. Re-run 0.1.5-find-missing-sentiment-files.py to verify all files are present")
