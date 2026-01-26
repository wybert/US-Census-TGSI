#!/usr/bin/env python
"""
Script to recompute missing sentiment files by calling the original sentiment script

This version directly calls the original main_sentiment_imputer.py script
from the geotweet-sentiment-geography repository.

Usage:
    python 0.1.6-recompute-missing-sentiment-direct.py --year 2020
"""

import os
import json
import pandas as pd
import argparse
import shutil
import subprocess
from pathlib import Path

def prepare_data_for_year(config, year, missing_df, args):
    """
    Prepare missing tweet files for a specific year for sentiment computation
    """

    print(f"\n{'='*80}")
    print(f"Processing Year {year}")
    print(f"{'='*80}")

    # Filter missing files for this year
    year_missing = missing_df[missing_df['year'] == year]

    if len(year_missing) == 0:
        print(f"No missing files for year {year}")
        return None, None

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

    return year_input_dir, year_output_dir


def run_sentiment_analysis_direct(year_input_dir, year_output_dir, repo_path, args):
    """
    Run sentiment analysis by directly calling the original script
    """

    print(f"\n{'='*80}")
    print("Running Sentiment Analysis (Direct Call)")
    print(f"{'='*80}")

    # Call the original script
    script_path = os.path.join(repo_path, 'src', 'main_sentiment_imputer.py')

    print(f"Script: {script_path}")
    print(f"Input: {year_input_dir}")
    print(f"Output: {year_output_dir}")

    cmd = [
        'python',
        script_path,
        '--data_path', year_input_dir,
        '--output_path', year_output_dir,
        '--emb_methods', 'bert',
        '--batch_size', str(args.batch_size)
    ]

    print(f"\nCommand: {' '.join(cmd)}")
    print()

    try:
        result = subprocess.run(
            cmd,
            cwd=repo_path,
            capture_output=False,
            text=True
        )

        if result.returncode == 0:
            print("\n✓ Sentiment computation completed successfully")
            return True
        else:
            print(f"\n✗ Sentiment computation failed with exit code {result.returncode}")
            return False
    except Exception as e:
        print(f"\n✗ Error running sentiment analysis: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Recompute missing sentiment files (direct call version)')
    parser.add_argument('--year', type=int, default=None,
                        help='Specific year to process')
    parser.add_argument('--batch_size', type=int, default=100,
                        help='Batch size for BERT processing')
    parser.add_argument('--use_symlink', action='store_true',
                        help='Use symbolic links instead of copying files')
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
        exit(1)

    print("=" * 80)
    print("Recompute Missing Sentiment Files (Direct Call Version)")
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
            exit(0)
    else:
        years_to_process = sorted(missing_df['year'].unique())

    print(f"Years to process: {years_to_process}")

    # Show summary by year
    print("\nMissing files by year:")
    for year in years_to_process:
        count = len(missing_df[missing_df['year'] == year])
        print(f"  {year}: {count} files")

    if args.dry_run:
        print("\n[DRY RUN] Would process the above files.")
        exit(0)

    # Create base directories
    os.makedirs(config['sentiment_computing_path'], exist_ok=True)
    os.makedirs(os.path.join(config['sentiment_computing_path'], 'input'), exist_ok=True)
    os.makedirs(os.path.join(config['sentiment_computing_path'], 'output'), exist_ok=True)

    repo_path = '/n/home11/xiaokangfu/xiaokang/geotweet-sentiment-geography'

    # Process each year
    for year in years_to_process:
        try:
            year_input_dir, year_output_dir = prepare_data_for_year(
                config, year, missing_df, args
            )

            if year_input_dir is None:
                continue

            success = run_sentiment_analysis_direct(
                year_input_dir, year_output_dir, repo_path, args
            )

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
    print("1. Verify the output files")
    print("2. If everything looks good, copy them to the main sentiment directory")
    print("3. Re-run 0.1.5-find-missing-sentiment-files.py to verify all files are present")
