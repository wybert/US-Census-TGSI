#!/usr/bin/env python
"""
Script to verify recomputed sentiment files and copy them to the main sentiment directory

This script:
1. Verifies that recomputed sentiment files are valid
2. Compares file counts and basic statistics
3. Optionally copies verified files to the main sentiment directory

Usage:
    # Dry run (check without copying)
    python 0.1.8-verify-and-copy-sentiment.py --dry_run

    # Verify and copy
    python 0.1.8-verify-and-copy-sentiment.py --copy

    # Verify specific year
    python 0.1.8-verify-and-copy-sentiment.py --year 2014 --dry_run
"""

import os
import json
import pandas as pd
import argparse
from pathlib import Path
import shutil
from datetime import datetime

def verify_sentiment_file(file_path):
    """
    Verify that a sentiment file is valid

    Args:
        file_path: Path to sentiment file

    Returns:
        dict with verification results
    """
    try:
        # Try to read the file
        df = pd.read_csv(file_path, sep='\t', compression='gzip', nrows=10)

        # Check for required columns
        if 'message_id' not in df.columns or 'score' not in df.columns:
            return {
                'valid': False,
                'error': 'Missing required columns (message_id, score)',
                'row_count': 0
            }

        # Read full file to get row count
        df_full = pd.read_csv(file_path, sep='\t', compression='gzip')
        row_count = len(df_full)

        # Check for null scores
        null_scores = df_full['score'].isna().sum()

        return {
            'valid': True,
            'row_count': row_count,
            'null_scores': null_scores,
            'score_mean': df_full['score'].mean(),
            'score_std': df_full['score'].std(),
            'error': None
        }

    except Exception as e:
        return {
            'valid': False,
            'error': str(e),
            'row_count': 0
        }


def verify_year(config, year, args):
    """
    Verify all recomputed sentiment files for a specific year

    Args:
        config: Configuration dictionary
        year: Year to verify
        args: Command line arguments

    Returns:
        verification results dictionary
    """

    print(f"\n{'='*80}")
    print(f"Verifying Year {year}")
    print(f"{'='*80}")

    year_output_dir = os.path.join(config['sentiment_computing_path'], 'output', str(year))

    if not os.path.exists(year_output_dir):
        print(f"✗ Output directory not found: {year_output_dir}")
        return None

    # Get all sentiment files
    sentiment_files = [f for f in os.listdir(year_output_dir) if f.startswith('bert_sentiment_')]

    if len(sentiment_files) == 0:
        print(f"⚠️  No sentiment files found in {year_output_dir}")
        return None

    print(f"Found {len(sentiment_files)} sentiment files")

    # Verify each file
    valid_files = []
    invalid_files = []
    verification_results = []

    for i, file in enumerate(sentiment_files, 1):
        file_path = os.path.join(year_output_dir, file)

        if i % 100 == 0:
            print(f"  Verifying file {i}/{len(sentiment_files)}...")

        result = verify_sentiment_file(file_path)
        result['filename'] = file
        result['year'] = year
        verification_results.append(result)

        if result['valid']:
            valid_files.append(file)
        else:
            invalid_files.append(file)
            print(f"  ✗ Invalid file: {file} - {result['error']}")

    print(f"\n✓ Valid files: {len(valid_files)}")
    print(f"✗ Invalid files: {len(invalid_files)}")

    if len(valid_files) > 0:
        # Show statistics
        valid_results = [r for r in verification_results if r['valid']]
        total_rows = sum(r['row_count'] for r in valid_results)
        total_nulls = sum(r['null_scores'] for r in valid_results)
        avg_score = sum(r['score_mean'] * r['row_count'] for r in valid_results) / total_rows

        print(f"\nStatistics:")
        print(f"  Total rows: {total_rows:,}")
        print(f"  Null scores: {total_nulls:,} ({total_nulls/total_rows*100:.2f}%)")
        print(f"  Average score: {avg_score:.4f}")

    return {
        'year': year,
        'total_files': len(sentiment_files),
        'valid_files': len(valid_files),
        'invalid_files': len(invalid_files),
        'verification_results': verification_results
    }


def copy_verified_files(config, year, verification_result, args):
    """
    Copy verified sentiment files to the main sentiment directory

    Args:
        config: Configuration dictionary
        year: Year to copy
        verification_result: Results from verification
        args: Command line arguments
    """

    if verification_result is None:
        print(f"\n⚠️  Skipping copy for year {year} - no verification results")
        return

    if verification_result['valid_files'] == 0:
        print(f"\n⚠️  Skipping copy for year {year} - no valid files")
        return

    print(f"\n{'='*80}")
    print(f"Copying Files for Year {year}")
    print(f"{'='*80}")

    source_dir = os.path.join(config['sentiment_computing_path'], 'output', str(year))
    dest_dir = os.path.join(config['sentiment_file_base_path'], str(year))

    # Create destination directory if it doesn't exist
    os.makedirs(dest_dir, exist_ok=True)

    print(f"Source: {source_dir}")
    print(f"Destination: {dest_dir}")

    if args.dry_run:
        print("\n[DRY RUN] Would copy the following files:")

    copied_count = 0
    skipped_count = 0
    error_count = 0

    # Copy only valid files
    valid_files = [r['filename'] for r in verification_result['verification_results'] if r['valid']]

    for i, file in enumerate(valid_files, 1):
        source_file = os.path.join(source_dir, file)
        dest_file = os.path.join(dest_dir, file)

        if i % 100 == 0:
            print(f"  Processing file {i}/{len(valid_files)}...")

        # Check if file already exists in destination
        if os.path.exists(dest_file) and not args.overwrite:
            if args.dry_run:
                print(f"  Would skip (exists): {file}")
            skipped_count += 1
            continue

        if args.dry_run:
            print(f"  Would copy: {file}")
            copied_count += 1
        else:
            try:
                shutil.copy2(source_file, dest_file)
                copied_count += 1
            except Exception as e:
                print(f"  ✗ Error copying {file}: {e}")
                error_count += 1

    print(f"\n{'='*80}")
    if args.dry_run:
        print("Dry Run Summary")
    else:
        print("Copy Summary")
    print(f"{'='*80}")
    print(f"Would copy/Copied: {copied_count} files")
    print(f"Skipped (already exist): {skipped_count} files")
    if not args.dry_run:
        print(f"Errors: {error_count} files")


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Verify and copy recomputed sentiment files')
    parser.add_argument('--year', type=int, default=None,
                        help='Specific year to verify (if not specified, verify all years)')
    parser.add_argument('--copy', action='store_true',
                        help='Copy verified files to main sentiment directory')
    parser.add_argument('--dry_run', action='store_true',
                        help='Show what would be done without actually copying')
    parser.add_argument('--overwrite', action='store_true',
                        help='Overwrite existing files in destination')

    args = parser.parse_args()

    # Load configuration
    with open('setting.json') as f:
        config = json.load(f)

    print("=" * 80)
    print("Verify and Copy Recomputed Sentiment Files")
    print("=" * 80)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Source: {config['sentiment_computing_path']}/output/")
    print(f"Destination: {config['sentiment_file_base_path']}")
    print(f"Copy mode: {args.copy}")
    print(f"Dry run: {args.dry_run}")
    print(f"Overwrite: {args.overwrite}")

    # Get years to process
    output_base = os.path.join(config['sentiment_computing_path'], 'output')

    if not os.path.exists(output_base):
        print(f"\n✗ Output directory not found: {output_base}")
        print("Have you run 0.1.6-recompute-missing-sentiment.py yet?")
        exit(1)

    if args.year:
        years_to_verify = [args.year]
    else:
        # Find all year directories
        years_to_verify = []
        for item in os.listdir(output_base):
            item_path = os.path.join(output_base, item)
            if os.path.isdir(item_path) and item.isdigit():
                years_to_verify.append(int(item))
        years_to_verify.sort()

    if len(years_to_verify) == 0:
        print("\n✗ No year directories found in output")
        exit(1)

    print(f"\nYears to verify: {years_to_verify}")

    # Verify each year
    all_results = []

    for year in years_to_verify:
        result = verify_year(config, year, args)
        if result:
            all_results.append(result)

            # Copy if requested
            if args.copy or args.dry_run:
                copy_verified_files(config, year, result, args)

    # Save verification report
    if len(all_results) > 0:
        report_path = os.path.join(config['outputs_dir'], 'recomputed_sentiment_verification.txt')

        with open(report_path, 'w') as f:
            f.write("=" * 80 + "\n")
            f.write("Recomputed Sentiment Files Verification Report\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("=" * 80 + "\n\n")

            f.write("SUMMARY\n")
            f.write("-" * 80 + "\n")
            total_files = sum(r['total_files'] for r in all_results)
            total_valid = sum(r['valid_files'] for r in all_results)
            total_invalid = sum(r['invalid_files'] for r in all_results)

            f.write(f"Total files verified: {total_files}\n")
            f.write(f"Valid files: {total_valid} ({total_valid/total_files*100:.2f}%)\n")
            f.write(f"Invalid files: {total_invalid} ({total_invalid/total_files*100:.2f}%)\n\n")

            f.write("BY YEAR\n")
            f.write("-" * 80 + "\n")
            f.write(f"{'Year':<8} {'Total':<10} {'Valid':<10} {'Invalid':<10} {'Success Rate':<15}\n")
            f.write("-" * 80 + "\n")

            for result in all_results:
                success_rate = result['valid_files'] / result['total_files'] * 100 if result['total_files'] > 0 else 0
                f.write(f"{result['year']:<8} {result['total_files']:<10} {result['valid_files']:<10} "
                        f"{result['invalid_files']:<10} {success_rate:<14.2f}%\n")

        print(f"\n✓ Verification report saved to: {report_path}")

    print("\n" + "=" * 80)
    print("Verification Complete")
    print("=" * 80)

    if args.dry_run:
        print("\n[DRY RUN] No files were actually copied.")
        print("Remove --dry_run and add --copy to actually copy the files.")
    elif args.copy:
        print("\n✓ Files have been copied to the main sentiment directory")
        print("\nNext steps:")
        print("1. Re-run 0.1.5-find-missing-sentiment-files.py to verify completeness")
        print("2. If everything looks good, you can remove the sentiment_computing_path directory")
    else:
        print("\nTo copy the verified files, run with --copy flag")
