import os
import json
import sys
import argparse

os.environ["USE_PYGEOS"] = "0"
import geopandas as gpd
from pandarallel import pandarallel

pandarallel.initialize(progress_bar=False)
import pandas as pd
from tqdm import tqdm
import datetime

# Parse command line arguments
parser = argparse.ArgumentParser(description='Spatial join between tweets and census blocks')
parser.add_argument('--year', type=int, help='Process only specific year (e.g., 2010 for testing)')
parser.add_argument('--start-year', type=int, default=2010, help='Start year (default: 2010)')
parser.add_argument('--end-year', type=int, default=2023, help='End year (default: 2023)')
args = parser.parse_args()

# Load configuration
with open('setting.json') as f:
    config = json.load(f)


def spatial_join(row, blocks_gdf, block_suffix):
    # Read parquet file (sentiment tweets already merged)
    df = pd.read_parquet(row["input_file"])

    # Convert latitude/longitude to float (they may be stored as strings)
    df["latitude"] = pd.to_numeric(df["latitude"], errors='coerce')
    df["longitude"] = pd.to_numeric(df["longitude"], errors='coerce')

    # Remove rows with missing coordinates
    df = df.dropna(subset=['latitude', 'longitude'])
    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df["longitude"], df["latitude"]),
        crs="EPSG:4326",
    )
    join_inner_df = gdf.sjoin(blocks_gdf, how="inner")
    join_inner_df = join_inner_df.drop(["geom"], axis=1)
    join_inner_df.to_parquet(
        row["output_file"].replace(".parquet", f"-{block_suffix}.parquet")
    )


input_path_base = config['geotweets_with_sentiment']
output_path_base = config['tweets_with_census_blocks']
census_data_path = config['census_data_2020']

# Determine which years to process
if args.year:
    years_to_process = [args.year]
    print(f"\n{'='*60}")
    print(f"TEST MODE: Processing only year {args.year}")
    print(f"{'='*60}\n")
else:
    years_to_process = range(args.start_year, args.end_year + 1)
    print(f"\n{'='*60}")
    print(f"FULL MODE: Processing years {args.start_year} to {args.end_year}")
    print(f"{'='*60}\n")

t1 = datetime.datetime.now()
# read file paths
files_df = pd.DataFrame()
for year in years_to_process:
    input_path = os.path.join(input_path_base, str(year))

    # Check if directory exists
    if not os.path.exists(input_path):
        print(f"Warning: Directory {input_path} does not exist, skipping year {year}")
        continue

    output_path = os.path.join(output_path_base, str(year))
    os.makedirs(output_path, exist_ok=True)

    input_file_list = [
        os.path.join(input_path, file)
        for file in os.listdir(input_path)
        if file.endswith(".parquet")
    ]

    if len(input_file_list) == 0:
        print(f"Warning: No parquet files found in {input_path}")
        continue

    print(f"Found {len(input_file_list)} files for year {year}")

    file_names = [file.split("/")[-1] for file in input_file_list]
    output_file_names = [os.path.join(output_path, file) for file in file_names]
    files_df = pd.concat(
        [
            files_df,
            pd.DataFrame(
                {
                    "input_file": input_file_list,
                    "output_file": output_file_names,
                    "file_name": file_names,
                    "year": year,
                }
            ),
        ],
        ignore_index=True
    )

# Check if we have any files to process
if len(files_df) == 0:
    print("Error: No files to process!")
    exit(1)

print(f"\n{'='*60}")
print(f"Total files to process: {len(files_df)}")
print(f"Years covered: {sorted(files_df['year'].unique())}")
print(f"Census states to load: {len(os.listdir(census_data_path))}")
print(f"{'='*60}\n")

# process data
# files_df = files_df[files_df["year"] == year]  # Uncomment to test with single year
for census_file_name in tqdm(list(os.listdir(census_data_path))):
    suffix = census_file_name.split(".zip")[0]
    block = gpd.read_file(os.path.join(census_data_path, census_file_name)).to_crs(
        "EPSG:4326"
    )
    files_df.parallel_apply(
        spatial_join,
        args=(
            block,
            suffix,
        ),
        axis=1,
    )
t2 = datetime.datetime.now()
print("all done!")
print("time used:", t2 - t1)
