# This script downloads census data for each state in the US.

import os
import json

# Load configuration
with open('setting.json') as f:
    config = json.load(f)

state_list = {"01":"ALABAMA",
"02":"ALASKA",
"04":"ARIZONA",
"05":"ARKANSAS",
"06":"CALIFORNIA",
"08":"COLORADO",
"09":"CONNECTICUT",
"10":"DELAWARE",
"11":"DISTRICT OF COLUMBIA",
"12":"FLORIDA",
"13":"GEORGIA",
"15":"HAWAII",
"16":"IDAHO",
"17":"ILLINOIS",
"18":"INDIANA",
"19":"IOWA",
"20":"KANSAS",
"21":"KENTUCKY",
"22":"LOUISIANA",
"23":"MAINE",
"24":"MARYLAND",
"25":"MASSACHUSETTS",
"26":"MICHIGAN",
"27":"MINNESOTA",
"28":"MISSISSIPPI",
"29":"MISSOURI",
"30":"MONTANA",
"31":"NEBRASKA",
"32":"NEVADA",
"33":"NEW HAMPSHIRE",
"34":"NEW JERSEY",
"35":"NEW MEXICO",
"36":"NEW YORK",
"37":"NORTH CAROLINA",
"38":"NORTH DAKOTA",
"39":"OHIO",
"40":"OKLAHOMA",
"41":"OREGON",
"42":"PENNSYLVANIA",
"44":"RHODE ISLAND",
"45":"SOUTH CAROLINA",
"46":"SOUTH DAKOTA",
"47":"TENNESSEE",
"48":"TEXAS",
"49":"UTAH",
"50":"VERMONT",
"51":"VIRGINIA",
"53":"WASHINGTON",
"54":"WEST VIRGINIA",
"55":"WISCONSIN",
"56":"WYOMING"}

# base_url = "https://www2.census.gov/geo/tiger/TIGER2021/TABBLOCK20/tl_2021_%s_tabblock20.zip"
base_url = "https://www2.census.gov/geo/tiger/TIGER2020/TABBLOCK20/tl_2020_%s_tabblock20.zip"
# https://www2.census.gov/geo/tiger/TIGER2020/TRACT/tl_2020_01_tract.zip
outpath = config['census_data_2020'] + "/"
os.makedirs(outpath, exist_ok=True)

from tqdm import tqdm

# Track download statistics
downloaded = 0
skipped = 0
failed = 0

print(f"\nChecking and downloading census data to: {outpath}")
print(f"Total states to process: {len(state_list)}\n")

for state in tqdm(state_list, total=len(state_list), desc="Processing states"):
    url = base_url % state
    filename = f"tl_2020_{state}_tabblock20.zip"
    filepath = os.path.join(outpath, filename)

    # Check if file already exists
    if os.path.exists(filepath):
        file_size = os.path.getsize(filepath)
        # Only skip if file size is > 0 (valid file)
        if file_size > 0:
            tqdm.write(f"✓ Skipping {state_list[state]:20s} - already downloaded ({file_size:,} bytes)")
            skipped += 1
            continue
        else:
            tqdm.write(f"⚠ Re-downloading {state_list[state]:20s} - existing file is empty")
            os.remove(filepath)

    # Download the file
    tqdm.write(f"↓ Downloading {state_list[state]:20s}...")
    result = os.system(f"wget -q {url} -P {outpath}")

    if result == 0:
        downloaded += 1
    else:
        tqdm.write(f"✗ Failed to download {state_list[state]}")
        failed += 1

print("\n" + "="*60)
print("Download Summary:")
print(f"  Downloaded: {downloaded}")
print(f"  Skipped:    {skipped}")
print(f"  Failed:     {failed}")
print(f"  Total:      {len(state_list)}")
print("="*60)
