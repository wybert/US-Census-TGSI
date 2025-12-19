#!/usr/bin/env python
"""
Downloads Census 2020 Block-Level Population Data (P1_001N) for all states
using the Census API.
"""

import os
import json
import requests
import pandas as pd
from tqdm import tqdm
import time

# Load configuration
with open('setting.json') as f:
    config = json.load(f)

# Output directory for population data
output_pop_dir = config['census_pop']
os.makedirs(output_pop_dir, exist_ok=True)

# List of states (FIPS codes) - same as in 0.1-download_cenus_data.py
STATE_FIPS = [
    "01", "02", "04", "05", "06", "08", "09", "10", "11", "12",
    "13", "15", "16", "17", "18", "19", "20", "21", "22", "23",
    "24", "25", "26", "27", "28", "29", "30", "31", "32", "33",
    "34", "35", "36", "37", "38", "39", "40", "41", "42", "44",
    "45", "46", "47", "48", "49", "50", "51", "53", "54", "55", "56"
]

# Census API endpoint for 2020 P.L. 94-171 Redistricting Data
API_URL_BASE = "https://api.census.gov/data/2020/dec/pl"
# We want 'P1_001N': Total Population (from Table P1)
# We want 'GEO_ID': Geographic Identifier
# We want all blocks ('for=block:*') within each state ('in=state:XX')
CENSUS_VARS = "P1_001N,GEO_ID"

print("=" * 60)
print("Downloading Census 2020 Block-Level Population Data")
print("=" * 60)
print(f"Output directory: {output_pop_dir}")
print(f"Total states to download: {len(STATE_FIPS)}\n")

downloaded_count = 0
skipped_count = 0
failed_count = 0

for fips in tqdm(STATE_FIPS, desc="Processing states"):
    output_filepath = os.path.join(output_pop_dir, f"{fips}.parquet")

    if os.path.exists(output_filepath):
        tqdm.write(f"✓ Skipping FIPS {fips} - already downloaded: {output_filepath}")
        skipped_count += 1
        continue

    params = {
        "get": CENSUS_VARS,
        "for": "block", # Query for 'block'
        "in": f"state:{fips} county:*", # within all counties of the state
        # "key": "YOUR_CENSUS_API_KEY" # Optional: Add your API key if you have one to increase request limits
    }

    tqdm.write(f"↓ Downloading population for State FIPS: {fips}...")
    try:
        response = requests.get(API_URL_BASE, params=params, timeout=300) # 5 min timeout
        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx) 
        
        data = response.json()
        
        # The first row is headers, subsequent rows are data
        headers = data[0]
        rows = data[1:]
        
        if not rows:
            tqdm.write(f"⚠ No data returned for FIPS {fips}. Skipping.")
            skipped_count += 1
            continue

        df = pd.DataFrame(rows, columns=headers)
        
        # Rename P1_001N to population, and process GEO_ID to match GEOID20
        df = df.rename(columns={'P1_001N': 'population'})
        df['GEOID20'] = df['GEO_ID'].str.replace('1000000US', '') # Remove '1000000US' prefix
        
        # Select relevant columns and convert population to integer
        df = df[['GEOID20', 'population']].copy()
        df['population'] = pd.to_numeric(df['population'], errors='coerce').fillna(0).astype(int)
        
        # Save to parquet
        df.to_parquet(output_filepath, index=False)
        tqdm.write(f"✓ Downloaded and saved {len(df):,} blocks for FIPS {fips} to {output_filepath}")
        downloaded_count += 1
        
    except requests.exceptions.RequestException as e:
        tqdm.write(f"✗ Failed to download FIPS {fips} due to network/API error: {e}")
        failed_count += 1
    except json.JSONDecodeError:
        tqdm.write(f"✗ Failed to decode JSON response for FIPS {fips}. API might have returned an error message.")
        tqdm.write(f"  Response content: {response.text[:200]}...")
        failed_count += 1
    except Exception as e:
        tqdm.write(f"✗ An unexpected error occurred for FIPS {fips}: {e}")
        failed_count += 1
        
    time.sleep(0.5) # Be kind to the API, wait 0.5 seconds between requests

print("\n" + "="*60)
print("Download Summary:")
print(f"  Downloaded: {downloaded_count}")
print(f"  Skipped:    {skipped_count}")
print(f"  Failed:     {failed_count}")
print(f"  Total states in list: {len(STATE_FIPS)}")
print("="*60)
