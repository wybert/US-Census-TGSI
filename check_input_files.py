import os
import glob

YEAR = 2020
MONTHS = range(1, 13)
BASE_INPUT_DIR = "/n/netscratch/cga/Lab/xiaokang/US-Census-TGSI-workspace/data/geotweets_with_sentiment"

print(f"Checking for input files for year {YEAR} in {BASE_INPUT_DIR}...")
print("-" * 50)

for month in MONTHS:
    month_str = f"{month:02d}"
    input_pattern = os.path.join(BASE_INPUT_DIR, str(YEAR), f"{YEAR}_{month_str}_*.parquet")
    
    matched_files = glob.glob(input_pattern)
    
    print(f"{YEAR}-{month_str}: Found {len(matched_files)} files matching '{input_pattern}'")

print("-" * 50)
print("Verification complete.")
