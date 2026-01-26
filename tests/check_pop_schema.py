import glob
import os

import polars as pl

pop_dir = (
    "/n/netscratch/cga/Lab/xiaokang/US-Census-TGSI-workspace/data/census_pop/pop data"
)
files = glob.glob(os.path.join(pop_dir, "*.parquet"))

print(f"Found {len(files)} files.")

for f in files:
    try:
        s = pl.scan_parquet(f).schema
        if "GEOID20" not in s:
            print(f"MISSING GEOID20: {f} - Schema: {s}")
    except Exception as e:
        print(f"ERROR reading {f}: {e}")
