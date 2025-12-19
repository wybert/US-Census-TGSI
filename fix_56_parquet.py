import os

import polars as pl

file_path = "/n/netscratch/cga/Lab/xiaokang/US-Census-TGSI-workspace/data/census_pop/pop data/56.parquet"

print(f"Fixing {file_path}...")

# Read the malformed parquet
df = pl.read_parquet(file_path)

# Extract GEOID20 from GEO_ID (remove "1000000US")
# Check if GEO_ID exists
if "GEO_ID" in df.columns:
    df = df.with_columns(pl.col("GEO_ID").str.replace("1000000US", "").alias("GEOID20"))
else:
    print("Error: GEO_ID column not found.")
    exit(1)

# Rename P1_001N to population
if "P1_001N" in df.columns:
    df = df.rename({"P1_001N": "population"})
else:
    print("Error: P1_001N column not found.")
    exit(1)

# Select only relevant columns
df = df.select(["GEOID20", "population"])

# Verify schema
print(df.schema)
print(df.head())

# Save back
df.write_parquet(file_path)
print("Fixed and saved.")
