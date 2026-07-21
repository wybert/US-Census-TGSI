import shapely.ops
import geopandas as gpd
import pandas as pd
import numpy as np
import json
import os
import argparse

# ---------- args ----------
parser = argparse.ArgumentParser()
parser.add_argument("--input", help="Path to input block-level CR parquet", default=None)
parser.add_argument("--output", help="Path to output merged tract geoparquet", default=None)
args = parser.parse_args()


def shift_lon(lon):
    return lon - 360 if lon > 0 else lon


def shift_geometry(geometry):
    """Apply shift_lon to every coordinate (handles Aleutian islands at +lon)."""
    def shift_func(x, y, z=None):
        return (shift_lon(x), y) if z is None else (shift_lon(x), y, z)
    return shapely.ops.transform(shift_func, geometry)


with open('setting.json') as f:
    config = json.load(f)

input_cr_path = args.input or os.path.join(
    config["workspace"], "data/all_years_tweet_count_with_pop_CR.parquet")
output_merged_path = args.output or os.path.join(
    config["workspace"], "data/census_tracts_merged_shifted_geo.parquet")

# 1) Aggregate block-level CR -> census tract (GEOID20 first 11 digits).
#    CR must be recomputed from summed tweets/population, not averaged.
#    Population is a static census fact and is always summed over ALL
#    blocks. Tweet counts are summed only over blocks NOT flagged
#    mask_low_coverage=1 upstream (low count, zero population, or
#    coordinate-collapse artifact -- see 03_calculate_cr_*.py), so a single
#    artifact block cannot contaminate its containing tract's map value.
cr = pd.read_parquet(input_cr_path, columns=["GEOID20", "T_i", "P_i", "mask_low_coverage"])
cr["GEOID"] = cr["GEOID20"].astype(str).str[:11]
pop_by_tract = cr.groupby("GEOID", as_index=False).agg(P_i=("P_i", "sum"))
tweets_by_tract = (cr[cr["mask_low_coverage"] == 0]
                   .groupby("GEOID", as_index=False).agg(T_i=("T_i", "sum")))
tract = pop_by_tract.merge(tweets_by_tract, on="GEOID", how="left")
tract["T_i"] = tract["T_i"].fillna(0)

T_tot = tract["T_i"].sum()
P_tot = tract["P_i"].sum()
tract["CR"] = (tract["T_i"] / T_tot) / (tract["P_i"] / P_tot)
with np.errstate(divide="ignore", invalid="ignore"):
    tract["log2CR"] = np.log2(tract["CR"])
tract["mask_low_coverage"] = ((tract["T_i"] < 20) | (tract["P_i"] <= 0)).astype(int)
print(f"Tracts: {len(tract)} | T_tot={T_tot:.0f} P_tot={P_tot:.0f} "
      f"(excluded {int((cr['mask_low_coverage']==1).sum())} masked/artifact blocks from tweet sums)")

# 2) Tract geometry from TIGER 2020 tract shapefiles (GEOID = 11-digit tract).
tract_geo_dir = os.path.join(config["workspace"], "data", "census_tracts_geo")
parts = []
for fn in sorted(os.listdir(tract_geo_dir)):
    if fn.endswith(".zip"):
        parts.append(gpd.read_file(os.path.join(tract_geo_dir, fn))[["GEOID", "geometry"]])
geo = gpd.GeoDataFrame(pd.concat(parts, ignore_index=True), geometry="geometry")
print(f"Tract geometries loaded: {len(geo)}")

# 3) Merge geometry + tract CR, shift longitudes, save.
merged = geo.merge(tract, on="GEOID", how="left")
merged["geometry"] = merged["geometry"].apply(shift_geometry)
merged = gpd.GeoDataFrame(merged, geometry="geometry")
merged.to_parquet(output_merged_path)
print(f"Wrote {output_merged_path}: {len(merged)} tracts, "
      f"{int(merged['mask_low_coverage'].eq(0).sum())} with coverage")
