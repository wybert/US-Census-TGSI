import shapely.ops
import geopandas as gpd
import pandas as pd
import json
import os
from tqdm import tqdm
from pandarallel import pandarallel
pandarallel.initialize(progress_bar=False)

def shift_lon(lon):
    if lon > 0:
        return lon - 360
    else:
        return lon


def shift_geometry(geometry):
    """Apply shift_lon to every coordinate in a geometry."""
    def shift_func(x, y, z=None):
        return (shift_lon(x), y) if z is None else (shift_lon(x), y, z)
    return shapely.ops.transform(shift_func, geometry)

# Load configuration
with open('setting.json') as f:
    config = json.load(f)

census_tracts_path = config["census_geometry"]

census_tracts = gpd.GeoDataFrame()
for file_name in tqdm(list(os.listdir(census_tracts_path))):
    state_census_tracts = gpd.read_file(
        os.path.join(census_tracts_path, file_name))
    # break
    census_tracts = pd.concat([census_tracts, state_census_tracts])

cr_df = pd.read_parquet(
    os.path.join(config["workspace"], "data/all_years_tweet_count_with_pop_CR.parquet"))

# merge geometry on GEOID20

census_tracts_merged = census_tracts.merge(
    cr_df, left_on="GEOID20", right_on="GEOID20", how="left")
# shift the geometry to make map
tqdm.pandas()
census_tracts_merged["geometry"]=census_tracts_merged["geometry"].parallel_apply(shift_geometry)

# save it to parquet
census_tracts_merged.to_parquet(os.path.join(config["workspace"], "data/census_tracts_merged_shifted_geo.parquet"))
