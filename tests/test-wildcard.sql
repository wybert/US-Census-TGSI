INSTALL spatial;
LOAD spatial;

-- Test wildcard loading
CREATE OR REPLACE TABLE test_census AS
  SELECT
    GEOID20,
    STATEFP20
  FROM ST_Read('/n/netscratch/cga/Lab/xiaokang/US-Census-TGSI-workspace/data/census_data_2020/shp/tl_2020_*_tabblock20.shp');

SELECT COUNT(*) as total_blocks FROM test_census;
SELECT STATEFP20, COUNT(*) as block_count FROM test_census GROUP BY STATEFP20 ORDER BY STATEFP20;
