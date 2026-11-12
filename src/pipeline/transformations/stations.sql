-- Bronze: raw GBFS feeds, one row per array element per file. No type or time zone conversion.

CREATE OR REFRESH STREAMING TABLE bronze_station_status
COMMENT 'Raw station_status feed: one row per station per pulled file'
AS
SELECT
    station.*,
    last_updated,
    ttl,
    SIZE(data.stations) AS n_stations_in_file,
    _metadata.file_name AS file_name,
    _metadata.file_modification_time AS file_modification_time,
    CURRENT_TIMESTAMP() AS ingested_at
FROM STREAM read_files(
    CONCAT(:raw_path, '/station_status'),
    format => 'json')
LATERAL VIEW EXPLODE(data.stations) AS station;


CREATE OR REFRESH STREAMING TABLE bronze_station_information
COMMENT 'Raw station_information feed: one row per station per pulled file'
AS
SELECT
    station.*,
    last_updated,
    ttl,
    _metadata.file_name AS file_name,
    _metadata.file_modification_time AS file_modification_time,
    CURRENT_TIMESTAMP() AS ingested_at
FROM STREAM read_files(
    CONCAT(:raw_path, '/station_information'),
    format => 'json')
LATERAL VIEW EXPLODE(data.stations) AS station;


CREATE OR REFRESH STREAMING TABLE bronze_providers
COMMENT 'Raw providers feed: one row per provider per pulled file'
AS
SELECT
    provider.*,
    last_updated AS feed_last_updated,
    ttl AS feed_ttl,
    _metadata.file_name AS file_name,
    _metadata.file_modification_time AS file_modification_time,
    CURRENT_TIMESTAMP() AS ingested_at
FROM STREAM read_files(
    CONCAT(:raw_path, '/providers'),
    format => 'json')
LATERAL VIEW EXPLODE(data.providers) AS provider;


CREATE OR REFRESH STREAMING TABLE bronze_system_regions
COMMENT 'Raw system_regions feed: one row per region per pulled file'
AS
SELECT
    region.*,
    last_updated,
    ttl,
    _metadata.file_name AS file_name,
    _metadata.file_modification_time AS file_modification_time,
    CURRENT_TIMESTAMP() AS ingested_at
FROM STREAM read_files(
    CONCAT(:raw_path, '/system_regions'),
    format => 'json')
LATERAL VIEW EXPLODE(data.regions) AS region;