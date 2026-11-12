-- Bronze: raw GBFS feeds, one row per array element per file. No type or time zone conversion.

CREATE OR REFRESH STREAMING TABLE bronze_station_status
COMMENT 'Raw station_status feed: one row per station per pulled file'
AS
SELECT
    station.*,
    last_updated AS feed_last_updated,
    ttl AS feed_ttl,
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
    last_updated AS feed_last_updated,
    ttl AS feed_ttl,
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
    last_updated AS feed_last_updated,
    ttl AS feed_ttl,
    _metadata.file_name AS file_name,
    _metadata.file_modification_time AS file_modification_time,
    CURRENT_TIMESTAMP() AS ingested_at
FROM STREAM read_files(
    CONCAT(:raw_path, '/system_regions'),
    format => 'json')
LATERAL VIEW EXPLODE(data.regions) AS region;


-- Silver: typed and validated station status, all providers. Timestamps are UTC.

CREATE OR REFRESH STREAMING TABLE silver_station_status
(
    CONSTRAINT valid_station_id EXPECT (station_id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_provider_id EXPECT (provider_id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_snapshot_ts EXPECT (snapshot_ts IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_num_bikes EXPECT (num_bikes_available >= 0) ON VIOLATION DROP ROW,
    CONSTRAINT reported_within_30_days EXPECT (
        last_reported_ts IS NULL OR last_reported_ts >= snapshot_ts - INTERVAL 30 DAYS
    ),
    CONSTRAINT plausible_num_bikes EXPECT (num_bikes_available <= 200),
    CONSTRAINT station_in_service EXPECT (is_in_service),
    CONSTRAINT valid_num_docks EXPECT (num_docks_available IS NULL OR num_docks_available >= 0)
)
CLUSTER BY (snapshot_ts)
COMMENT 'Station status per snapshot, typed and validated, all providers'
AS
SELECT
    provider_id,
    station_id,
    TIMESTAMP_SECONDS(feed_last_updated) AS snapshot_ts,
    TIMESTAMP_SECONDS(last_reported) AS last_reported_ts,
    ROUND((feed_last_updated - last_reported) / 3600.0, 1) AS hours_since_reported,
    num_bikes_available,
    num_docks_available,
    is_installed,
    is_renting,
    is_returning,
    COALESCE(is_installed AND is_renting AND is_returning, FALSE) AS is_in_service,
    file_name
FROM STREAM bronze_station_status;


-- Reference data: latest version of each provider and region (SCD type 1)

CREATE OR REFRESH STREAMING TABLE silver_providers
(
    CONSTRAINT valid_provider_id EXPECT (provider_id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT has_vehicle_type EXPECT (vehicle_type IS NOT NULL)
)
COMMENT 'Current attributes per provider, including vehicle_type';

CREATE FLOW silver_providers_flow AS AUTO CDC INTO silver_providers
FROM STREAM(bronze_providers)
KEYS (provider_id)
SEQUENCE BY feed_last_updated
COLUMNS provider_id, name, vehicle_type, timezone, url, last_updated, feed_last_updated
STORED AS SCD TYPE 1;


CREATE OR REFRESH STREAMING TABLE silver_system_regions
(
    CONSTRAINT valid_region_id EXPECT (region_id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT has_name EXPECT (name IS NOT NULL)
)
COMMENT 'Current name per region';

CREATE FLOW silver_system_regions_flow AS AUTO CDC INTO silver_system_regions
FROM STREAM(bronze_system_regions)
KEYS (region_id)
SEQUENCE BY feed_last_updated
COLUMNS * EXCEPT (feed_ttl, file_name, file_modification_time, ingested_at)
STORED AS SCD TYPE 1;


-- Station metadata with history (SCD type 2): a new version only when a tracked column changes

CREATE OR REFRESH STREAMING TABLE silver_station_information
(
    CONSTRAINT valid_station_id EXPECT (station_id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_provider_id EXPECT (provider_id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT coordinates_in_switzerland EXPECT (
        lat BETWEEN 45.7 AND 47.9 AND lon BETWEEN 5.9 AND 10.6
    ),
    CONSTRAINT has_capacity EXPECT (capacity IS NOT NULL),
    CONSTRAINT valid_capacity EXPECT (capacity IS NULL OR capacity > 0)
)
COMMENT 'Station attributes with validity periods (__START_AT, __END_AT)';
CREATE FLOW silver_station_information_flow AS AUTO CDC INTO silver_station_information
FROM STREAM(bronze_station_information)
KEYS (provider_id, station_id)
SEQUENCE BY feed_last_updated
COLUMNS provider_id, station_id, name, short_name, address, post_code, lat, lon, capacity, region_id, feed_last_updated
STORED AS SCD TYPE 2
TRACK HISTORY ON name, lat, lon, capacity, region_id;