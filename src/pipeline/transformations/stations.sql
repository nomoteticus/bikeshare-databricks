-- Bronze Stations
CREATE OR REFRESH STREAMING TABLE bronze_stations AS 
SELECT 
    station.*,
    _metadata.file_name,
    from_utc_timestamp(TIMESTAMP(last_updated), "Europe/Zurich") as last_updated_ts,
    SIZE(data.stations) AS n_stations
FROM STREAM read_files(
    CONCAT(:raw_path,'/station_status'),
    format => 'json')
LATERAL VIEW EXPLODE(data.stations) AS station;

-- Silver Stations
CREATE OR REFRESH STREAMING TABLE silver_stations 
(
    CONSTRAINT valid_station_id EXPECT  (station_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
    CONSTRAINT valid_provider_id EXPECT  (provider_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
    CONSTRAINT last_reported_ts_this_year EXPECT (last_reported_ts >= "2026-01-01") ON VIOLATION DROP ROW,
    CONSTRAINT valid_num_bikes_available EXPECT (num_bikes_available IS NOT NULL),
    CONSTRAINT num_docks_available EXPECT (num_docks_available IS NOT NULL),
    CONSTRAINT valid_status EXPECT (is_installed AND is_renting AND is_returning)
)
AS 
SELECT 
    station_id,
    provider_id,
    from_utc_timestamp(TIMESTAMP(last_reported), "Europe/Zurich") AS last_reported_ts,
    num_bikes_available,
    num_docks_available,
    is_installed,
    is_renting,
    is_returning,
    file_name,
    last_updated_ts
FROM STREAM bronze_stations;

-- Gold Stations
CREATE OR REFRESH MATERIALIZED VIEW gold_stations
AS
SELECT
    provider_id,
    DATE(last_reported_ts) AS report_date,
    HOUR(last_reported_ts) AS report_hour,
    COUNT(DISTINCT station_id) AS n_stations,
    SUM(num_bikes_available) AS total_bikes_available
FROM silver_stations
GROUP BY ALL;