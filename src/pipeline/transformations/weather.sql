-- Grid cell (0.1°) of each current docked-bike station
CREATE OR REFRESH MATERIALIZED VIEW silver_station_weather_location
COMMENT "Current docked-bike stations mapped to a 0.1 degree weather grid cell (cell center)"
AS
WITH current_bike_stations  AS (
SELECT 
    *,
    CAST(ROUND(lat * 10) / 10 AS DECIMAL(4, 1)) AS cell_lat,   -- nearest multiple of 0.1
    CAST(ROUND(lon * 10) / 10 AS DECIMAL(4, 1)) AS cell_lon
FROM silver_station_information
WHERE __END_AT IS NULL
  AND provider_id IN (SELECT provider_id FROM ref_bike_systems)
)
SELECT
    provider_id,
    station_id, 
    lat,            -- lat/lon = station centroid used for the weather query
    lon,
    cell_lat,
    cell_lon,
    CONCAT(cell_lat, '_', cell_lon)                       AS location_id -- e.g. '47.4_8.5'
FROM current_bike_stations;

-- One row per grid cell: the list of places the weather pull asks for
CREATE OR REFRESH MATERIALIZED VIEW silver_weather_locations
COMMENT "Weather grid cells (0.1 degree) that contain at least one docked-bike station"
AS
SELECT
  location_id,
  cell_lat,
  cell_lon,
  CAST(ROUND(AVG(lat), 2) AS DECIMAL(5, 2)) AS lat,    -- centroid of the stations in the cell
  CAST(ROUND(AVG(lon), 2) AS DECIMAL(5, 2)) AS lon,
  COUNT(*) AS n_stations
FROM silver_station_weather_location
GROUP BY location_id, cell_lat, cell_lon;


-- Bronze: raw Open-Meteo pulls, one row per location per hour per pulled file. No conversions.

-- Explicit schema: lets the pipeline start on an empty raw/weather folder (prod incident 13 Nov 2026).
-- When adding a weather variable, add it here as well as in pull_weather.ipynb.
CREATE OR REFRESH STREAMING TABLE bronze_weather
COMMENT 'Raw Open-Meteo pulls: one row per location per hour per pulled file'
AS
SELECT
    location_id,
    requested_lat,
    requested_lon,
    latitude  AS model_lat,                    -- grid point the model actually used
    longitude AS model_lon,
    elevation,
    model,
    run_date,
    pulled_at,
    time,                                      -- Unix seconds, UTC
    hourly.temperature_2m[pos]       AS temperature_2m,
    hourly.relative_humidity_2m[pos] AS relative_humidity_2m,
    hourly.precipitation[pos]        AS precipitation,
    hourly.snowfall[pos]             AS snowfall,
    hourly.weather_code[pos]         AS weather_code,
    hourly.cloud_cover[pos]          AS cloud_cover,
    hourly.wind_speed_10m[pos]       AS wind_speed_10m,
    hourly.wind_gusts_10m[pos]       AS wind_gusts_10m,
    hourly.sunshine_duration[pos]    AS sunshine_duration,
    _metadata.file_name              AS file_name,
    _metadata.file_modification_time AS file_modification_time,
    CURRENT_TIMESTAMP()              AS ingested_at
FROM STREAM read_files(
    CONCAT(:raw_path, '/weather'),
    format => 'json',
    schema => '
    location_id STRING,
    requested_lat DOUBLE,
    requested_lon DOUBLE,
    latitude DOUBLE,
    longitude DOUBLE,
    elevation DOUBLE,
    model STRING,
    run_date STRING,
    pulled_at STRING,
    hourly STRUCT<
        time: ARRAY<BIGINT>,
        temperature_2m: ARRAY<DOUBLE>,
        relative_humidity_2m: ARRAY<BIGINT>,
        precipitation: ARRAY<DOUBLE>,
        snowfall: ARRAY<DOUBLE>,
        weather_code: ARRAY<BIGINT>,
        cloud_cover: ARRAY<BIGINT>,
        wind_speed_10m: ARRAY<DOUBLE>,
        wind_gusts_10m: ARRAY<DOUBLE>,
        sunshine_duration: ARRAY<DOUBLE>
    >')   
LATERAL VIEW POSEXPLODE(hourly.time) AS pos, time;


-- Silver input: typed rows from bronze (pipeline-internal, not published)
CREATE TEMPORARY VIEW weather_typed
AS
SELECT
    location_id,
    TIMESTAMP_SECONDS(time)       AS valid_ts,      -- UTC; sums (precipitation, snowfall, sunshine) cover the hour before
    CAST(pulled_at AS TIMESTAMP)  AS pulled_ts,
    TIMESTAMP_SECONDS(time) > CAST(pulled_at AS TIMESTAMP) AS is_forecast,
    temperature_2m,
    relative_humidity_2m,
    precipitation,
    snowfall,
    weather_code,
    cloud_cover,
    wind_speed_10m,
    wind_gusts_10m,
    sunshine_duration,
    model,
    elevation,
    model_lat,
    model_lon
FROM STREAM bronze_weather;


-- Silver: one row per location and hour, values from the most recent pull
CREATE OR REFRESH STREAMING TABLE silver_weather
(
    CONSTRAINT valid_location_id EXPECT (location_id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_valid_ts EXPECT (valid_ts IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_pulled_ts EXPECT (pulled_ts IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT has_temperature EXPECT (temperature_2m IS NOT NULL),
    CONSTRAINT plausible_temperature EXPECT (temperature_2m BETWEEN -40 AND 45),
    CONSTRAINT valid_precipitation EXPECT (precipitation >= 0),
    CONSTRAINT valid_humidity EXPECT (relative_humidity_2m BETWEEN 0 AND 100)
)
COMMENT 'Hourly weather per location (0.1 degree cell), latest Open-Meteo pull wins. Timestamps UTC.';

CREATE FLOW silver_weather_flow AS AUTO CDC INTO silver_weather
FROM STREAM(weather_typed)
KEYS (location_id, valid_ts)
IGNORE NULL UPDATES
SEQUENCE BY pulled_ts;