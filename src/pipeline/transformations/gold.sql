-- Gold: docked-bike stations at station x hour grain. hour_ts is the UTC hour start.
CREATE OR REFRESH MATERIALIZED VIEW gold_station_hour
CLUSTER BY (hour_ts)
COMMENT 'Docked-bike stations: one row per station and UTC hour'
AS
SELECT
    s.provider_id,
    s.station_id,
    DATE_TRUNC('hour', s.snapshot_ts)               AS hour_ts,   
    COUNT(*)                                        AS n_snapshots,
    MIN_BY(s.num_bikes_available, s.snapshot_ts)    AS bikes_at_hour_start,
    AVG(s.num_bikes_available)                      AS bikes_avg,
    MIN(s.num_bikes_available)                      AS bikes_min,
    MAX(s.num_bikes_available)                      AS bikes_max,
    AVG(s.num_docks_available)                      AS docks_avg,
    BOOL_AND(s.is_in_service)                       AS all_in_service      
FROM silver_station_status AS s
LEFT SEMI JOIN ref_bike_systems AS b
    ON s.provider_id = b.provider_id
GROUP BY s.provider_id, s.station_id, hour_ts;


-- Gold: station x hour with the station attributes valid at that hour (point-in-time join on SCD2) and weather.
CREATE OR REFRESH MATERIALIZED VIEW gold_station_hour_features
CLUSTER BY (hour_ts)
COMMENT 'Docked-bike stations per UTC hour with station attributes valid at that hour and weather'
AS
WITH station_versions AS (
    SELECT
        provider_id,
        station_id,
        name,
        lat,
        lon,
        capacity,
        region_id,
        CASE
            WHEN LAG(__START_AT) OVER (PARTITION BY provider_id, station_id ORDER BY __START_AT) IS NULL
            THEN TIMESTAMP '1970-01-01'                       -- first version: valid from the beginning
            ELSE TIMESTAMP_SECONDS(__START_AT)
        END AS valid_from,
        COALESCE(TIMESTAMP_SECONDS(__END_AT), TIMESTAMP '9999-12-31') AS valid_to   -- open version: __END_AT is NULL
    FROM silver_station_information
),
station_hour AS (
    SELECT
        h.*,
        v.name      AS station_name,
        v.lat,
        v.lon,
        v.region_id,
        v.capacity,
        TRY_DIVIDE(LEAST(h.bikes_at_hour_start, v.capacity), v.capacity) AS fill_rate,   -- capped at 1, NULL without capacity
        CONCAT(
            CAST(ROUND(v.lat * 10) / 10 AS DECIMAL(4, 1)), '_',
            CAST(ROUND(v.lon * 10) / 10 AS DECIMAL(4, 1))
        )           AS location_id                            -- same formula as silver_station_weather_location
    FROM gold_station_hour AS h
    LEFT JOIN station_versions AS v                           -- keep station-hours even when no attributes match
        ON  h.provider_id = v.provider_id
        AND h.station_id  = v.station_id
        AND h.hour_ts >= v.valid_from
        AND h.hour_ts <  v.valid_to
)
SELECT
    sh.*,
    -- instant values at the hour start
    w.temperature_2m,
    w.relative_humidity_2m,
    w.cloud_cover,
    w.wind_speed_10m,
    w.weather_code,
    -- hour before the hour start
    w.precipitation            AS precipitation_prev_hour,
    w.snowfall                 AS snowfall_prev_hour,
    w.sunshine_duration        AS sunshine_prev_hour,
    w.wind_gusts_10m           AS wind_gusts_prev_hour,
    -- during the bike hour
    w_next.precipitation       AS precipitation_in_hour,
    w_next.snowfall            AS snowfall_in_hour,
    w_next.sunshine_duration   AS sunshine_in_hour,
    w_next.wind_gusts_10m      AS wind_gusts_in_hour,
    w.is_forecast              AS weather_is_forecast
FROM station_hour AS sh
LEFT JOIN silver_weather AS w                                 -- keep bike rows even without weather
    ON  sh.location_id = w.location_id
    AND w.valid_ts = sh.hour_ts
LEFT JOIN silver_weather AS w_next
    ON  sh.location_id = w_next.location_id
    AND w_next.valid_ts = sh.hour_ts + INTERVAL 1 HOUR;