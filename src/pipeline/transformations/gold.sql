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


-- Gold: station x hour with the station attributes valid at that hour (point-in-time join on SCD2).
CREATE OR REFRESH MATERIALIZED VIEW gold_station_hour_features
CLUSTER BY (hour_ts)
COMMENT 'Docked-bike stations per UTC hour with station attributes valid at that hour'
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
            THEN TIMESTAMP '1970-01-01'
            ELSE TIMESTAMP_SECONDS(__START_AT)
        END AS valid_from,
        COALESCE(TIMESTAMP_SECONDS(__END_AT), TIMESTAMP '9999-12-31') AS valid_to
    FROM silver_station_information
)
SELECT 
    h.*,
    v.name      AS station_name,
    v.lat,
    v.lon,
    v.region_id,
    v.capacity,
    TRY_DIVIDE(LEAST(h.bikes_at_hour_start, v.capacity), v.capacity) AS fill_rate,
    CONCAT(
        CAST(ROUND(v.lat * 10) / 10 AS DECIMAL(4, 1)), '_',
        CAST(ROUND(v.lon * 10) / 10 AS DECIMAL(4, 1))
    ) AS location_id
FROM dev.bikeshare.gold_station_hour AS h
LEFT JOIN station_versions AS v
    ON  h.provider_id = v.provider_id
    AND h.station_id  = v.station_id
    AND h.hour_ts >= v.valid_from
    AND h.hour_ts <  v.valid_to;