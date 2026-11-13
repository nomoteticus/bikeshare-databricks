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