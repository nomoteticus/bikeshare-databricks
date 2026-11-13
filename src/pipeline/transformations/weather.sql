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
  cell_lat AS lat,
  cell_lon AS lon,
  COUNT(*) AS n_stations
FROM silver_station_weather_location
GROUP BY location_id, cell_lat, cell_lon;