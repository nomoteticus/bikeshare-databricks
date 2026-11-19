/*
Station to city: one row per current docked-bike station that lies in a city of ref_cities. Single definition of the
city mapping, reused by gold_city_hour and gold_station_hour_features.

RULE: a station belongs to the NEAREST city whose radius_km contains it (haversine distance to the city center,
earth radius 6,371 km). Stations outside every radius are not listed. Uses current station positions
(silver_station_weather_location), like the weather-cell mapping; stations rarely move, so no history is kept.
*/

CREATE OR REFRESH MATERIALIZED VIEW silver_station_city (
  provider_id                 STRING COMMENT 'Provider of the station',
  station_id                  STRING COMMENT 'Station id as in the feed (<system>:<id>)',
  city_id                     STRING COMMENT 'City key, see ref_cities',
  distance_to_city_center_km  DOUBLE COMMENT 'Great-circle distance from the station to the city center, km'
)
COMMENT 'Current docked-bike stations mapped to the nearest city of ref_cities within its radius'
AS
WITH station_city_distance AS (
    SELECT
        s.provider_id,
        s.station_id,
        c.city_id,
        c.radius_km,
        2 * 6371 * ASIN(SQRT(
              POWER(SIN(RADIANS(s.lat - c.center_lat) / 2), 2)
            + COS(RADIANS(c.center_lat)) * COS(RADIANS(s.lat)) * POWER(SIN(RADIANS(s.lon - c.center_lon) / 2), 2)
        )) AS distance_km
    FROM silver_station_weather_location AS s
    CROSS JOIN ref_cities AS c
)
SELECT
    provider_id,
    station_id,
    city_id,
    CAST(ROUND(distance_km, 3) AS DOUBLE) AS distance_to_city_center_km
FROM station_city_distance
WHERE distance_km <= radius_km
QUALIFY ROW_NUMBER() OVER (PARTITION BY provider_id, station_id ORDER BY distance_km) = 1;


/*
Gold: one row per city (ref_cities) and UTC hour, on a complete hour spine (missing hours still get a row).
Semantic layer for the daily animation and candidate city-level features. All business rules live here:

STATION TO CITY: from silver_station_city (rule documented there).

DATA STATUS (first matching rule wins):
    glitch_known      the hour lies in a confirmed incident of a provider present in the city (ref_data_incidents)
    glitch_suspected  AUTOMATIC RULE: the provider's NATIONAL total of docked bikes at the hour start is below 80% of
                       its maximum within +/-12 hours AND at least 200 bikes below it. Evaluated on all stations of
                       the provider, not only the city. The 80% part separates feed faults from commuting in large
                       fleets (commute dips 2 to 6%; Velospot fault 18 Nov 2026: 63%, about 2,800 bikes). The 200-bike
                       part was added 19 Nov 2026 after false positives in Yverdon (one-city fleet of about 130 bikes,
                       normal 08:00 commute dips of 25 to 33 bikes = up to 26%). Faults in small fleets are therefore not
                       detected automatically; confirmed ones go into ref_data_incidents.
     missing           no snapshot for the city in that hour (pull failed or not yet arrived)
     partial           fewer than 90% of the city's stations reported in that hour
     ok                none of the above

CLEANED COUNT: docked_bikes = docked_bikes_raw when ok; otherwise linear interpolation in time between the nearest
ok hours before and after (or the nearest ok hour at the edges); is_imputed marks those rows.

BIKES OUT (estimate, for display; NOT usable as a forecasting feature because it uses future hours):
fleet_baseline_bikes = max of docked_bikes within +/-12 hours (rows 12 PRECEDING to 12 FOLLOWING; exact because
the spine has no gaps). bikes_out_est = fleet_baseline_bikes - docked_bikes. The rolling baseline absorbs slow
fleet changes (repairs, operator moves); found 19 Nov 2026 in Zurich, where docked bikes at night fell about 65
in two days. The last 12 hours are provisional: their baseline can still rise when new hours arrive.

WEATHER: from the city's weather cell (ref_cities.weather_location_id). Instant values at the hour start; precipitation
and snowfall summed DURING the hour (silver_weather row stamped hour_ts + 1 hour, as in gold_station_hour_features).

SUN: from silver_sun_location_hour (calendar.sql) for the city's weather cell (rules documented there).

Local time: Europe/Zurich, stored as TIMESTAMP holding the wall-clock time.

Refresh: this view has expectations and unbounded windows, so every pipeline update recomputes it fully. That is
intended: the table is small (12 cities x 24 hours a day), and a full recompute also re-evaluates the +/-12 h
baseline, the interpolation and the 7-day peak whenever new hours arrive.
*/

CREATE OR REFRESH MATERIALIZED VIEW gold_city_hour (
  city_id                  STRING    COMMENT 'City key, see ref_cities',
  city_name                STRING    COMMENT 'Display name of the city',
  hour_ts                  TIMESTAMP COMMENT 'Hour start, UTC',
  hour_local_ts            TIMESTAMP COMMENT 'Hour start, Europe/Zurich wall-clock time',
  date_local               DATE      COMMENT 'Local date of the hour',
  hour_local               INT       COMMENT 'Local hour of day, 0 to 23',
  weekday_local            STRING    COMMENT 'Local weekday, Mon to Sun',
  is_weekend               BOOLEAN   COMMENT 'Saturday or Sunday (local); public holidays not included',
  n_stations_city          INT       COMMENT 'Docked-bike stations assigned to the city (current stations)',
  n_stations_reporting     INT       COMMENT 'Stations of the city with a snapshot in this hour',
  docked_bikes_raw         INT       COMMENT 'Sum of bikes docked at the hour start (first snapshot) over reporting stations; NULL if missing',
  data_status              STRING    COMMENT 'ok, missing, partial, glitch_known or glitch_suspected; rules in the header of city.sql',
  is_imputed               BOOLEAN   COMMENT 'TRUE when docked_bikes is interpolated because data_status is not ok',
  docked_bikes             INT       COMMENT 'Cleaned docked bikes: raw value when ok, else linear interpolation between neighboring ok hours',
  fleet_baseline_bikes     INT       COMMENT 'Max of docked_bikes within +/-12 hours: the bikes the city would hold if none were out',
  bikes_out_est            INT       COMMENT 'Estimated bikes out of docks = fleet_baseline_bikes - docked_bikes; uses future hours, display only, not a forecasting feature',
  bikes_out_provisional    BOOLEAN   COMMENT 'TRUE for the last 12 hours of data: baseline may still change as new hours arrive',
  bikes_out_peak_recent_7d INT       COMMENT 'Max bikes_out_est of the city over the last 168 hours of data (same value on all rows); display scale',
  bikes_out_share_of_peak  DOUBLE    COMMENT 'bikes_out_est / bikes_out_peak_recent_7d, 0 to 1; the animation draws 9 bikes at 1',
  temperature_c            DOUBLE    COMMENT 'Air temperature at 2 m at the hour start, deg C (city weather cell)',
  precipitation_mm         DOUBLE    COMMENT 'Total precipitation during the hour, mm (rain + snow water equivalent)',
  snowfall_cm              DOUBLE    COMMENT 'Snowfall during the hour, cm',
  precipitation_kind       STRING    COMMENT 'snow if snowfall_cm > 0, rain if precipitation_mm >= 0.1, else none',
  cloud_cover_pct          DOUBLE    COMMENT 'Cloud cover at the hour start, percent',
  weather_code             INT       COMMENT 'WMO weather code at the hour start',
  weather_is_forecast      BOOLEAN   COMMENT 'TRUE when the weather values are still a forecast (after the latest weather pull)',
  sunrise_local            TIMESTAMP COMMENT 'Sunrise on date_local for the city weather cell, Europe/Zurich wall-clock time',
  sunset_local             TIMESTAMP COMMENT 'Sunset on date_local for the city weather cell, Europe/Zurich wall-clock time',
  daylight_hours           DOUBLE    COMMENT 'Hours between sunrise and sunset on date_local',
  sun_elevation_deg        DOUBLE    COMMENT 'Sun elevation above the horizon at the hour start, degrees (negative = below)',
  light_phase              STRING    COMMENT 'day (elevation > -0.833), twilight (to -6) or night, at the hour start',
  CONSTRAINT known_status        EXPECT (data_status IN ('ok', 'missing', 'partial', 'glitch_known', 'glitch_suspected')),
  CONSTRAINT bikes_out_not_negative EXPECT (bikes_out_est >= 0),
  CONSTRAINT has_weather         EXPECT (temperature_c IS NOT NULL)
)
CLUSTER BY (city_id, hour_ts)
COMMENT 'City x UTC hour: docked bikes (raw and cleaned), data status, estimated bikes out, weather and sun. Rules in city.sql'
AS
WITH hour_spine AS (                                          -- every UTC hour from the first to the last gold hour
    SELECT EXPLODE(SEQUENCE(first_hour_ts, last_hour_ts, INTERVAL 1 HOUR)) AS hour_ts
    FROM (SELECT MIN(hour_ts) AS first_hour_ts, MAX(hour_ts) AS last_hour_ts FROM gold_station_hour)
),
city_size AS (
    SELECT city_id, COUNT(*) AS n_stations_city
    FROM silver_station_city
    GROUP BY city_id
),
city_providers AS (
    SELECT DISTINCT city_id, provider_id
    FROM silver_station_city
),
city_observed AS (                                            -- observed hours only
    SELECT
        sc.city_id,
        h.hour_ts,
        COUNT(*)                   AS n_stations_reporting,
        SUM(h.bikes_at_hour_start) AS docked_bikes_raw
    FROM gold_station_hour AS h
    JOIN silver_station_city AS sc
        ON  h.provider_id = sc.provider_id
        AND h.station_id  = sc.station_id
    GROUP BY sc.city_id, h.hour_ts
),
provider_totals AS (                                          -- national total per provider on the full spine
    SELECT
        b.provider_id,
        sp.hour_ts,
        t.total_bikes
    FROM ref_bike_systems AS b
    CROSS JOIN hour_spine AS sp
    LEFT JOIN (
        SELECT provider_id, hour_ts, SUM(bikes_at_hour_start) AS total_bikes
        FROM gold_station_hour
        GROUP BY provider_id, hour_ts
    ) AS t
        ON  t.provider_id = b.provider_id
        AND t.hour_ts     = sp.hour_ts
),
provider_suspect AS (                                         -- automatic glitch rule (see header)
    SELECT provider_id, hour_ts
    FROM (
        SELECT
            provider_id,
            hour_ts,
            total_bikes,
            MAX(total_bikes) OVER (PARTITION BY provider_id ORDER BY hour_ts
                                   ROWS BETWEEN 12 PRECEDING AND 12 FOLLOWING) AS total_max_24h
        FROM provider_totals
    )
    WHERE total_bikes < 0.8 * total_max_24h                   -- relative drop of more than 20% ...
      AND total_max_24h - total_bikes >= 200                   -- ... and at least 200 bikes (small fleets: see header)
),
city_known_glitch AS (
    SELECT DISTINCT cp.city_id, sp.hour_ts
    FROM city_providers AS cp
    JOIN ref_data_incidents AS i
        ON cp.provider_id = i.provider_id
    JOIN hour_spine AS sp
        ON  sp.hour_ts >= i.start_ts
        AND sp.hour_ts <  i.end_ts
),
city_suspect_glitch AS (
    SELECT DISTINCT cp.city_id, ps.hour_ts
    FROM city_providers AS cp
    JOIN provider_suspect AS ps
        ON cp.provider_id = ps.provider_id
),
city_hour_status AS (
    SELECT
        c.city_id,
        c.city_name,
        c.weather_location_id,
        sp.hour_ts,
        cs.n_stations_city,
        COALESCE(o.n_stations_reporting, 0) AS n_stations_reporting,
        o.docked_bikes_raw,
        CASE
            WHEN kg.hour_ts IS NOT NULL                                      THEN 'glitch_known'
            WHEN sg.hour_ts IS NOT NULL                                      THEN 'glitch_suspected'
            WHEN o.docked_bikes_raw IS NULL                                  THEN 'missing'
            WHEN o.n_stations_reporting < 0.9 * cs.n_stations_city           THEN 'partial'
            ELSE 'ok'
        END AS data_status
    FROM ref_cities AS c
    JOIN city_size AS cs
        ON c.city_id = cs.city_id
    CROSS JOIN hour_spine AS sp
    LEFT JOIN city_observed AS o
        ON  o.city_id = c.city_id
        AND o.hour_ts = sp.hour_ts
    LEFT JOIN city_known_glitch AS kg
        ON  kg.city_id = c.city_id
        AND kg.hour_ts = sp.hour_ts
    LEFT JOIN city_suspect_glitch AS sg
        ON  sg.city_id = c.city_id
        AND sg.hour_ts = sp.hour_ts
),
city_hour_neighbors AS (                                      -- nearest ok hours before and after, for interpolation
    SELECT
        *,
        CASE WHEN data_status = 'ok' THEN docked_bikes_raw END AS ok_bikes,
        LAST_VALUE(CASE WHEN data_status = 'ok' THEN docked_bikes_raw END, TRUE) OVER w_before AS prev_ok_bikes,
        LAST_VALUE(CASE WHEN data_status = 'ok' THEN hour_ts END, TRUE)          OVER w_before AS prev_ok_ts,
        FIRST_VALUE(CASE WHEN data_status = 'ok' THEN docked_bikes_raw END, TRUE) OVER w_after AS next_ok_bikes,
        FIRST_VALUE(CASE WHEN data_status = 'ok' THEN hour_ts END, TRUE)          OVER w_after AS next_ok_ts
    FROM city_hour_status
    WINDOW
        w_before AS (PARTITION BY city_id ORDER BY hour_ts ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
        w_after  AS (PARTITION BY city_id ORDER BY hour_ts ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING)
),
city_hour_clean AS (
    SELECT
        *,
        data_status <> 'ok' AS is_imputed,
        CAST(ROUND(CASE
            WHEN data_status = 'ok' THEN docked_bikes_raw
            WHEN prev_ok_bikes IS NOT NULL AND next_ok_bikes IS NOT NULL THEN
                prev_ok_bikes + (next_ok_bikes - prev_ok_bikes)
                    * (UNIX_SECONDS(hour_ts) - UNIX_SECONDS(prev_ok_ts))
                    / (UNIX_SECONDS(next_ok_ts) - UNIX_SECONDS(prev_ok_ts))
            ELSE COALESCE(prev_ok_bikes, next_ok_bikes)
        END) AS INT) AS docked_bikes
    FROM city_hour_neighbors
),
city_hour_out AS (
    SELECT
        *,
        MAX(docked_bikes) OVER (PARTITION BY city_id ORDER BY hour_ts
                                ROWS BETWEEN 12 PRECEDING AND 12 FOLLOWING) AS fleet_baseline_bikes,
        MAX(hour_ts) OVER (PARTITION BY city_id)                           AS last_hour_ts
    FROM city_hour_clean
),
city_hour_out_scaled AS (
    SELECT
        *,
        fleet_baseline_bikes - docked_bikes AS bikes_out_est,
        hour_ts > last_hour_ts - INTERVAL 12 HOURS AS bikes_out_provisional,
        MAX(CASE WHEN hour_ts > last_hour_ts - INTERVAL 168 HOURS THEN fleet_baseline_bikes - docked_bikes END)
            OVER (PARTITION BY city_id) AS bikes_out_peak_recent_7d
    FROM city_hour_out
),
city_hour_local AS (
    SELECT
        *,
        FROM_UTC_TIMESTAMP(hour_ts, 'Europe/Zurich')          AS hour_local_ts,
        TO_DATE(FROM_UTC_TIMESTAMP(hour_ts, 'Europe/Zurich')) AS date_local
    FROM city_hour_out_scaled
)
SELECT
    s.city_id,
    s.city_name,
    s.hour_ts,
    s.hour_local_ts,
    s.date_local,
    CAST(HOUR(s.hour_local_ts) AS INT)                                  AS hour_local,
    DATE_FORMAT(s.hour_local_ts, 'E')                                   AS weekday_local,
    DAYOFWEEK(s.hour_local_ts) IN (1, 7)                                AS is_weekend,
    CAST(s.n_stations_city AS INT)                                      AS n_stations_city,
    CAST(s.n_stations_reporting AS INT)                                 AS n_stations_reporting,
    CAST(s.docked_bikes_raw AS INT)                                     AS docked_bikes_raw,
    s.data_status,
    s.is_imputed,
    s.docked_bikes,
    CAST(s.fleet_baseline_bikes AS INT)                                 AS fleet_baseline_bikes,
    CAST(s.bikes_out_est AS INT)                                        AS bikes_out_est,
    s.bikes_out_provisional,
    CAST(s.bikes_out_peak_recent_7d AS INT)                             AS bikes_out_peak_recent_7d,
    CAST(TRY_DIVIDE(s.bikes_out_est, s.bikes_out_peak_recent_7d) AS DOUBLE) AS bikes_out_share_of_peak,
    CAST(w.temperature_2m AS DOUBLE)                                    AS temperature_c,
    CAST(w_next.precipitation AS DOUBLE)                                AS precipitation_mm,
    CAST(w_next.snowfall AS DOUBLE)                                     AS snowfall_cm,
    CASE
        WHEN w_next.snowfall > 0         THEN 'snow'
        WHEN w_next.precipitation >= 0.1 THEN 'rain'
        ELSE 'none'
    END                                                                 AS precipitation_kind,
    CAST(w.cloud_cover AS DOUBLE)                                       AS cloud_cover_pct,
    CAST(w.weather_code AS INT)                                         AS weather_code,
    w.is_forecast                                                       AS weather_is_forecast,
    sun.sunrise_local,
    sun.sunset_local,
    sun.daylight_hours,
    sun.sun_elevation_deg,
    sun.light_phase
FROM city_hour_local AS s
LEFT JOIN silver_weather AS w                                 -- instant values at the hour start
    ON  w.location_id = s.weather_location_id
    AND w.valid_ts    = s.hour_ts
LEFT JOIN silver_weather AS w_next                            -- sums during the hour
    ON  w_next.location_id = s.weather_location_id
    AND w_next.valid_ts    = s.hour_ts + INTERVAL 1 HOUR
LEFT JOIN silver_sun_location_hour AS sun                     -- sun of the city's weather cell
    ON  sun.location_id = s.weather_location_id
    AND sun.hour_ts     = s.hour_ts;
