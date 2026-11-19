/*
Calendar and astronomy: deterministic attributes of a place and a time, computed once and joined by gold tables.
(Holidays and school vacations from step 3d will also go in this file.)

SUN: NOAA general solar position formulas (accuracy about 1 to 2 minutes), computed for every weather grid cell
(silver_weather_locations: centroid of the cell's stations) and every hour that silver_weather holds for it. The hour
list therefore follows the weather data, including its forecast hours (up to about 3 days ahead), so sun values exist
wherever weather values exist, also for next-day forecasting. Cities use the sun of their weather cell (ref_cities);
within a 0.1 degree cell the difference to the city center is a matter of seconds.

  sun_elevation_deg  elevation at the hour START (consistent with bikes_at_hour_start), negative = below the horizon
  sunrise / sunset   for the local date, standard -0.833 degree horizon (refraction + solar disc)
  light_phase        day (elevation > -0.833), twilight (civil, down to -6), night (below -6)

Local time: Europe/Zurich, stored as TIMESTAMP holding the wall-clock time. Assumes the pipeline session time zone is
UTC (verified 19 Nov 2026: Zurich sunrise 19 Nov computed 07:35:22, published 07:35).
*/

CREATE OR REFRESH MATERIALIZED VIEW silver_sun_location_hour (
  location_id        STRING    COMMENT 'Weather grid cell, e.g. 47.4_8.5 (see silver_weather_locations)',
  hour_ts            TIMESTAMP COMMENT 'Hour start, UTC',
  date_local         DATE      COMMENT 'Local date (Europe/Zurich) of the hour',
  sun_elevation_deg  DOUBLE    COMMENT 'Sun elevation above the horizon at the hour start, degrees (negative = below)',
  light_phase        STRING    COMMENT 'day (elevation > -0.833), twilight (down to -6) or night, at the hour start',
  sunrise_local      TIMESTAMP COMMENT 'Sunrise on date_local, Europe/Zurich wall-clock time',
  sunset_local       TIMESTAMP COMMENT 'Sunset on date_local, Europe/Zurich wall-clock time',
  daylight_hours     DOUBLE    COMMENT 'Hours between sunrise and sunset on date_local'
)
CLUSTER BY (location_id, hour_ts)
COMMENT 'Sun position, light phase, sunrise and sunset per weather grid cell and UTC hour (NOAA formulas)'
AS
WITH location_hours AS (
    SELECT DISTINCT
        w.location_id,
        w.valid_ts                                                  AS hour_ts,
        CAST(l.lat AS DOUBLE)                                       AS lat,
        CAST(l.lon AS DOUBLE)                                       AS lon,
        TO_DATE(FROM_UTC_TIMESTAMP(w.valid_ts, 'Europe/Zurich'))    AS date_local
    FROM silver_weather AS w
    JOIN silver_weather_locations AS l
        ON w.location_id = l.location_id
),
fractional_year AS (                                          -- NOAA angle (radians) for the hour and for local noon
    SELECT
        *,
        2 * PI() / 365 * (DAYOFYEAR(hour_ts) - 1 + (HOUR(hour_ts) - 12) / 24.0)  AS g_hour,
        2 * PI() / 365 * (DAYOFYEAR(date_local) - 1)                             AS g_noon
    FROM location_hours
),
sun_terms AS (                                                -- equation of time (minutes) and declination (radians)
    SELECT
        *,
        229.18 * (0.000075 + 0.001868 * COS(g_hour) - 0.032077 * SIN(g_hour)
                  - 0.014615 * COS(2 * g_hour) - 0.040849 * SIN(2 * g_hour))          AS eqtime_hour,
        0.006918 - 0.399912 * COS(g_hour) + 0.070257 * SIN(g_hour) - 0.006758 * COS(2 * g_hour)
            + 0.000907 * SIN(2 * g_hour) - 0.002697 * COS(3 * g_hour) + 0.00148 * SIN(3 * g_hour) AS decl_hour,
        229.18 * (0.000075 + 0.001868 * COS(g_noon) - 0.032077 * SIN(g_noon)
                  - 0.014615 * COS(2 * g_noon) - 0.040849 * SIN(2 * g_noon))          AS eqtime_noon,
        0.006918 - 0.399912 * COS(g_noon) + 0.070257 * SIN(g_noon) - 0.006758 * COS(2 * g_noon)
            + 0.000907 * SIN(2 * g_noon) - 0.002697 * COS(3 * g_noon) + 0.00148 * SIN(3 * g_noon) AS decl_noon
    FROM fractional_year
),
sun AS (
    SELECT
        *,
        -- hour angle from true solar time in minutes: UTC minutes + equation of time + 4 min per degree of longitude
        DEGREES(ASIN(
              SIN(RADIANS(lat)) * SIN(decl_hour)
            + COS(RADIANS(lat)) * COS(decl_hour)
              * COS(RADIANS((HOUR(hour_ts) * 60 + eqtime_hour + 4 * lon) / 4 - 180))
        )) AS elevation_deg,
        -- sunrise/sunset hour angle (degrees) for the -0.833 degree horizon
        DEGREES(ACOS(
              COS(RADIANS(90.833)) / (COS(RADIANS(lat)) * COS(decl_noon))
            - TAN(RADIANS(lat)) * TAN(decl_noon)
        )) AS ha_sunrise_deg
    FROM sun_terms
)
SELECT
    location_id,
    hour_ts,
    date_local,
    CAST(ROUND(elevation_deg, 2) AS DOUBLE)                           AS sun_elevation_deg,
    CASE
        WHEN elevation_deg > -0.833 THEN 'day'
        WHEN elevation_deg > -6     THEN 'twilight'
        ELSE 'night'
    END                                                               AS light_phase,
    FROM_UTC_TIMESTAMP(TIMESTAMPADD(SECOND,                          -- minutes after 00:00 UTC of the local date
        CAST(ROUND((720 - 4 * (lon + ha_sunrise_deg) - eqtime_noon) * 60) AS INT),
        CAST(date_local AS TIMESTAMP)), 'Europe/Zurich')              AS sunrise_local,
    FROM_UTC_TIMESTAMP(TIMESTAMPADD(SECOND,
        CAST(ROUND((720 - 4 * (lon - ha_sunrise_deg) - eqtime_noon) * 60) AS INT),
        CAST(date_local AS TIMESTAMP)), 'Europe/Zurich')              AS sunset_local,
    CAST(8 * ha_sunrise_deg / 60 AS DOUBLE)                           AS daylight_hours   -- 2 x 4 min per degree
FROM sun;
