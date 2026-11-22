/*
City data quality for the daily animation (rule in reference.sql, ref_cities.in_daily_animation).
Per city over one week: share of station-hours whose bike count changed, never-changing and always-empty stations,
median hours since a station's last report, and the 7-day bikes-out peak relative to docked bikes.
Leave a city out of the animation when avg_share_hours_changed < 0.10 OR median_hours_since_reported > 9.
Compare median_hours_since_reported only within a provider: nextbike reports on every pull (always about 0).
Change the two timestamps to the week you want to check (UTC).
*/
WITH st AS (
    SELECT
        f.city_id, f.provider_id, f.station_id, f.hour_ts, f.bikes_at_hour_start,
        f.bikes_at_hour_start <> LAG(f.bikes_at_hour_start) OVER (PARTITION BY f.provider_id, f.station_id ORDER BY f.hour_ts) AS changed
    FROM prod.bikeshare.gold_station_hour_features AS f
    WHERE f.city_id IS NOT NULL
      AND f.hour_ts >= TIMESTAMP '2026-11-12 23:00:00' AND f.hour_ts < TIMESTAMP '2026-11-19 23:00:00'
),
per_station AS (
    SELECT city_id, provider_id, station_id,
           AVG(CASE WHEN changed THEN 1.0 ELSE 0 END) AS share_hours_changed,
           MAX(bikes_at_hour_start)                    AS max_bikes
    FROM st GROUP BY ALL
),
fresh AS (
    SELECT c.city_id, PERCENTILE(s.hours_since_reported, 0.5) AS median_hours_since_reported
    FROM prod.bikeshare.silver_station_status AS s
    JOIN prod.bikeshare.silver_station_city AS c ON s.provider_id = c.provider_id AND s.station_id = c.station_id
    WHERE s.snapshot_ts >= TIMESTAMP '2026-11-12 23:00:00' AND s.snapshot_ts < TIMESTAMP '2026-11-19 23:00:00'
    GROUP BY c.city_id
),
city AS (
    SELECT city_id, MAX(bikes_out_peak_recent_7d) AS peak_out, AVG(docked_bikes) AS avg_docked
    FROM prod.bikeshare.gold_city_hour
    GROUP BY city_id
)
SELECT
    p.city_id,
    ANY_VALUE(p.provider_id)                                     AS provider,
    COUNT(*)                                                     AS n_stations,
    ROUND(AVG(p.share_hours_changed), 3)                         AS avg_share_hours_changed,
    COUNT_IF(p.share_hours_changed = 0)                          AS n_never_changed,
    COUNT_IF(p.max_bikes = 0)                                    AS n_always_empty,
    ROUND(MAX(f.median_hours_since_reported), 1)                 AS median_hours_since_reported,
    MAX(c.peak_out)                                              AS peak_out_7d,
    ROUND(100 * MAX(c.peak_out) / MAX(c.avg_docked), 1)          AS peak_out_pct_of_docked
FROM per_station AS p
JOIN fresh AS f ON p.city_id = f.city_id
JOIN city AS c ON p.city_id = c.city_id
GROUP BY p.city_id
ORDER BY avg_share_hours_changed;
