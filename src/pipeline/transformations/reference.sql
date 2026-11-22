-- Docked bike-share systems used in this project (the feed also contains cars, mopeds, scooters)
-- operator_name: short brand for display (the feed's own names are city-specific, e.g. "Donkey Republic Geneva")
CREATE OR REFRESH MATERIALIZED VIEW ref_bike_systems
COMMENT "Docked bike-share systems in scope; single definition reused by weather, gold, export and ML"
AS
SELECT *
FROM VALUES
  ('velospot',                  'full',    'Velospot'),
  ('donkey_ge',                 'full',    'Donkey Republic'),
  ('donkey_kreuzlingen',        'full',    'Donkey Republic'),
  ('donkey_neuchatel',          'full',    'Donkey Republic'),
  ('donkey_thun',               'full',    'Donkey Republic'),
  ('donkey_yverdon-les-bains',  'full',    'Donkey Republic'),
  ('nextbike_ms',               'partial', 'invia BikeShare'),
  ('nextbike_ch',               'none',    'nextbike')
AS t(provider_id, capacity_coverage, operator_name);   -- capacity_coverage: 'full' / 'partial' / 'none'

/*
Cities for city-level aggregates (gold_city_hour) and the daily animation.
Rule for being listed: 45+ current docked-bike stations within radius_km of the center (threshold set 19 Nov 2026,
lowered from 50 to keep Chur at 48; others below 45 then: Bellinzona about 42, Sursee 41, Thun 40).
Radius rule: radius_km covers the city and the directly adjoining places of the same operator network
(e.g. Bern with Koeniz and Ittigen); chosen by hand per city from the station positions, not one global value.
Station-to-city rule (in city.sql): a station belongs to the NEAREST listed city whose radius it falls in;
stations outside every radius belong to no city.
canton = canton of the main municipality (stations of an agglomeration can lie in a neighboring canton).
weather_location_id = the weather grid cell (see silver_weather_locations) used for the whole city.
in_daily_animation / animation_note: whether the daily animation shows the city (all cities stay in gold_city_hour).
Rule (22 Nov 2026, measured 13 to 19 Nov): leave a city out when its station counts are too sparse for an hourly pattern,
i.e. stations change count in less than 10% of station-hours OR the median time since a station's last report is above
9 h. Out: Basel (8%, 15.7 h), Locarno (8%, 16.7 h), Sion (11%, 9.8 h). Next in line, kept: Geneve (12%, 7.3 h).
Reference: Zurich 43%, 1.3 h. The report-age part only applies to providers that report on events (Velospot, Donkey);
nextbike reports on every pull, so its median is always about 0.
Recheck with the query in src/pipeline/explorations when the data has more weeks.
*/
CREATE OR REFRESH MATERIALIZED VIEW ref_cities
COMMENT "Cities with 45+ docked-bike stations: center, radius for the station-to-city mapping, canton, weather cell, daily-animation flag"
AS
SELECT
  city_id,
  city_name,
  canton,
  CAST(center_lat AS DOUBLE) AS center_lat,
  CAST(center_lon AS DOUBLE) AS center_lon,
  CAST(radius_km  AS DOUBLE) AS radius_km,
  weather_location_id,
  in_daily_animation,
  animation_note
FROM VALUES
  ('geneve',    'Genève',                 'GE', 46.204, 6.143,  7.0, '46.2_6.1', TRUE,  NULL),
  ('bern',      'Bern',                   'BE', 46.948, 7.447, 10.0, '46.9_7.4', TRUE,  NULL),
  ('basel',     'Basel',                  'BS', 47.557, 7.590,  5.0, '47.6_7.6', FALSE, 'Stations report rarely (median 15.7 h since last report, counts change in 8% of station-hours), so the daily pattern is not visible'),
  ('zurich',    'Zürich',                 'ZH', 47.378, 8.540,  6.0, '47.4_8.5', TRUE,  NULL),
  ('luzern',    'Luzern',                 'LU', 47.050, 8.309,  7.0, '47.0_8.3', TRUE,  NULL),
  ('locarno',   'Locarno',                'TI', 46.170, 8.795,  8.0, '46.2_8.8', FALSE, 'Stations report rarely (median 16.7 h since last report, counts change in 8% of station-hours), so the daily pattern is not visible'),
  ('sion',      'Sion',                   'VS', 46.233, 7.360,  6.0, '46.2_7.4', FALSE, 'Stations report rarely (median 9.8 h since last report, counts change in 11% of station-hours), so the daily pattern is barely visible'),   -- Sion + Saviese; Grone (13 km) left out
  ('yverdon',   'Yverdon-les-Bains',      'VD', 46.778, 6.641,  4.0, '46.8_6.6', TRUE,  NULL),
  ('lugano',    'Lugano',                 'TI', 46.005, 8.950,  6.0, '46.0_9.0', TRUE,  NULL),
  ('fribourg',  'Fribourg',               'FR', 46.802, 7.152,  5.0, '46.8_7.1', TRUE,  NULL),
  ('biel',      'Biel/Bienne',            'BE', 47.137, 7.250,  5.0, '47.1_7.3', TRUE,  NULL),
  ('neuchatel', 'Neuchâtel',              'NE', 46.992, 6.931,  6.0, '47.0_6.9', TRUE,  NULL),
  ('chur',      'Chur',                   'GR', 46.853, 9.529,  6.0, '46.9_9.5', TRUE,  NULL)    -- 48 stations (19 Nov 2026)
AS t(city_id, city_name, canton, center_lat, center_lon, radius_km, weather_location_id, in_daily_animation, animation_note);


/*
Confirmed data incidents, entered by hand after investigation. Times are UTC, [start_ts, end_ts) (end exclusive),
on the hour grain of gold (hour_ts). gold_city_hour marks every hour of an affected provider as 'glitch_known'.
An automatic rule in city.sql also flags SUSPECTED glitches; confirmed ones are added here.
*/
CREATE OR REFRESH MATERIALIZED VIEW ref_data_incidents
COMMENT "Confirmed data incidents per provider (UTC, end exclusive), used to flag affected hours in gold"
AS
SELECT
  provider_id,
  CAST(start_ts AS TIMESTAMP) AS start_ts,
  CAST(end_ts   AS TIMESTAMP) AS end_ts,
  incident_type,
  note
FROM VALUES
  ('velospot', '2026-11-18 07:00:00', '2026-11-18 12:00:00', 'wrong_counts',
   'Nationwide: about 2,800 of 7,800 bikes (37%) missing 08:00 to 11:59 Zurich time, most cities recovered at 12:00 but Basel (and possibly Bern) only by 12:30, so the 12:00 hour is included (extended 22 Nov 2026); stations kept reporting fresh values; cause unknown (investigated 19 Nov 2026)')
AS t(provider_id, start_ts, end_ts, incident_type, note);
