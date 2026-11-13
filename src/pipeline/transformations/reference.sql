-- Docked bike-share systems used in this project (the feed also contains cars, mopeds, scooters)
CREATE OR REFRESH MATERIALIZED VIEW ref_bike_systems
COMMENT "Docked bike-share systems in scope; single definition reused by weather, gold and ML"
AS
SELECT *
FROM VALUES
  ('velospot',                  'full'),
  ('donkey_ge',                 'full'),
  ('donkey_kreuzlingen',        'full'),
  ('donkey_neuchatel',          'full'),
  ('donkey_thun',               'full'),
  ('donkey_yverdon-les-bains',  'full'),
  ('nextbike_ms',               'partial'),
  ('nextbike_ch',               'none')
AS t(provider_id, capacity_coverage);   -- 'full' / 'partial' / 'none'