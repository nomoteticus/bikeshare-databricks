# Data pipeline: from feed to metrics

This page describes how the data gets from the public feeds into the tables and how the metrics on the
daily animation page are computed. The SQL files under `src/pipeline/transformations/` are the reference;
each one starts with a header comment that states its rules.

```mermaid
flowchart LR
    gbfs["sharedmobility.ch<br/>GBFS feeds"]
    meteo["Open-Meteo<br/>weather API"]
    jobs["Lakeflow Jobs<br/>(Python notebooks)"]
    raw[("UC volume<br/>raw JSON files")]

    subgraph sdp["Spark Declarative Pipeline (Auto Loader, SQL)"]
        direction LR
        bronze["bronze_*<br/>raw rows"]
        silver["silver_*<br/>typed, validated, history"]
        gold["gold_*<br/>station × hour<br/>city × hour"]
        bronze --> silver --> gold
    end

    export["Export job"]
    json[("JSON files")]
    page["Static page<br/>daily animation"]
    ml["Forecasting<br/>(planned)"]

    gbfs --> jobs
    meteo --> jobs
    jobs --> raw --> bronze
    gold --> export --> json --> page
    gold -.-> ml
```

All timestamps are stored in UTC. Local time (Europe/Zurich) appears only in columns whose names say so
(`*_local`), for display and for local-time features.

## 1. Sources

| Source | Content | Terms |
|---|---|---|
| [sharedmobility.ch](https://sharedmobility.ch), Swiss Federal Office of Energy (SFOE) | Real-time station status and station information for shared mobility in Switzerland, GBFS 2.0 (`https://sharedmobility.ch/gbfs.json`); per-system GBFS 2.3 feeds as a fallback | Open use, commercial use allowed, source must be provided ([dataset on opendata.swiss](https://opendata.swiss/en/dataset/standorte-und-verfugbarkeit-von-shared-mobility-angeboten)) |
| [Open-Meteo](https://open-meteo.com) | Hourly weather from the MeteoSwiss ICON-CH1/CH2 models (`meteoswiss_icon_seamless`) | [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/); values here are aggregated and joined to bike data |

The feed is real-time only: there is no history to download, so every missed pull is a lost snapshot.

## 2. Ingestion (Lakeflow Jobs)

| Job | Schedule (Europe/Zurich) | What it does |
|---|---|---|
| `bikeshare_pull_status` | every 30 min | Downloads `station_status` and writes the JSON file unchanged to `/Volumes/<catalog>/bikeshare/raw/station_status/` |
| `bikeshare_pull_reference` | daily 04:15 | Downloads `station_information`, `providers`, `system_regions` |
| `bikeshare_pull_weather` | daily 05:20 | One Open-Meteo request for all weather grid cells, window from 2 days before to 2 days after the run date; one JSON Lines file per run |
| `bikeshare_refresh` | hourly at :10 | Runs the pipeline `bikeshare_stations` (all tables below) |
| `bikeshare_export_daily_animation` | daily 06:40 | Writes the JSON files for the animation page |

Details worth knowing:

- **Status fallback.** The combined GBFS 2.0 file (about 2.5 MB, all 46 providers) is sometimes cut off by the
  server mid-download. When three attempts fail, `pull_gbfs.ipynb` downloads the eight bike systems from the
  per-system GBFS 2.3 feeds instead and writes one file in the same shape as the 2.0 file (same eight station
  fields, `station_id` as `<system>:<id>`), named `..._v2.json`. The pipeline treats both the same way.
  Each download attempt has a wall-clock limit, so a slow server cannot block the fallback.
- **Weather grid.** Stations are grouped into 0.1° cells (about 11 × 7.6 km); 110 cells contain at least one
  bike station. The weather is requested at the centroid of the cell's stations, not at the cell center,
  so mountain cells are not given summit weather for valley stations.
- **Backfills.** The weather notebook takes a `run_date` parameter, so the Jobs "Run backfill" feature can
  replay past days (using Open-Meteo's Historical Forecast API when the whole window is in the past).

## 3. Bronze: raw rows

Streaming tables fed by Auto Loader (`read_files`), each new file read once.

| Table | Grain | Notes |
|---|---|---|
| `bronze_station_status` | station × file | Exploded `data.stations`, plus feed `last_updated`, `ttl`, stations in file, file name |
| `bronze_station_information`, `bronze_providers`, `bronze_system_regions` | element × file | Same pattern |
| `bronze_weather` | grid cell × hour × file | Hourly arrays exploded by position; explicit schema, so the pipeline also starts on an empty folder. Keeps every pull, i.e. the full history of forecasts |

No type or time-zone conversions happen in bronze.

## 4. Silver: typed, validated, with history

| Table | Grain | Logic |
|---|---|---|
| `silver_station_status` | station × snapshot | `snapshot_ts` = feed `last_updated` (UTC); `last_reported_ts`, `hours_since_reported`, `is_in_service` (installed, renting and returning). Rows with missing keys or negative bike counts are dropped; implausible values are counted but kept (warnings) |
| `silver_station_information` | station × version | AUTO CDC, **SCD type 2**: a new version only when name, position, capacity or region changes |
| `silver_providers`, `silver_system_regions` | provider / region | AUTO CDC, SCD type 1 (latest values) |
| `silver_station_weather_location`, `silver_weather_locations` | station / grid cell | Station-to-cell mapping and the list of cells the weather job asks for |
| `silver_weather` | grid cell × hour | AUTO CDC keyed on (cell, hour), sequenced by pull time: **the latest pull wins**. Past hours hold after-the-fact model values; hours after the latest pull hold forecasts (`is_forecast`) |
| `silver_station_city` | station | Nearest city of `ref_cities` whose radius contains the station (haversine distance) |
| `silver_sun_location_hour` | grid cell × hour | Sun elevation, light phase (day / twilight / night), sunrise, sunset, daylight hours from the NOAA solar formulas |

Hand-maintained reference tables (`reference.sql`):

- `ref_bike_systems`: the eight docked-bike systems in scope (Velospot, nextbike ×2, Donkey Republic ×5),
  with how complete their `capacity` is. The feed also carries cars, scooters and mopeds, and its own
  `vehicle_type` field is not reliable, so this list is the single filter for "bikes".
- `ref_cities`: 13 cities with 45 or more docked-bike stations (center, radius, canton, weather cell, and
  whether the animation shows the city).
- `ref_data_incidents`: confirmed feed incidents per provider (see section 7).

## 5. Gold tables

### `gold_station_hour`: one row per bike station and UTC hour

Aggregates the snapshots of each hour (normally two, at :00 and :30):

| Column | Meaning |
|---|---|
| `bikes_at_hour_start` | Bikes at the first snapshot of the hour (main candidate target) |
| `bikes_avg`, `bikes_min`, `bikes_max` | Over the hour's snapshots |
| `docks_avg` | Free docks reported (not meaningful for every provider) |
| `n_snapshots` | Snapshots in the hour (1 when a pull was lost) |
| `all_in_service` | Station in service in every snapshot of the hour |

### `gold_station_hour_features`: the station-hour table for modeling

`gold_station_hour` plus:

- **Station attributes valid at that hour** (point-in-time join to the SCD type 2 history): name, position,
  capacity, region, `fill_rate` = bikes / capacity capped at 1 (empty where capacity is unknown).
- **Weather of the station's grid cell.** Instant values at the hour start (temperature, humidity, cloud
  cover, wind, weather code) and hourly sums twice: for the hour before (`*_prev_hour`) and during the
  hour (`*_in_hour`). `weather_is_forecast` marks hours still holding forecast values.
- **City and sun:** `city_id`, distance to the city center, sun elevation, light phase, sunrise and sunset.

Planned: calendar features (public holidays, school vacations) and lagged values for day-ahead forecasting.

### `gold_city_hour`: one row per city and UTC hour

Built on a complete hour spine, so an hour without data still has a row. This table is the semantic layer
of the animation: the page only draws what is here. All rules are in the header of `city.sql`.

| Column | Meaning |
|---|---|
| `docked_bikes_raw` | Sum of `bikes_at_hour_start` over the city's stations that reported |
| `data_status` | `ok`, `missing`, `partial`, `glitch_known` or `glitch_suspected` (section 7) |
| `docked_bikes` | Raw value when `ok`; otherwise linear interpolation between the nearest `ok` hours (`is_imputed`) |
| `fleet_baseline_bikes` | Most bikes docked within ±12 hours |
| `bikes_out_est` | `fleet_baseline_bikes − docked_bikes` |
| `bikes_out_peak_recent_7d`, `bikes_out_share_of_peak` | Highest `bikes_out_est` of the last 7 days and the hour's share of it |
| `share_stations_active` | Share of stations whose count changed during the hour (section 6) |
| weather, sun | From the city's weather cell |

## 6. Metrics on the animation page

**Bikes docked.** `docked_bikes`: bikes standing in a dock at the start of the hour, summed over the city's
stations, with gaps filled as described above.

**Bikes out (estimate).** The feed has no trips, only how many bikes stand at each station. A bike that is
not docked is in use, in the workshop or moved by the operator. The page therefore estimates bikes in use
as the drop from the city's recent maximum:

```
bikes_out_est = max(docked_bikes within ±12 h) − docked_bikes
```

The rolling ±12-hour maximum (instead of a fixed fleet size) absorbs slow changes in the fleet, such as
bikes going to repair. It uses later hours, so it is a display metric, not a forecasting feature, and the
last 12 hours are provisional (`bikes_out_provisional`): their baseline can still rise.

**The bikes on the street.** The scene draws 0 to 9 bikes: `9 × bikes_out_share_of_peak`, i.e. 9 bikes =
the city's busiest hour of the last 7 days. Cities are therefore comparable in rhythm, not in size.

**Stations with activity (%).** A station counts as active in an hour when its bike count at the start of
the next hour differs from the count at the start of this hour. `share_stations_active` = active stations /
stations compared. It counts stations with a pickup or return, not trips: a pickup and a return at the same
station within the hour cancel out. It is computed only when this and the next hour are `ok` and at least 90%
of the city's stations can be compared; other hours are interpolated, and the latest hour is empty because
its next hour does not exist yet.

**Weather and light.** Temperature at the hour start, precipitation during the hour (`snow` when snowfall
> 0, `rain` from 0.1 mm), and sun elevation for the day/night cycle. A "forecast" label marks hours whose
weather is still a forecast.

**Grey bands.** Hours whose bike values were filled in (`data_status` not `ok`).

## 7. Data quality

**Expectations** in the pipeline drop rows with missing keys or impossible values and count warnings for
implausible ones (for example more than 200 bikes, reports older than 30 days, temperatures outside −40 to 45 °C).

**`data_status`** in `gold_city_hour` (first matching rule wins):

| Status | Rule |
|---|---|
| `glitch_known` | The hour falls in a confirmed incident (`ref_data_incidents`) of a provider present in the city |
| `glitch_suspected` | Automatic: the provider's national total is below 80% of its ±12-hour maximum **and** at least 200 bikes below it. The 80% separates feed faults from commuting (normal dips are a few percent in large fleets); the 200-bike floor avoids false alarms in small one-city fleets |
| `missing` | No snapshot for the city in that hour |
| `partial` | Fewer than 90% of the city's stations reported |
| `ok` | None of the above |

**Known incident.** On 18 Nov 2026, from about 08:00 to noon Zurich time, Velospot reported about 37% fewer
bikes nationwide while its stations kept reporting. The cause is unknown; the hours are flagged
`glitch_known` and interpolated. The data is shown as received from the feed.

**Which cities the animation shows.** Some providers report a station only when something changes, sometimes
hours later. Where that dominates, hourly totals hide most movement. A city is left out of the animation (it
stays in `gold_city_hour`) when its stations change count in fewer than 10% of station-hours or the median
time since a station's last report is above 9 hours. This currently leaves out Basel, Locarno and Sion; the
page footer names them with the reason. The check is in `src/pipeline/explorations/city_data_quality.sql`.

**Sanity check before publication (22 Nov 2026, prod, data from 12 Nov 23:00 UTC):**

- 472 of 476 half-hourly snapshots received. The four lost ones are from 16 and 17 Nov: three during a server
  outage before the fallback existed (including one complete hour, 16 Nov 09:00 to 10:00 Zurich time) and one
  timed-out download that led to the per-attempt time limit.
- Station tables: no duplicate keys; no violations of min ≤ average ≤ max or negative counts; no missing
  weather; 2,970 stations every hour of the last 7 days.
- City table: no `glitch_suspected` or `partial` hours; the incident hours flagged as expected.
- The exported JSON matched `gold_city_hour` for all shown cities with no differences.

## 8. Limitations

- **Stock, not trips.** The feed shows bikes per station. Trips, rebalancing by the operator and repairs all
  look the same: a change in a count. "Bikes out" and "activity" are estimates built on that.
- **Late reports.** Velospot and Donkey stations report on events, often hours late (median a few hours).
  Activity then appears in the hour the report arrives. Part of the night-time activity in some cities is this effect.
- **Capacity.** Not reported for `nextbike_ch` and only partly for `nextbike_ms`; `fill_rate` is empty there.
  Some stations report more bikes than their capacity; the fill rate is capped at 1.
- **Model weather.** Weather values come from a weather model on a 0.1° grid, not from station measurements.
- **Short history.** Collection started on 12 Nov 2026. Weekly and seasonal patterns, holidays and weather
  effects need weeks to months of data before they can be estimated reliably.
