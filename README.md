# bikeshare-databricks

Forecasting shared-bike availability at Swiss stations from time of day, seasonality and weather, built on Databricks.

This is a learning project for Databricks data engineering: Lakeflow Jobs, Spark Declarative Pipelines, the medallion architecture, Unity Catalog, Git folders and Declarative Automation Bundles (formerly Databricks Asset Bundles). It runs on Databricks Free Edition (serverless).

## Goal

1. **Collect** station-level bike availability snapshots and matching weather observations on a schedule.
2. **Model** the data in bronze / silver / gold layers, with data-quality expectations.
3. **Forecast** available bikes per station from:
   - time of day and day of week
   - seasonality (month, holidays, school vacations)
   - weather (temperature, precipitation, wind)

## Data sources

| Source | What | Format |
|---|---|---|
| [sharedmobility.ch](https://sharedmobility.ch) | Swiss shared-mobility feeds: station information and station status (bikes and docks available) | GBFS (JSON) |
| Weather API (to be chosen: MeteoSwiss open data or Open-Meteo) | Hourly observations near the stations | JSON / CSV |

## Architecture

```
GBFS feed ──┐
            ├─► Lakeflow Job (scheduled ingestion) ─► UC Volume (raw JSON)
Weather  ───┘                                            │
                                                         ▼
                              Spark Declarative Pipeline (Auto Loader)
                              bronze  → raw snapshots, as ingested
                              silver  → cleaned, typed, deduplicated, with expectations
                              gold    → station × hour availability joined with weather
                                                         │
                                                         ▼
                                       Forecasting (planned)
```

## Repository layout

```
bikeshare-databricks/
├─ databricks.yml        # bundle config: targets (dev / prod), variables
├─ resources/            # job and pipeline definitions (YAML)
├─ src/
│  ├─ ingestion/         # notebook that pulls the GBFS feeds (weather planned)
│  ├─ pipeline/
│  │  ├─ transformations/  # pipeline code: bronze and silver (gold planned)
│  │  └─ explorations/     # reusable data-check queries, not run by the pipeline
│  └─ forecasting/       # planned
├─ docs/                 # project context and working notes
└─ README.md
```

## Status

- [x] GBFS ingestion notebook and scheduled Lakeflow Jobs
- [x] Spark Declarative Pipeline: bronze and silver tables with expectations, SCD type 2 station history
- [x] Code in a Git folder (this repo)
- [x] Jobs and pipeline defined as a bundle with `dev` and `prod` targets
- [x] Running in prod on a schedule, with failure alerts
- [ ] Weather ingestion
- [ ] Gold table: station × hour availability + weather
- [ ] Forecasting model and evaluation
- [ ] CI: `databricks bundle validate` on every pull request (GitHub Actions)

## Running it

Requires the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/) and access to a Databricks workspace.

```bash
databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run      -t dev <job_key>
```

The `dev` target deploys in development mode: resource names get a `[dev <user>]` prefix and schedules are paused.