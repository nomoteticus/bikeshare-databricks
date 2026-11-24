"""Download the latest daily-animation export from the Unity Catalog volume into the site folder.

Used by .github/workflows/pages.yml before the site is uploaded to GitHub Pages, so the page shows
the newest export while the daily data never enters Git history. The files committed under
visualizations/daily-animation/data/ are only a fallback snapshot.

What it does:
  1. Downloads cities.json and every <city_id>.json it lists (Databricks Files API,
     GET /api/2.0/fs/files/Volumes/...), into a temporary folder.
  2. Checks the export: it ends on a complete local day (last hour 23:00), that day is recent enough
     (--max-age-days, counted in Europe/Zurich dates), and every city file covers the same window.
  3. Only when everything passed, replaces the JSON files in --out. On any failure it exits with an
     error and changes nothing, so the workflow fails and the previous deployment stays online.

Without DATABRICKS_TOKEN (secret not set yet) it keeps the committed snapshot and exits successfully.

Environment:
  DATABRICKS_HOST   workspace URL, e.g. https://dbc-xxxxxxxx-xxxx.cloud.databricks.com
  DATABRICKS_TOKEN  token allowed to read the exports volume (sent as a Bearer token, never printed)

Local test without Databricks:  --source-dir <folder with an export>  --today 2026-11-23
"""

import argparse
import datetime as dt
import json
import os
import shutil
import sys
import tempfile
import urllib.error
import urllib.parse
import urllib.request
import zoneinfo

DEFAULT_VOLUME_DIR = "/Volumes/prod/bikeshare/exports/daily-animation"
LOCAL_TZ = zoneinfo.ZoneInfo("Europe/Zurich")


class ExportError(Exception):
    """The export could not be downloaded or is not fit to publish."""


def http_reader(host, token, volume_dir):
    """Return read(name) -> bytes for files in the volume folder, via the Files API."""
    host = host.strip().rstrip("/")
    if not host.startswith("https://"):
        host = "https://" + host.split("://")[-1]

    def read(name):
        url = host + "/api/2.0/fs/files" + urllib.parse.quote(volume_dir.rstrip("/") + "/" + name)
        request = urllib.request.Request(url, headers={"Authorization": "Bearer " + token})
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                return response.read()
        except urllib.error.HTTPError as e:
            detail = e.read(300).decode("utf-8", "replace").strip()
            raise ExportError(f"{name}: HTTP {e.code} from the Files API ({detail})") from None
        except (urllib.error.URLError, TimeoutError) as e:
            raise ExportError(f"{name}: could not reach the workspace ({e})") from None

    return read


def dir_reader(source_dir):
    """Return read(name) -> bytes for files in a local folder (tests)."""

    def read(name):
        try:
            with open(os.path.join(source_dir, name), "rb") as f:
                return f.read()
        except OSError as e:
            raise ExportError(f"{name}: {e}") from None

    return read


def parse_json(name, raw):
    try:
        return json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise ExportError(f"{name}: not valid JSON ({e})") from None


def check_index(index, today, max_age_days):
    """Check cities.json: complete last day, recent enough. Returns the end date."""
    for key in ("end_date", "first_hour_local", "last_hour_local", "window_days", "cities"):
        if key not in index:
            raise ExportError(f"cities.json: field '{key}' is missing")
    try:
        end_date = dt.date.fromisoformat(index["end_date"])
    except (TypeError, ValueError):
        raise ExportError(f"cities.json: end_date {index['end_date']!r} is not a date") from None
    if index["last_hour_local"] != f"{end_date.isoformat()}T23:00":
        raise ExportError(f"cities.json: last hour {index['last_hour_local']} is not 23:00 of end_date {end_date}")
    age = (today - end_date).days  # 1 = the export ends yesterday
    if age < 1:
        raise ExportError(f"cities.json: end_date {end_date} is not before today ({today}), the day is not complete")
    if age > max_age_days:
        raise ExportError(f"stale export: data ends {end_date}, {age} days before today ({today}); "
                          f"at most {max_age_days} allowed. Did the export job run?")
    if not index["cities"]:
        raise ExportError("cities.json: no cities listed")
    return end_date


def check_city(name, city, city_id, index):
    """Check one city file against the window in cities.json."""
    hours = city.get("hours") or []
    expected = int(index["window_days"]) * 24
    if city.get("city_id") != city_id:
        raise ExportError(f"{name}: city_id is {city.get('city_id')!r}, expected {city_id!r}")
    if len(hours) != expected:
        raise ExportError(f"{name}: {len(hours)} hours, expected {expected}")
    first, last = hours[0].get("hour_local"), hours[-1].get("hour_local")
    if first != index["first_hour_local"] or last != index["last_hour_local"]:
        raise ExportError(f"{name}: covers {first} to {last}, cities.json says "
                          f"{index['first_hour_local']} to {index['last_hour_local']}")


def fetch(read, out_dir, today, max_age_days):
    """Download, check, then replace the JSON files in out_dir. Returns the index."""
    with tempfile.TemporaryDirectory() as tmp:
        raw = read("cities.json")
        index = parse_json("cities.json", raw)
        check_index(index, today, max_age_days)
        files = {"cities.json": raw}
        for entry in index["cities"]:
            city_id = entry.get("city_id")
            if not city_id or not str(city_id).replace("_", "").replace("-", "").isalnum():
                raise ExportError(f"cities.json: unusable city_id {city_id!r}")
            name = f"{city_id}.json"
            raw = read(name)
            check_city(name, parse_json(name, raw), city_id, index)
            files[name] = raw
        for name, raw in files.items():
            with open(os.path.join(tmp, name), "wb") as f:
                f.write(raw)
        # Everything passed: swap the data files (old JSON out, so a dropped city does not linger)
        os.makedirs(out_dir, exist_ok=True)
        for old in os.listdir(out_dir):
            if old.endswith(".json"):
                os.remove(os.path.join(out_dir, old))
        for name in files:
            shutil.copyfile(os.path.join(tmp, name), os.path.join(out_dir, name))
    return index


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", required=True, help="site data folder, e.g. visualizations/daily-animation/data")
    parser.add_argument("--max-age-days", type=int, default=1,
                        help="newest allowed data age in local days: 1 = must end yesterday (default)")
    parser.add_argument("--volume-dir", default=DEFAULT_VOLUME_DIR, help="export folder in the volume")
    parser.add_argument("--source-dir", help="read the export from a local folder instead of Databricks (tests)")
    parser.add_argument("--today", help="override today's local date, yyyy-MM-dd (tests)")
    args = parser.parse_args()

    today = dt.date.fromisoformat(args.today) if args.today else dt.datetime.now(LOCAL_TZ).date()
    if args.source_dir:
        read = dir_reader(args.source_dir)
    else:
        host, token = os.environ.get("DATABRICKS_HOST", ""), os.environ.get("DATABRICKS_TOKEN", "")
        if not token.strip() or not host.strip():
            print("::warning::DATABRICKS_HOST or DATABRICKS_TOKEN is not set: publishing the committed snapshot.")
            return 0
        read = http_reader(host, token.strip(), args.volume_dir)

    try:
        index = fetch(read, args.out, today, args.max_age_days)
    except ExportError as e:
        print(f"::error::{e}")
        return 1
    message = (f"Data through {index['end_date']} ({index['first_hour_local']} to {index['last_hour_local']}, "
               f"{len(index['cities'])} cities, exported {index.get('generated_at_utc', '?')})")
    print(message)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as f:
            f.write(message + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
