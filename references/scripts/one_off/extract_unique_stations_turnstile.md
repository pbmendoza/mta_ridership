# extract_unique_stations_turnstile.py

## Purpose

`scripts/one_off/extract_unique_stations_turnstile.py` builds a station-to-turnstile
reference mapping from historical combined turnstile logs.

It reads `data/local/staging/turnstile/turnstile_combined.csv`, extracts unique
`STATION`/`LINENAME` combinations, and writes the result to
`references/stations/stations_turnstile.csv`.

## Why this exists

This was used to generate a station metadata reference file from turnstile data.

## Current status

The pipeline no longer uses turnstile data to create the baseline, so this script is
now treated as a one-off utility and is not part of the active pipeline.

## Run command

- From repo root:
  `python scripts/one_off/extract_unique_stations_turnstile.py`
- From `scripts/one_off`:
  `python extract_unique_stations_turnstile.py`
