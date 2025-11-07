# 🚇 MTA Hourly Ridership Data Documentation

## 📋 Overview

The MTA Hourly Ridership datasets provide estimated entries at the subway station complex level, broken down by hour and fare payment category. These modern datasets supersede legacy turnstile files for current analysis and are suitable for post-2020 ridership monitoring.

**Status**: ✅ Active

## 🗂️ Data Location (repo)

- Raw files: `data/raw/ridership/`
  - `ridership_2020_2024.csv`
  - `ridership_2025.csv`
- Provenance: `data/raw/ridership/metadata.yaml`

## 🔗 Data Sources (NY Open Data)

- 2020–2024: `wujg-7c2s` — MTA Subway Hourly Ridership 2020–2024  
  Source: https://data.ny.gov/Transportation/MTA-Subway-Hourly-Ridership-2020-2024/wujg-7c2s/
- 2025+: `5wq4-mkjj` — MTA Subway Hourly Ridership Beginning 2025  
  Source: https://data.ny.gov/Transportation/MTA-Subway-Hourly-Ridership-Beginning-2025/5wq4-mkjj/

## 🧾 Coverage and Update Cadence (from metadata)

| Dataset | Last updated | Retrieved | Source |
|---|---|---|---|
| 2020–2024 (wujg-7c2s) | 2025-05-14 | 2025-06-10 | https://data.ny.gov/Transportation/MTA-Subway-Hourly-Ridership-2020-2024/wujg-7c2s/ |
| 2025+ (5wq4-mkjj) | 2025-10-22 | 2025-10-29 | https://data.ny.gov/Transportation/MTA-Subway-Hourly-Ridership-Beginning-2025/5wq4-mkjj/ |

Values reflect the repository’s `metadata.yaml` and may lag subsequent portal updates.

## 🧱 Key Schema Notes

- Unit of analysis: station complex (not individual stations)  
  See cross-references for station complex details.
- Time granularity: hourly; timestamps are in local Eastern Time (check dataset “Columns” for timezone specifics).
- Measure: hourly estimated entries (sometimes labeled “ridership”). Exits are not provided.
- Fare payment category: entries are segmented by payment class (e.g., MetroCard, OMNY, reduced, etc.); exact labels can evolve—verify on the dataset “Columns” tab.

For the authoritative field list and definitions, consult each dataset’s Columns section on NY Open Data.

## 🔄 Differences: 2020–2024 vs 2025+

- Dataset split: the series continues under a new identifier in 2025 (`5wq4-mkjj`).
- Concept and structure are intended to be consistent; minor field name or label changes may occur—always verify the Columns table on the portal when building joins.

## ⚠️ Caveats and Notes

- Complex-level aggregation: multi-station complexes aggregate all connected platforms; do not analyze at individual stop level.
- Daylight saving transitions: treat timestamps as local; expect repeated or skipped hours around DST changes.
- Backfills/corrections: the portal may revise historical rows; use `last_updated` in `metadata.yaml` and consider periodic refreshes.
- Comparability with legacy turnstiles: modern estimates differ methodologically from raw turnstile counters; prefer modern datasets for 2020+ analysis.

## 🧭 Cross-References

- `references/docs/subway_stations.md`
- `references/docs/turnstile_data.md`
- `references/docs/PIPELINE.md`
- `references/docs/scp_identifier.md`

## 📝 Citation and Licensing

- Cite NY Open Data dataset titles and identifiers: `wujg-7c2s` and `5wq4-mkjj`. Link to the dataset pages above.
- Respect the licensing/terms on the dataset pages; this repository mirrors metadata for provenance only.


