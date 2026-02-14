# Monthly Ridership Update Pipeline

## What it does

This pipeline refreshes our subway ridership dataset with the latest monthly data from the MTA. It pulls new ridership numbers from the NYC Open Data API, rolls them up to different geographic levels, compares them against historical baselines, and produces the final analysis-ready files.

When it finishes, the files in `data/production/` are up to date and ready for use in dashboards and reports.

## What it produces

| File | Description |
|------|-------------|
| `monthly_ridership_station.csv` | Ridership per station complex, with station names, baseline comparison, and OMNY adoption rate |
| `monthly_ridership_puma.csv` | Ridership aggregated to PUMA (neighborhood) level, with PUMA names |
| `monthly_ridership_nyc.csv` | City-wide ridership totals |

Each file breaks down ridership by **weekday**, **weekend**, and **total** for every month.

## How to run it

Open and run `pipelines/monthly_ridership_update.py`. By default it only fetches months that are not already in the data — so a typical monthly update takes just a few minutes.

### Full refresh option

If you need to rebuild everything from scratch (e.g. after a data correction from the MTA), open the pipeline file and change the setting near the top:

```
FULL_REFRESH = True
```

This tells the first step to re-download all months instead of only new ones. Set it back to `False` after running.

## Pipeline steps (in order)

1. **Download station ridership** — Pulls raw ridership data from the NYC Open Data API for each month, splits it into weekday/weekend, calculates OMNY adoption percentages, and saves station-level results.

2. **Aggregate to PUMA & NYC** — Rolls up station numbers to the PUMA (neighborhood) and city-wide levels.

3. **Merge with baselines** — Joins current ridership with pre-pandemic baseline values and calculates a baseline comparison ratio (e.g. 0.85 means ridership is at 85% of baseline).

4. **Enrich & sort** — Adds human-readable names (station names, PUMA/neighborhood names) and sorts the final output files for consistent ordering.

## When to run

Run this pipeline once a month after the MTA publishes new ridership data on NYC Open Data (typically within the first two weeks of the following month).
