# How To Run The Pipeline

This guide is for non-technical users running the pipeline from VS Code Terminal.

## First Time Setup

### Before You Start

1. Make sure Python 3.10 or newer is installed.
2. Open this repository folder in VS Code.
3. Open the VS Code terminal in the project root.

Check Python:

```bash
python --version
```

If `python` is not recognized and you are on Windows, use:

```powershell
py --version
```

### One-Time Credential Setup (Recommended)

The pipeline can run without a Socrata app token, but it may hit API rate limits more often.

Create `.env` from the template:

```bash
cp .env.example .env
```

Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

Then open `.env` and fill in:

- `SOCRATA_APP_TOKEN`
- `SOCRATA_SECRET_TOKEN` (optional)

If you do not have a token yet, see `references/docs/socrata_api_setup.md`.

### Run The Pipeline

From the project root, run:

```bash
python run_pipeline.py
```

If `python` is not recognized on Windows, use:

```powershell
py run_pipeline.py
```

### What The Command Does Automatically

The first run is designed to be self-setup:

- creates the repo virtual environment in `.venv` if it does not exist
- installs or refreshes Python dependencies when needed
- runs the pipeline inside that virtual environment
- builds baseline files automatically if they are missing
- writes final files to `data/production/`

The first run may take several minutes because it may need to build the environment and the baseline files.

## Monthly Use

### Standard Monthly Run

Open this repository in VS Code, open the terminal in the project root, and run:

```bash
python run_pipeline.py
```

If `python` is not recognized on Windows, use:

```powershell
py run_pipeline.py
```

By default, the runner:

- uses the repo virtual environment automatically
- only fetches missing monthly ridership data
- reuses existing baseline files if they are already present
- refreshes the production output files in `data/production/`

### Common Options

Refresh one specific month:

```bash
python run_pipeline.py --year 2025 --month 2
```

Refresh a whole year:

```bash
python run_pipeline.py --year 2025
```

Re-download all available monthly ridership data:

```bash
python run_pipeline.py --full-refresh
```

Force the baseline files to be rebuilt:

```bash
python run_pipeline.py --rebuild-baseline
```

You can use the same options with `py` on Windows if needed.

### What To Check After A Run

When the run finishes successfully, these files should be up to date:

- `data/production/monthly_ridership_station.csv`
- `data/production/monthly_ridership_puma.csv`
- `data/production/monthly_ridership_nyc.csv`

The terminal should end with a completion message instead of a Python traceback.

## Troubleshooting

If you see `python: command not found` on Windows:

- use `py run_pipeline.py` instead

If the run warns about `.env` or a placeholder token:

- the pipeline can still run
- adding a real Socrata app token makes rate-limit problems less likely

If dependency installation fails on the first run:

- confirm Python is 3.10+
- close and reopen the terminal
- run `python run_pipeline.py` again

If the terminal shows replacement characters instead of emoji on Windows:

- that is okay
- the pipeline should still work as long as there is no traceback

## When To Use The Special Flags

Use `--year` and `--month` when:

- you want to refresh one month again after a data correction
- you want to rerun a specific recent update without touching everything else

Use `--full-refresh` when:

- the upstream ridership data changed broadly
- you want to rebuild the monthly ridership history from scratch

Use `--rebuild-baseline` when:

- the baseline files are missing
- the baseline logic or reference data changed and you want a fresh baseline build
