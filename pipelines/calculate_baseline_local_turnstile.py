#!/usr/bin/env python3
"""
Run the historical turnstile pipeline to generate baseline ridership files.

This standalone pipeline processes historical MTA turnstile data (2014–2023)
to produce monthly baseline averages at three geographic levels:
station, PUMA, and NYC-wide.

Steps:
    1. Stage turnstile data (combine raw files → turnstile_combined.csv)
       Skipped if the cached file already exists; use --force-stage to regenerate.
    2. Process turnstile data (daily complex-level aggregates)
    3. Calculate baseline (monthly averages for the selected years)

Usage:
    python pipelines/calculate_baseline_local_turnstile.py
    python pipelines/calculate_baseline_local_turnstile.py --force-stage
    python pipelines/calculate_baseline_local_turnstile.py --years 2017 2018 2019

Output:
    results/baseline_turnstile/monthly_baseline_station.csv
    results/baseline_turnstile/monthly_baseline_puma.csv
    results/baseline_turnstile/monthly_baseline_nyc.csv
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = PROJECT_ROOT / "scripts" / "local"
SCRIPTS_ROOT = PROJECT_ROOT / "scripts"
TURNSTILE_COMBINED = PROJECT_ROOT / "data" / "staging" / "turnstile" / "turnstile_combined.csv"

SCRIPTS_REQUIRED = [
    SCRIPTS_DIR / "stage_turnstile_data.py",
    SCRIPTS_DIR / "process_turnstile_data.py",
    SCRIPTS_DIR / "calculate_baseline.py",
]


def print_header(message: str) -> None:
    print(f"\n{'=' * 48}")
    print(message)
    print(f"{'=' * 48}\n")


def print_step(message: str) -> None:
    print(f"[STEP] {message}")


def print_error(message: str) -> None:
    print(f"[ERROR] {message}", file=sys.stderr)


def clean_csv_dir(path: Path) -> int:
    """Delete top-level CSV files in a directory and return count removed."""
    if not path.exists() or not path.is_dir():
        return 0
    count = 0
    for csv_path in path.glob("*.csv"):
        try:
            csv_path.unlink()
            count += 1
        except FileNotFoundError:
            continue
    return count


def run_command(step_name: str, cmd: list[str]) -> None:
    """Run a child command and fail with context on non-zero exit."""
    print_step(f"{step_name}: {' '.join(cmd)}")
    completed = subprocess.run(cmd, cwd=PROJECT_ROOT, check=False)
    if completed.returncode != 0:
        raise subprocess.CalledProcessError(
            returncode=completed.returncode,
            cmd=cmd,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run the historical turnstile pipeline to generate baseline "
            "ridership files (results/baseline/)."
        )
    )
    parser.add_argument(
        "--force-stage",
        action="store_true",
        help=(
            "Force regeneration of data/staging/turnstile/turnstile_combined.csv "
            "even if it already exists."
        ),
    )
    parser.add_argument(
        "--years",
        type=int,
        nargs="+",
        default=[2015, 2016, 2017, 2018, 2019],
        help="Baseline years to average over (default: 2015 2016 2017 2018 2019).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    started = time.monotonic()

    print_header("Historical Turnstile Pipeline")
    print(f"Started at: {datetime.now().isoformat(timespec='seconds')}")
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Python: {sys.executable}")

    try:
        # Validate scripts exist
        missing = [p for p in SCRIPTS_REQUIRED if not p.is_file()]
        if missing:
            missing_list = "\n".join(f"  - {p.relative_to(PROJECT_ROOT)}" for p in missing)
            raise FileNotFoundError(f"Missing required scripts:\n{missing_list}")

        # Clean baseline output directory
        print_header("Step 0: Cleaning Baseline Output")
        removed = clean_csv_dir(PROJECT_ROOT / "results" / "baseline_turnstile")
        print_step(f"Cleaned results/baseline_turnstile ({removed} file(s))")

        # Step 1: Stage turnstile data
        print_header("Step 1: Stage Turnstile Data")
        should_stage = args.force_stage or not TURNSTILE_COMBINED.is_file()

        if should_stage:
            if args.force_stage:
                print_step("Force mode enabled — regenerating turnstile_combined.csv")
            run_command(
                "Staging turnstile data",
                [sys.executable, str(SCRIPTS_DIR / "stage_turnstile_data.py")],
            )
        else:
            print_step(
                "Skipping staging; using cached "
                "data/staging/turnstile/turnstile_combined.csv"
            )

        # Step 2: Process turnstile data
        print_header("Step 2: Process Turnstile Data")
        run_command(
            "Processing turnstile data",
            [sys.executable, str(SCRIPTS_DIR / "process_turnstile_data.py")],
        )

        # Step 3: Calculate baseline
        print_header("Step 3: Calculate Baseline")
        run_command(
            "Calculating baseline",
            [sys.executable, str(SCRIPTS_DIR / "calculate_baseline.py"),
             "--years", *[str(y) for y in args.years]],
        )

        elapsed = time.monotonic() - started
        minutes, seconds = divmod(int(elapsed), 60)

        print_header("Pipeline Completed Successfully")
        print(f"Completed at: {datetime.now().isoformat(timespec='seconds')}")
        print(f"Total time: {minutes} minute(s) {seconds} second(s)")
        print("\nOutputs:")
        print("  - results/baseline_turnstile/monthly_baseline_station.csv")
        print("  - results/baseline_turnstile/monthly_baseline_puma.csv")
        print("  - results/baseline_turnstile/monthly_baseline_nyc.csv")
        return 0

    except (FileNotFoundError, RuntimeError, subprocess.CalledProcessError) as exc:
        print_error(str(exc))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
