#!/usr/bin/env python3
"""Pipeline runner for final merge and enrichment using precomputed inputs."""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Iterable, List


PROJECT_ROOT = Path(__file__).resolve().parent
SCRIPTS_DIR = PROJECT_ROOT / "scripts" / "local"
SCRIPTS_ROOT = PROJECT_ROOT / "scripts"

BASELINE_REQUIRED_FILES = [
    PROJECT_ROOT / "results" / "baseline" / "monthly_baseline_station.csv",
    PROJECT_ROOT / "results" / "baseline" / "monthly_baseline_puma.csv",
    PROJECT_ROOT / "results" / "baseline" / "monthly_baseline_nyc.csv",
]

RIDERSHIP_LOCAL_REQUIRED_FILES = [
    PROJECT_ROOT / "results" / "ridership_local" / "monthly_ridership_station.csv",
    PROJECT_ROOT / "results" / "ridership_local" / "monthly_ridership_puma.csv",
    PROJECT_ROOT / "results" / "ridership_local" / "monthly_ridership_nyc.csv",
]

SCRIPTS_REQUIRED = [
    SCRIPTS_DIR / "calculate_final.py",
    SCRIPTS_ROOT / "enrich_final_data.py",
]


def print_header(message: str) -> None:
    print(f"\n{'=' * 48}")
    print(message)
    print(f"{'=' * 48}\n")


def print_step(message: str) -> None:
    print(f"[STEP] {message}")


def print_error(message: str) -> None:
    print(f"[ERROR] {message}", file=sys.stderr)


def ensure_pandas_available() -> None:
    """Fail fast if required dependency is missing."""
    try:
        import pandas  # noqa: F401
    except ImportError as exc:  # pragma: no cover - tested via CLI behavior
        raise RuntimeError(
            "Required dependency 'pandas' is not installed for this Python interpreter. "
            f"Interpreter: {sys.executable}"
        ) from exc


def ensure_required_scripts_exist() -> None:
    """Ensure all scripts invoked by the runner exist."""
    missing = [path for path in SCRIPTS_REQUIRED if not path.is_file()]
    if missing:
        missing_list = "\n".join(f"  - {path.relative_to(PROJECT_ROOT)}" for path in missing)
        raise FileNotFoundError(f"Missing required scripts:\n{missing_list}")


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


def validate_baseline_files() -> None:
    """Require baseline files to exist before default-mode final merge."""
    missing = [path for path in BASELINE_REQUIRED_FILES if not path.is_file()]
    if missing:
        missing_list = "\n".join(f"  - {path.relative_to(PROJECT_ROOT)}" for path in missing)
        raise RuntimeError(
            "Baseline files are missing. This pipeline requires existing baseline outputs.\n"
            f"{missing_list}\n"
            "Run 'python pipelines/calculate_baseline_local_turnstile.py' to generate them."
        )


def validate_ridership_local_files() -> None:
    """Require modern local ridership outputs before final merge."""
    missing = [path for path in RIDERSHIP_LOCAL_REQUIRED_FILES if not path.is_file()]
    if missing:
        missing_list = "\n".join(f"  - {path.relative_to(PROJECT_ROOT)}" for path in missing)
        raise RuntimeError(
            "Modern ridership files are missing. This pipeline requires existing local ridership outputs.\n"
            f"{missing_list}\n"
            "Run 'python pipelines/calculate_ridership_local.py' to generate them."
        )


def run_command(step_name: str, args_list: Iterable[str]) -> None:
    """Run a child command and fail with context on non-zero exit."""
    cmd = list(args_list)
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
            "Run the MTA finalization pipeline. Merges existing modern ridership and "
            "baseline outputs, then optionally enriches final files."
        )
    )
    parser.add_argument(
        "--skip-enrich",
        action="store_true",
        help="Skip scripts/enrich_final_data.py after final merge.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    started = time.monotonic()
    run_summary: List[str] = []

    print_header("MTA Ridership Final Pipeline (Python Runner)")
    print(f"Started at: {datetime.now().isoformat(timespec='seconds')}")
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Python: {sys.executable}")

    try:
        print_step("Validating runtime dependencies")
        ensure_pandas_available()
        ensure_required_scripts_exist()

        print_header("Step 1: Baseline Validation")
        validate_baseline_files()
        print_step("Baseline files detected")

        print_header("Step 2: Modern Ridership Validation")
        validate_ridership_local_files()
        print_step("Local ridership files detected")

        print_header("Step 3: Cleaning Final Output Directory")
        removed = clean_csv_dir(PROJECT_ROOT / "results" / "final")
        print_step(f"Cleaned results/final ({removed} file(s))")

        print_header("Step 4: Final Merge")
        run_command(
            "Merging ridership with baseline",
            [sys.executable, str(SCRIPTS_DIR / "calculate_final.py")],
        )
        run_summary.append("final merge")

        if args.skip_enrich:
            print_step("Skipping enrichment by request (--skip-enrich)")
            run_summary.append("enrichment skipped")
        else:
            print_header("Step 5: Enrichment")
            run_command(
                "Enriching final outputs",
                [sys.executable, str(SCRIPTS_ROOT / "enrich_final_data.py")],
            )
            run_summary.append("enrichment")

        elapsed = time.monotonic() - started
        minutes, seconds = divmod(int(elapsed), 60)

        print_header("Pipeline Completed Successfully")
        print(f"Completed at: {datetime.now().isoformat(timespec='seconds')}")
        print(f"Total time: {minutes} minute(s) {seconds} second(s)")
        print("Ran: " + ", ".join(run_summary))
        print("\nOutputs:")
        print("  - results/ridership_local/ (existing)")
        print("  - results/final/")
        print("  - results/baseline/ (existing)")
        return 0

    except (FileNotFoundError, RuntimeError, subprocess.CalledProcessError) as exc:
        print_error(str(exc))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
