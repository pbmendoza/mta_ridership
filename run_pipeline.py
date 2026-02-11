#!/usr/bin/env python3
"""Cross-platform pipeline runner for MTA ridership processing."""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Iterable, List


PROJECT_ROOT = Path(__file__).resolve().parent
SCRIPTS_DIR = PROJECT_ROOT / "scripts"

TURNSTILE_COMBINED = PROJECT_ROOT / "data" / "staging" / "turnstile" / "turnstile_combined.csv"
RIDERSHIP_RAW_DIR = PROJECT_ROOT / "data" / "raw" / "ridership"

BASELINE_REQUIRED_FILES = [
    PROJECT_ROOT / "results" / "baseline" / "monthly_baseline_station.csv",
    PROJECT_ROOT / "results" / "baseline" / "monthly_baseline_puma.csv",
    PROJECT_ROOT / "results" / "baseline" / "monthly_baseline_nyc.csv",
]

SCRIPTS_REQUIRED = [
    SCRIPTS_DIR / "stage_ridership_data.py",
    SCRIPTS_DIR / "process_ridership_data.py",
    SCRIPTS_DIR / "calculate_ridership.py",
    SCRIPTS_DIR / "calculate_final.py",
    SCRIPTS_DIR / "enrich_final_data.py",
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


def print_warning(message: str) -> None:
    print(f"[WARN] {message}")


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


def find_ridership_files() -> List[Path]:
    """Find top-level raw ridership CSV files sorted by filename."""
    if not RIDERSHIP_RAW_DIR.exists():
        raise FileNotFoundError(f"Ridership source directory not found: {RIDERSHIP_RAW_DIR}")

    files = sorted(
        [path for path in RIDERSHIP_RAW_DIR.glob("*.csv") if path.is_file()],
        key=lambda path: path.name.lower(),
    )
    if not files:
        raise FileNotFoundError(f"No ridership CSV files found in {RIDERSHIP_RAW_DIR}")
    return files


def validate_baseline_files() -> None:
    """Require baseline files to exist before default-mode final merge."""
    missing = [path for path in BASELINE_REQUIRED_FILES if not path.is_file()]
    if missing:
        missing_list = "\n".join(f"  - {path.relative_to(PROJECT_ROOT)}" for path in missing)
        raise RuntimeError(
            "Baseline files are missing. Default mode requires existing baseline outputs.\n"
            f"{missing_list}\n"
            "Run with '--include-historical' to generate baseline files."
        )


def should_stage_turnstile(force_flag: bool) -> bool:
    """Determine whether historical turnstile staging should run."""
    return force_flag or (not TURNSTILE_COMBINED.is_file())


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
            "Run the MTA ridership pipeline. Default mode runs modern ridership "
            "processing, final merge, and enrichment using existing baseline files."
        )
    )
    parser.add_argument(
        "--include-historical",
        action="store_true",
        help=(
            "Run historical turnstile staging/processing and baseline calculation "
            "before modern branch."
        ),
    )
    parser.add_argument(
        "--force-turnstile-stage",
        action="store_true",
        help=(
            "Force regeneration of data/staging/turnstile/turnstile_combined.csv. "
            "Requires --include-historical."
        ),
    )
    parser.add_argument(
        "--skip-enrich",
        action="store_true",
        help="Skip scripts/enrich_final_data.py after final merge.",
    )

    args = parser.parse_args()
    if args.force_turnstile_stage and not args.include_historical:
        parser.error("--force-turnstile-stage requires --include-historical")
    return args


def main() -> int:
    args = parse_args()
    started = time.monotonic()
    run_summary: List[str] = []

    print_header("MTA Ridership Data Processing Pipeline (Python Runner)")
    print(f"Started at: {datetime.now().isoformat(timespec='seconds')}")
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Python: {sys.executable}")

    try:
        print_step("Validating runtime dependencies")
        ensure_pandas_available()
        ensure_required_scripts_exist()

        print_header("Step 0: Cleaning Output Directories")
        clean_targets = [
            PROJECT_ROOT / "data" / "staging" / "ridership",
            PROJECT_ROOT / "data" / "processed" / "ridership",
            PROJECT_ROOT / "results" / "ridership",
            PROJECT_ROOT / "results" / "final",
        ]
        if args.include_historical:
            clean_targets.append(PROJECT_ROOT / "results" / "baseline")

        for target in clean_targets:
            removed = clean_csv_dir(target)
            print_step(f"Cleaned {target.relative_to(PROJECT_ROOT)} ({removed} file(s))")

        ridership_files = find_ridership_files()
        print_step(
            "Found ridership files: "
            + ", ".join(path.name for path in ridership_files)
        )

        if args.include_historical:
            print_header("Step 1: Historical Branch (Optional)")

            if should_stage_turnstile(args.force_turnstile_stage):
                if args.force_turnstile_stage:
                    print_step("Force mode enabled for turnstile staging")
                run_command(
                    "Staging turnstile data",
                    [sys.executable, str(SCRIPTS_DIR / "stage_turnstile_data.py")],
                )
            else:
                print_step(
                    "Skipping turnstile staging; using cached "
                    "data/staging/turnstile/turnstile_combined.csv"
                )

            run_command(
                "Processing turnstile data",
                [sys.executable, str(SCRIPTS_DIR / "process_turnstile_data.py")],
            )
            run_command(
                "Calculating baseline",
                [sys.executable, str(SCRIPTS_DIR / "calculate_baseline.py")],
            )
            run_summary.append("historical branch")
        else:
            print_header("Step 1: Baseline Validation (Default Mode)")
            validate_baseline_files()
            print_step("Baseline files detected; proceeding without historical branch")
            run_summary.append("baseline validation only")

        print_header("Step 2: Modern Ridership Branch")
        for ridership_file in ridership_files:
            run_command(
                f"Staging ridership file {ridership_file.name}",
                [
                    sys.executable,
                    str(SCRIPTS_DIR / "stage_ridership_data.py"),
                    "--filename",
                    ridership_file.name,
                ],
            )

        for ridership_file in ridership_files:
            run_command(
                f"Processing ridership file {ridership_file.name}",
                [
                    sys.executable,
                    str(SCRIPTS_DIR / "process_ridership_data.py"),
                    "--filename",
                    ridership_file.name,
                ],
            )

        run_command(
            "Calculating modern ridership metrics",
            [sys.executable, str(SCRIPTS_DIR / "calculate_ridership.py")],
        )
        run_summary.append("modern ridership branch")

        print_header("Step 3: Final Merge")
        run_command(
            "Merging ridership with baseline",
            [sys.executable, str(SCRIPTS_DIR / "calculate_final.py")],
        )
        run_summary.append("final merge")

        if args.skip_enrich:
            print_step("Skipping enrichment by request (--skip-enrich)")
            run_summary.append("enrichment skipped")
        else:
            print_header("Step 4: Enrichment")
            run_command(
                "Enriching final outputs",
                [sys.executable, str(SCRIPTS_DIR / "enrich_final_data.py")],
            )
            run_summary.append("enrichment")

        elapsed = time.monotonic() - started
        minutes, seconds = divmod(int(elapsed), 60)

        print_header("Pipeline Completed Successfully")
        print(f"Completed at: {datetime.now().isoformat(timespec='seconds')}")
        print(f"Total time: {minutes} minute(s) {seconds} second(s)")
        print("Ran: " + ", ".join(run_summary))
        print("\nOutputs:")
        print("  - results/ridership/")
        print("  - results/final/")
        if args.include_historical:
            print("  - results/baseline/ (regenerated)")
        else:
            print("  - results/baseline/ (reused existing)")
        return 0

    except (FileNotFoundError, RuntimeError, subprocess.CalledProcessError) as exc:
        print_error(str(exc))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
