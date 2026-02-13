#!/usr/bin/env python3
"""
Run the modern local ridership pipeline.

This standalone pipeline processes raw local ridership CSV files and produces
monthly ridership outputs used by the final merge pipeline.

Steps:
    1. Validate dependencies and required scripts.
    2. Discover raw ridership files in data/raw/ridership/.
    3. Clean ridership staging/processed/output directories.
    4. Stage each raw ridership file.
    5. Process each staged ridership file.
    6. Calculate monthly local ridership outputs.

Usage:
    python pipelines/calculate_ridership_local.py

Output:
    - results/ridership_local/monthly_ridership_station.csv
    - results/ridership_local/monthly_ridership_puma.csv
    - results/ridership_local/monthly_ridership_nyc.csv
"""

from __future__ import annotations

import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Iterable, List


PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = PROJECT_ROOT / "scripts" / "local"
RIDERSHIP_RAW_DIR = PROJECT_ROOT / "data" / "raw" / "ridership"

SCRIPTS_REQUIRED = [
    SCRIPTS_DIR / "stage_ridership_data.py",
    SCRIPTS_DIR / "process_ridership_data.py",
    SCRIPTS_DIR / "calculate_ridership.py",
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


def main() -> int:
    started = time.monotonic()
    run_summary: List[str] = []

    print_header("Modern Ridership Local Pipeline")
    print(f"Started at: {datetime.now().isoformat(timespec='seconds')}")
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Python: {sys.executable}")

    try:
        print_step("Validating runtime dependencies")
        ensure_pandas_available()
        ensure_required_scripts_exist()

        ridership_files = find_ridership_files()
        print_step(
            "Found ridership files: "
            + ", ".join(path.name for path in ridership_files)
        )

        print_header("Step 1: Cleaning Modern Ridership Directories")
        clean_targets = [
            PROJECT_ROOT / "data" / "staging" / "ridership",
            PROJECT_ROOT / "data" / "processed" / "ridership",
            PROJECT_ROOT / "results" / "ridership_local",
        ]
        for target in clean_targets:
            removed = clean_csv_dir(target)
            print_step(f"Cleaned {target.relative_to(PROJECT_ROOT)} ({removed} file(s))")

        print_header("Step 2: Stage Ridership Data")
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
        run_summary.append("stage ridership")

        print_header("Step 3: Process Ridership Data")
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
        run_summary.append("process ridership")

        print_header("Step 4: Calculate Monthly Ridership")
        run_command(
            "Calculating modern ridership metrics",
            [sys.executable, str(SCRIPTS_DIR / "calculate_ridership.py")],
        )
        run_summary.append("calculate ridership")

        elapsed = time.monotonic() - started
        minutes, seconds = divmod(int(elapsed), 60)

        print_header("Pipeline Completed Successfully")
        print(f"Completed at: {datetime.now().isoformat(timespec='seconds')}")
        print(f"Total time: {minutes} minute(s) {seconds} second(s)")
        print("Ran: " + ", ".join(run_summary))
        print("\nOutputs:")
        print("  - results/ridership_local/monthly_ridership_station.csv")
        print("  - results/ridership_local/monthly_ridership_puma.csv")
        print("  - results/ridership_local/monthly_ridership_nyc.csv")
        return 0

    except (FileNotFoundError, RuntimeError, subprocess.CalledProcessError) as exc:
        print_error(str(exc))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
