#!/usr/bin/env python3
"""Run the full monthly ridership update pipeline.

Executes these scripts sequentially:
    1. scripts/api/calculate_ridership_by_station.py
    2. scripts/api/aggregate_puma_nyc.py
    3. scripts/calculate_final.py
    4. scripts/enrich_final_data.py

Set FULL_REFRESH = True to reprocess all months from scratch in step 1.
"""

import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# --- Configuration ---------------------------------------------------------
FULL_REFRESH = False
# ---------------------------------------------------------------------------


def run_step(label: str, script: Path, extra_args: list[str] | None = None) -> None:
    """Run a single pipeline step, aborting on failure."""
    cmd = [sys.executable, str(script)] + (extra_args or [])
    width = 58
    content = f"🚀 {label}"
    if len(content) > width - 4:
        content = content[: width - 7] + "…"

    print(f"\n┌{'─' * (width - 2)}┐")
    print(f"│ {content.ljust(width - 4)} │")
    print(f"└{'─' * (width - 2)}┘")

    result = subprocess.run(cmd, cwd=str(PROJECT_ROOT))
    if result.returncode != 0:
        print(f"\n❌ Pipeline stopped at {label} (could not complete).")
        print(f"   Exit code: {result.returncode}")
        print("   A non-zero exit means this step could not complete and stopped the pipeline.")
        sys.exit(result.returncode)


def main() -> None:
    """Run all pipeline steps in order."""
    print("📦 Monthly Ridership Update Pipeline")
    print(f"   Project root: {PROJECT_ROOT}")
    print(f"   Full refresh: {FULL_REFRESH}")

    # Step 1
    step1_args = ["--full-refresh"] if FULL_REFRESH else []
    run_step(
        "Step 1/4 — Calculate ridership by station",
        PROJECT_ROOT / "scripts" / "api" / "calculate_ridership_by_station.py",
        extra_args=step1_args,
    )

    # Step 2
    run_step(
        "Step 2/4 — Aggregate to PUMA & NYC",
        PROJECT_ROOT / "scripts" / "api" / "aggregate_puma_nyc.py",
    )

    # Step 3
    run_step(
        "Step 3/4 — Merge ridership + baseline (final)",
        PROJECT_ROOT / "scripts" / "calculate_final.py",
    )

    # Step 4
    run_step(
        "Step 4/4 — Enrich & sort final data",
        PROJECT_ROOT / "scripts" / "enrich_final_data.py",
    )

    print("\n╭──────────────────────────────────────╮")
    print("│ ✅ Monthly Ridership Pipeline complete │")
    print("╰──────────────────────────────────────╯")


if __name__ == "__main__":
    main()
