#!/usr/bin/env python3
"""Run the full monthly ridership update pipeline."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
import subprocess
import sys
from typing import Sequence

PROJECT_ROOT = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class PipelineStep:
    """A single executable pipeline step."""

    description: str
    script: Path
    extra_args: tuple[str, ...] = ()


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse CLI flags for the monthly runner."""
    parser = argparse.ArgumentParser(
        description="Run the monthly MTA ridership update pipeline."
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        default=False,
        help="Re-fetch all station ridership months from scratch.",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=None,
        help="Optional year to refresh in the station ridership step.",
    )
    parser.add_argument(
        "--month",
        type=int,
        default=None,
        help="Optional month (1-12). Requires --year.",
    )
    parser.add_argument(
        "--rebuild-baseline",
        action="store_true",
        default=False,
        help="Rebuild API baseline files even if they already exist.",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.month is not None and args.year is None:
        parser.error("--month requires --year.")
    if args.month is not None and not 1 <= args.month <= 12:
        parser.error("--month must be in 1..12.")
    if args.full_refresh and (args.year is not None or args.month is not None):
        print(
            "⚠️  --full-refresh processes all months; "
            "--year/--month filters will be ignored."
        )
        args.year = None
        args.month = None

    return args


def build_station_step_args(args: argparse.Namespace) -> list[str]:
    """Build the CLI args forwarded to the station ridership script."""
    forwarded: list[str] = []
    if args.full_refresh:
        forwarded.append("--full-refresh")
        return forwarded
    if args.year is not None:
        forwarded.extend(["--year", str(args.year)])
    if args.month is not None:
        forwarded.extend(["--month", str(args.month)])
    return forwarded


def baseline_file_paths(project_root: Path = PROJECT_ROOT) -> list[Path]:
    """Return the expected API baseline outputs."""
    baseline_dir = project_root / "data" / "api" / "baseline"
    return [
        baseline_dir / "monthly_baseline_station.csv",
        baseline_dir / "monthly_baseline_puma.csv",
        baseline_dir / "monthly_baseline_nyc.csv",
    ]


def missing_baseline_files(project_root: Path = PROJECT_ROOT) -> list[Path]:
    """Return any baseline outputs that are missing from disk."""
    return [path for path in baseline_file_paths(project_root) if not path.exists()]


def should_run_baseline(
    args: argparse.Namespace,
    project_root: Path = PROJECT_ROOT,
) -> tuple[bool, list[Path], str]:
    """Decide whether the baseline step should run and why."""
    missing = missing_baseline_files(project_root)
    if args.rebuild_baseline:
        return True, missing, "forced rebuild requested"
    if missing:
        return True, missing, "missing baseline file(s)"
    return False, missing, "reusing existing baseline files"


def build_steps(args: argparse.Namespace, project_root: Path = PROJECT_ROOT) -> list[PipelineStep]:
    """Build the ordered pipeline step list for this run."""
    run_baseline, missing, _ = should_run_baseline(args, project_root)
    steps = [
        PipelineStep(
            description="Calculate ridership by station",
            script=project_root / "scripts" / "api" / "calculate_ridership_by_station.py",
            extra_args=tuple(build_station_step_args(args)),
        ),
        PipelineStep(
            description="Aggregate to PUMA & NYC",
            script=project_root / "scripts" / "api" / "aggregate_puma_nyc.py",
        ),
    ]

    if run_baseline:
        description = "Rebuild baseline files" if args.rebuild_baseline else "Build baseline files"
        if missing and not args.rebuild_baseline:
            description += " (missing)"
        steps.append(
            PipelineStep(
                description=description,
                script=project_root / "scripts" / "api" / "calculate_baseline.py",
            )
        )

    steps.extend(
        [
            PipelineStep(
                description="Merge ridership + baseline (final)",
                script=project_root / "scripts" / "calculate_final.py",
            ),
            PipelineStep(
                description="Enrich & sort final data",
                script=project_root / "scripts" / "enrich_final_data.py",
            ),
        ]
    )
    return steps


def run_step(
    label: str,
    script: Path,
    *,
    project_root: Path,
    extra_args: list[str] | None = None,
) -> None:
    """Run a single pipeline step, aborting on failure."""
    cmd = [sys.executable, str(script)] + (extra_args or [])
    width = 58
    content = f"🚀 {label}"
    if len(content) > width - 4:
        content = content[: width - 7] + "…"

    print(f"\n┌{'─' * (width - 2)}┐")
    print(f"│ {content.ljust(width - 4)} │")
    print(f"└{'─' * (width - 2)}┘")

    result = subprocess.run(cmd, cwd=str(project_root), check=False)
    if result.returncode != 0:
        print(f"\n❌ Pipeline stopped at {label} (could not complete).")
        print(f"   Exit code: {result.returncode}")
        print("   A non-zero exit means this step could not complete and stopped the pipeline.")
        raise SystemExit(result.returncode)


def main(
    argv: Sequence[str] | None = None,
    *,
    project_root: Path = PROJECT_ROOT,
) -> int:
    """Run all pipeline steps in order."""
    args = parse_args(argv)
    run_baseline, missing, baseline_status = should_run_baseline(args, project_root)
    steps = build_steps(args, project_root)

    print("📦 Monthly Ridership Update Pipeline")
    print(f"   Project root: {project_root}")
    print(f"   Python: {sys.executable}")
    print(f"   Full refresh: {args.full_refresh}")
    if args.year is not None:
        target = str(args.year) if args.month is None else f"{args.year}-{args.month:02d}"
        print(f"   Targeted refresh: {target}")
    else:
        print("   Targeted refresh: all missing months")

    if run_baseline:
        if args.rebuild_baseline:
            print("   Baseline: will rebuild because --rebuild-baseline was passed")
        else:
            missing_names = ", ".join(path.name for path in missing)
            print(f"   Baseline: will build because these files are missing: {missing_names}")
    else:
        print(f"   Baseline: {baseline_status}")

    total_steps = len(steps)
    for index, step in enumerate(steps, start=1):
        run_step(
            f"Step {index}/{total_steps} — {step.description}",
            step.script,
            project_root=project_root,
            extra_args=list(step.extra_args),
        )

    print("\n╭──────────────────────────────────────╮")
    print("│ ✅ Monthly Ridership Pipeline complete │")
    print("╰──────────────────────────────────────╯")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
