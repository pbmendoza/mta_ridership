"""Combine monthly ridership CSVs into a single yearly file."""

from pathlib import Path

import pandas as pd


def find_project_root(start: Path | None = None) -> Path:
    """Walk parents from `start` upward to find a directory containing `.git`."""

    current = (start or Path.cwd()).resolve()

    for candidate in [current, *current.parents]:
        if (candidate / ".git").exists():
            return candidate

    raise FileNotFoundError(f"Unable to locate repository root from {start or Path.cwd()}")


PROJECT_ROOT = find_project_root(start=Path(__file__).resolve().parent)
SCRIPT_DIR = Path(__file__).parent

YEAR = 2025
INPUT_DIR = PROJECT_ROOT / "data" / "local" / "raw" / "ridership" / str(YEAR)
OUTPUT_FILE = PROJECT_ROOT / "data" / "local" / "raw" / "ridership" / f"{YEAR}.csv"

monthly_files = sorted(INPUT_DIR.glob("*.csv"), key=lambda f: int(f.stem))
print(f"📂 Found {len(monthly_files)} monthly files in {INPUT_DIR.relative_to(PROJECT_ROOT)}")

header_written = False
for f in monthly_files:
    print(f"  📄 Processing {f.name}...")
    for chunk in pd.read_csv(f, chunksize=500_000):
        chunk.to_csv(
            OUTPUT_FILE,
            mode="a" if header_written else "w",
            header=not header_written,
            index=False,
        )
        header_written = True

print(f"✅ Combined file saved to {OUTPUT_FILE.relative_to(PROJECT_ROOT)}")
