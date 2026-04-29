#!/usr/bin/env python3
"""Run all DuckDB Sketch2 SQL tutorials in sequence."""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run all DuckDB Sketch2 tutorial scripts end-to-end.")
    parser.add_argument(
        "--root",
        type=Path,
        default=Path("/tmp") / "sketch2duckdb_tutorial",
        help="Tutorial Sketch2 database root directory (default: /tmp/sketch2duckdb_tutorial)",
    )
    return parser.parse_args()


def run_cmd(argv: list[str]) -> None:
    print(f"$ {' '.join(argv)}")
    subprocess.run(argv, check=True)


def main() -> None:
    args = parse_args()
    root = args.root.resolve()
    script_dir = Path(__file__).resolve().parent
    py = sys.executable

    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True, exist_ok=True)

    for index in range(5, 9):
        script_name = f"tutorial_{index:02d}.py"
        dataset_name = f"tutorial_{index:02d}_dataset"
        run_cmd([py, str(script_dir / script_name), "--root", str(root), dataset_name])

    print("\nAll DuckDB tutorial scripts finished successfully.")


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as exc:
        raise SystemExit(exc.returncode) from exc
