"""
h3_events.py — Convert an AIS positions dataframe into H3 cell counts.

Reads a Parquet (or CSV) file of AIS positions with lat/lon columns,
buckets each position into an H3 cell at resolution 10, and writes
the result as JSON ready for MinIO upload.

Usage:
    python h3_events.py positions.parquet --mmsi 123456789
    python h3_events.py positions.csv --mmsi 123456789 --resolution 8
    python h3_events.py positions.parquet --mmsi 123456789 -o output/

Output (to stdout or file):
    {"resolution": 10, "cells": {"8a28a1a6c1c7fff": 42, ...}}
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import h3
import polars as pl


RESOLUTION = 10
LAT_COL = "lat"
LON_COL = "lon"
MMSI_COL = "mmsi"


def load_dataframe(path: str) -> pl.DataFrame:
    p = Path(path)
    if p.suffix == ".csv":
        return pl.read_csv(p)
    return pl.read_parquet(p)


def to_h3_cell(lat: float, lon: float, resolution: int) -> str | None:
    try:
        return h3.latlng_to_cell(lat, lon, resolution)
    except Exception:
        return None


def compute_h3_counts(df: pl.DataFrame, resolution: int) -> dict[str, int]:
    """Take a dataframe with lat/lon columns and return {h3_index: count}."""
    df = df.filter(
        pl.col(LAT_COL).is_not_null() & pl.col(LON_COL).is_not_null()
    )

    cells: dict[str, int] = {}
    for lat, lon in zip(df[LAT_COL].to_list(), df[LON_COL].to_list()):
        cell = to_h3_cell(lat, lon, resolution)
        if cell:
            cells[cell] = cells.get(cell, 0) + 1

    return cells


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Convert AIS positions to H3 cell counts."
    )
    parser.add_argument("input", help="Path to Parquet or CSV file")
    parser.add_argument("--mmsi", required=True, help="MMSI to filter for")
    parser.add_argument(
        "--resolution", type=int, default=RESOLUTION,
        help=f"H3 resolution (default {RESOLUTION}, max 15)",
    )
    parser.add_argument(
        "-o", "--output-dir",
        help="Write to <dir>/h3/<mmsi>/events.json instead of stdout",
    )
    args = parser.parse_args()

    resolution = max(0, min(args.resolution, 15))

    df = load_dataframe(args.input)

    # Normalise column names to lowercase
    df = df.rename({c: c.lower() for c in df.columns})

    if MMSI_COL in df.columns:
        df = df.filter(pl.col(MMSI_COL).cast(pl.Utf8) == str(args.mmsi))

    if df.is_empty():
        print(f"No rows found for MMSI {args.mmsi}", file=sys.stderr)
        sys.exit(1)

    cells = compute_h3_counts(df, resolution)
    payload = {"resolution": resolution, "cells": cells}

    if args.output_dir:
        out_path = Path(args.output_dir) / "h3" / args.mmsi / "events.json"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(payload))
        print(f"Wrote {len(cells)} cells to {out_path}", file=sys.stderr)
    else:
        json.dump(payload, sys.stdout, indent=2)
        print(file=sys.stdout)


if __name__ == "__main__":
    main()
