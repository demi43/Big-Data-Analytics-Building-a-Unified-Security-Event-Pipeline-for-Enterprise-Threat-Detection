#!/usr/bin/env python3
"""
Create line-based samples from compressed LANL-style data files.

Reads from data/*.gz and data/*.bz2, writes first N lines (or a random sample)
to sample data/ as CSV. Use these for local dev and notebooks without full datasets.

Usage:
  python scripts/create_samples.py
  python scripts/create_samples.py --lines 50000
  python scripts/create_samples.py --lines 10000 --random
  python scripts/create_samples.py --compress
"""

import argparse
import gzip
import bz2
import os
import random
import sys

# Project data paths (script may be run from project root or scripts/)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
SAMPLES_DIR = os.path.join(PROJECT_ROOT, "sample data")

# Files to sample: (filename, open_fn)
DATA_FILES = [
    ("dns.txt.gz", lambda p: gzip.open(p, "rt")),
    ("flows.txt.gz", lambda p: gzip.open(p, "rt")),
    ("proc.txt.gz", lambda p: gzip.open(p, "rt")),
]
# bz2 auth dataset (optional - may have different name)
AUTH_GLOB = "lanl-auth-dataset-1.bz2"


def reservoir_sample(stream, n: int) -> list[str]:
    """Collect up to n random lines using reservoir sampling."""
    reservoir = []
    for i, line in enumerate(stream):
        if i < n:
            reservoir.append(line)
        else:
            j = random.randint(0, i)
            if j < n:
                reservoir[j] = line
    return reservoir


def take_first_n(stream, n: int) -> list[str]:
    """Take first n lines from stream."""
    out = []
    for i, line in enumerate(stream):
        if i >= n:
            break
        out.append(line)
    return out


def main():
    parser = argparse.ArgumentParser(description="Create data samples from compressed LANL-style files.")
    parser.add_argument(
        "--lines",
        type=int,
        default=10_000,
        help="Number of lines per file (default: 10000)",
    )
    parser.add_argument(
        "--random",
        action="store_true",
        help="Use random sampling (reservoir) instead of first N lines",
    )
    parser.add_argument(
        "--compress",
        action="store_true",
        help="Write samples as .gz (default: plain .csv)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed when using --random (default: 42)",
    )
    args = parser.parse_args()

    os.makedirs(SAMPLES_DIR, exist_ok=True)
    if args.random:
        random.seed(args.seed)

    total_written = 0
    for basename, open_fn in DATA_FILES:
        path = os.path.join(DATA_DIR, basename)
        if not os.path.isfile(path):
            print(f"Skip (not found): {path}", file=sys.stderr)
            continue
        out_basename = basename.replace(".gz", "").replace(".bz2", "")
        out_name = out_basename.replace(".txt", "_sample.txt")
        if args.compress:
            out_name += ".gz"
        out_path = os.path.join(SAMPLES_DIR, out_name)

        try:
            with open_fn(path) as f:
                if args.random:
                    lines = reservoir_sample(f, args.lines)
                else:
                    lines = take_first_n(f, args.lines)
            mode = "wt"
            if args.compress:
                with gzip.open(out_path, "wt") as out:
                    out.writelines(lines)
            else:
                with open(out_path, "w", newline="", encoding="utf-8") as out:
                    out.writelines(lines)
            total_written += len(lines)
            print(f"Wrote {len(lines)} lines -> {out_path}")
        except Exception as e:
            print(f"Error processing {path}: {e}", file=sys.stderr)

    # Auth dataset (bz2)
    auth_path = os.path.join(DATA_DIR, AUTH_GLOB)
    if os.path.isfile(auth_path):
        out_name = "auth_sample.txt"
        if args.compress:
            out_name += ".gz"
        out_path = os.path.join(SAMPLES_DIR, out_name)
        try:
            with bz2.open(auth_path, "rt") as f:
                if args.random:
                    lines = reservoir_sample(f, args.lines)
                else:
                    lines = take_first_n(f, args.lines)
            if args.compress:
                with gzip.open(out_path, "wt") as out:
                    out.writelines(lines)
            else:
                with open(out_path, "w", newline="", encoding="utf-8") as out:
                    out.writelines(lines)
            total_written += len(lines)
            print(f"Wrote {len(lines)} lines -> {out_path}")
        except Exception as e:
            print(f"Error processing {auth_path}: {e}", file=sys.stderr)

    print(f"Done. Total lines written: {total_written} -> {SAMPLES_DIR}")


if __name__ == "__main__":
    main()
