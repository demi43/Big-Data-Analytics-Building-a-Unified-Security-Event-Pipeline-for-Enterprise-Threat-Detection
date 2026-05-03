#!/usr/bin/env python3
"""
executepipeline.py — Full end-to-end pipeline runner.

Stages:
  1. Fetch URLHaus threat intel from API
  2. Convert URLHaus JSON to Parquet
  3. Silver layer: auth, dns, flows, proc -> Parquet
  4. Gold layer:   enrich + aggregate
  5. Validation:   record counts and sample output

Usage:
  python executepipeline.py
  python executepipeline.py --skip-fetch      # use cached URLHaus data
  python executepipeline.py --skip-silver     # skip Silver (use existing Parquet)
  python executepipeline.py --skip-gold       # skip Gold enrichment
  python executepipeline.py --validate-only   # validation report only
"""

import argparse
import io
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent.resolve()
sys.path.insert(0, str(ROOT / "src"))

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env", override=False)
except ImportError:
    pass  # run: pip install -r requirements.txt

PROCESSING_SCRIPTS = [
    ROOT / "src" / "processing" / "auth_to_parquet.py",
    ROOT / "src" / "processing" / "dns_to_parquet.py",
    ROOT / "src" / "processing" / "flows_to_parquet.py",
    ROOT / "src" / "processing" / "proc_to_parquet.py",
]

ENRICH_SCRIPT    = ROOT / "src" / "scripts" / "enrich.py"
URLHAUS_SCRIPT   = ROOT / "scripts" / "fetch_urlhaus.py"
URLHAUS_JSON     = ROOT / "sample data" / "urlhaus_sample.json"

BAR = "=" * 60

# ── Helpers ───────────────────────────────────────────────────────────────────
def header(title):
    print(f"\n{BAR}\n{title}\n{BAR}")

def ok(msg):   print(f"  [OK]   {msg}")
def warn(msg): print(f"  [WARN] {msg}")
def fail(msg): print(f"  [FAIL] {msg}", file=sys.stderr)


def run_script(script_path):
    """Run a Python script as a subprocess. Returns (success, elapsed_seconds)."""
    t0 = time.time()
    try:
        subprocess.run([sys.executable, str(script_path)], check=True)
        return True, time.time() - t0
    except subprocess.CalledProcessError as e:
        print(f"  exit code: {e.returncode}", file=sys.stderr)
        return False, time.time() - t0


# ── Stage 1: URLHaus fetch ────────────────────────────────────────────────────
def fetch_urlhaus():
    header("Stage 1: Fetch URLHaus Threat Intel")

    if not os.environ.get("URLHAUS_API_KEY", "").strip():
        warn("URLHAUS_API_KEY not set — skipping API fetch")
        if URLHAUS_JSON.exists():
            ok(f"Using cached: {URLHAUS_JSON.name}")
            return True
        fail("No URLHaus data found")
        return False

    success, elapsed = run_script(URLHAUS_SCRIPT)
    if success:
        ok(f"Fetched  ({elapsed:.1f}s)")
        return True

    warn(f"Fetch failed — checking for cached data")
    if URLHAUS_JSON.exists():
        ok("Cached data found — continuing")
        return True
    fail("No URLHaus data available")
    return False


# ── Stage 2: URLHaus JSON → Parquet ──────────────────────────────────────────
def urlhaus_to_parquet():
    header("Stage 2: URLHaus JSON → Parquet")

    if not URLHAUS_JSON.exists():
        fail(f"Not found: {URLHAUS_JSON}")
        return False

    try:
        import pandas as pd
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as e:
        fail(f"Missing dependency: {e}. Run: pip install -r requirements.txt")
        return False

    with open(URLHAUS_JSON, encoding="utf-8") as f:
        data = json.load(f)

    rows = [
        {
            "url":        u.get("url", ""),
            "host":       u.get("host", ""),
            "threat":     u.get("threat", ""),
            "date_added": u.get("date_added", ""),
            "tags":       ",".join(u.get("tags") or []),
        }
        for u in data.get("urls", [])
    ]

    table = pa.Table.from_pandas(pd.DataFrame(rows), preserve_index=False)

    # Resolve output — S3 or local
    silver_uri = os.environ.get("SILVER_PARQUET_URI", "").strip()
    if silver_uri:
        try:
            import boto3
        except ImportError:
            fail("boto3 not installed. Run: pip install boto3")
            return False
        bucket = os.environ.get("S3_URLHAUS_BUCKET", "").strip()
        key    = os.environ.get("S3_URLHAUS_KEY", "silver/urlhaus/urlhaus.parquet").strip().lstrip("/")
        if not bucket:
            fail("S3_URLHAUS_BUCKET not set in .env")
            return False
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)
        boto3.client("s3", region_name=os.environ.get("AWS_REGION") or None).put_object(
            Bucket=bucket, Key=key, Body=buf.getvalue()
        )
        ok(f"Uploaded to s3://{bucket}/{key}  ({len(rows)} records)")
    else:
        out_dir = ROOT / "Parquet" / "urlhaus"
        out_dir.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, out_dir / "urlhaus.parquet", compression="snappy")
        ok(f"Written to {out_dir / 'urlhaus.parquet'}  ({len(rows)} records)")

    return True


# ── Stage 3: Silver layer ─────────────────────────────────────────────────────
def run_silver():
    header("Stage 3: Silver Layer  (raw → Parquet)")

    for script in PROCESSING_SCRIPTS:
        print(f"\n  Running {script.stem} ...")
        success, elapsed = run_script(script)
        if success:
            ok(f"{script.stem}  ({elapsed:.1f}s)")
        else:
            fail(f"{script.stem} failed after {elapsed:.1f}s — stopping")
            return False

    ok("All Silver scripts completed")
    return True


# ── Stage 4: Gold layer ───────────────────────────────────────────────────────
def run_gold():
    header("Stage 4: Gold Layer  (enrich + aggregate)")

    if not ENRICH_SCRIPT.exists():
        fail(f"Not found: {ENRICH_SCRIPT}")
        return False

    print("  Running enrich.py ...")
    success, elapsed = run_script(ENRICH_SCRIPT)
    if success:
        ok(f"Enrichment complete  ({elapsed:.1f}s)")
        return True
    fail(f"Enrichment failed after {elapsed:.1f}s")
    return False


# ── Stage 5: Validation ───────────────────────────────────────────────────────
def run_validation():
    header("Stage 5: Validation & Data Quality")

    try:
        import pyarrow.parquet as pq
        import pyarrow as pa
        import pandas as pd
    except ImportError as e:
        fail(f"Missing dependency: {e}")
        return False

    # Resolve output paths
    silver_uri = os.environ.get("SILVER_PARQUET_URI", "").strip().rstrip("/")
    parquet_root = ROOT / os.environ.get("PARQUET_OUTPUT_ROOT", "Parquet")

    def silver_path(name):
        return f"{silver_uri}/{name}" if silver_uri else str(parquet_root / name)

    def gold_path(name):
        gold_uri = os.environ.get("GOLD_PARQUET_URI", "").strip().rstrip("/")
        return f"{gold_uri}/{name}" if gold_uri else str(parquet_root / "gold" / name)

    datasets = {
        "auth":    silver_path("auth"),
        "dns":     silver_path("dns"),
        "flows":   silver_path("flows"),
        "proc":    silver_path("proc"),
        "urlhaus": silver_path("urlhaus"),
        "gold":    gold_path("user_activity_summary"),
    }

    print(f"\n  {'Dataset':<12} {'Files':<7} {'Records':>14}  {'Size MB':>9}  Status")
    print(f"  {'-' * 55}")

    all_ok = True
    for name, path_str in datasets.items():
        if path_str.startswith("s3"):
            print(f"  {name:<12} {'n/a':<7} {'(cloud)':>14}  {'n/a':>9}  SKIP")
            continue
        p = Path(path_str)
        if not p.exists():
            print(f"  {name:<12} {'-':<7} {'-':>14}  {'-':>9}  MISSING")
            all_ok = False
            continue
        files = list(p.rglob("*.parquet"))
        if not files:
            print(f"  {name:<12} {0:<7} {0:>14,}  {0:>9.1f}  EMPTY")
            all_ok = False
            continue
        rows  = sum(_rowcount(f, pq) for f in files)
        mb    = sum(f.stat().st_size for f in files) / 1_048_576
        status = "OK" if rows > 0 else "EMPTY"
        if rows == 0:
            all_ok = False
        print(f"  {name:<12} {len(files):<7} {rows:>14,}  {mb:>9.1f}  {status}")

    # Sample rows — auth
    auth_p = Path(silver_path("auth"))
    _print_sample("auth", auth_p, pq, pd)

    # Sample rows — gold
    gold_p = Path(gold_path("user_activity_summary"))
    _print_sample("gold (user_activity_summary)", gold_p, pq, pd)

    # Summary statistics on gold
    if gold_p.exists():
        gold_files = list(gold_p.rglob("*.parquet"))[:10]
        if gold_files:
            try:
                df = pa.concat_tables([pq.read_table(str(f)) for f in gold_files]).to_pandas()
                print("\n  Gold layer summary:")
                print(df.select_dtypes("number").describe().to_string())
                if "malicious_hits" in df.columns:
                    hits = int(df["malicious_hits"].gt(0).sum())
                    print(f"\n  Rows with malicious hits : {hits}")
                if "user" in df.columns:
                    print(f"  Distinct users tracked   : {df['user'].nunique()}")
            except Exception as e:
                warn(f"Could not compute gold stats: {e}")

    return all_ok


def _rowcount(filepath, pq):
    try:
        return pq.read_metadata(str(filepath)).num_rows
    except Exception:
        return 0


def _print_sample(label, path, pq, pd):
    if not path.exists():
        return
    files = list(path.rglob("*.parquet"))
    if not files:
        return
    try:
        df = pq.read_table(str(files[0])).to_pandas().head(5)
        print(f"\n  Sample {label} (5 rows):")
        print(df.to_string(index=False))
    except Exception as e:
        warn(f"Could not read sample for {label}: {e}")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="End-to-end security event pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--skip-fetch",    action="store_true", help="Skip URLHaus API fetch")
    parser.add_argument("--skip-silver",   action="store_true", help="Skip Silver stage")
    parser.add_argument("--skip-gold",     action="store_true", help="Skip Gold stage")
    parser.add_argument("--validate-only", action="store_true", help="Run validation only")
    args = parser.parse_args()

    t_start = datetime.now()
    print(f"\n{BAR}")
    print(f"Unified Security Event Pipeline")
    print(f"Started: {t_start.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Root:    {ROOT}")
    print(BAR)

    # results maps stage name -> {"ok": bool, "elapsed": float}
    results = {}

    def timed(name, fn):
        t0 = time.time()
        ok_flag = fn()
        results[name] = {"ok": ok_flag, "elapsed": time.time() - t0}
        return ok_flag

    if not args.validate_only:
        # Stage 1 — URLHaus fetch
        if not args.skip_fetch:
            timed("1  URLHaus fetch  ", fetch_urlhaus)

        # Stage 2 — URLHaus to Parquet
        timed("2  URLHaus parquet", urlhaus_to_parquet)

        # Stage 3 — Silver
        if not args.skip_silver:
            if not timed("3  Silver layer   ", run_silver):
                fail("Silver failed — aborting")
                return 1

        # Stage 4 — Gold
        if not args.skip_gold:
            timed("4  Gold layer     ", run_gold)

    # Stage 5 — Validation
    timed("5  Validation     ", run_validation)

    # Summary
    total_elapsed = (datetime.now() - t_start).total_seconds()
    all_ok = all(v["ok"] for v in results.values())

    header(f"Pipeline {'Complete' if all_ok else 'Finished with warnings'}")
    print(f"  {'Stage':<22} {'Status':<8} {'Time':>10}")
    print(f"  {'-' * 42}")
    for stage, r in results.items():
        status  = "OK" if r["ok"] else "WARN"
        elapsed = r["elapsed"]
        mins, secs = divmod(elapsed, 60)
        time_str = f"{int(mins)}m {secs:.1f}s" if mins >= 1 else f"{secs:.1f}s"
        print(f"  {stage:<22} {status:<8} {time_str:>10}")
    print(f"  {'-' * 42}")
    total_mins, total_secs = divmod(total_elapsed, 60)
    total_str = f"{int(total_mins)}m {total_secs:.1f}s" if total_mins >= 1 else f"{total_secs:.1f}s"
    print(f"  {'TOTAL':<22} {'':8} {total_str:>10}")
    print(f"\n  Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
