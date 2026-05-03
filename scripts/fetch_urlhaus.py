#!/usr/bin/env python3
"""
Fetch a small sample of recent malware URLs from the URLHaus API (abuse.ch).

Requires URLHAUS_API_KEY in the environment (e.g. from .env). Get a free key at:
https://auth.abuse.ch/

Saves JSON to sample data/ for use in the pipeline and as M2 evidence.

Usage:
  python scripts/fetch_urlhaus.py
  python scripts/fetch_urlhaus.py --limit 50
"""

import argparse
import json
import os
import sys

# Load .env if present (optional)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_OUTPUT_DIR = os.path.join(PROJECT_ROOT, "sample data")
API_BASE = "https://urlhaus-api.abuse.ch/v1/urls/recent"


def main():
    parser = argparse.ArgumentParser(description="Fetch recent URLs from URLHaus API.")
    parser.add_argument(
        "--limit",
        type=int,
        default=1000,
        help="Max number of URLs to fetch (1–1000, default 1000)",
    )
    parser.add_argument(
        "--out",
        default=os.path.join(DEFAULT_OUTPUT_DIR, "urlhaus_sample.json"),
        help="Output JSON file path",
    )
    args = parser.parse_args()

    key = os.environ.get("URLHAUS_API_KEY")
    if not key or not key.strip():
        print("Error: URLHAUS_API_KEY not set. Add it to .env or set the environment variable.", file=sys.stderr)
        print("Get a free key at https://auth.abuse.ch/", file=sys.stderr)
        sys.exit(1)

    limit = max(1, min(1000, args.limit))
    url = f"{API_BASE}/limit/{limit}/"

    try:
        import requests
    except ImportError:
        print("Error: requests not installed. Run: pip install -r requirements.txt", file=sys.stderr)
        sys.exit(1)

    headers = {"Auth-Key": key.strip()}
    try:
        r = requests.get(url, headers=headers, timeout=30)
        r.raise_for_status()
        data = r.json()
    except requests.RequestException as e:
        print(f"Error: API request failed: {e}", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON response: {e}", file=sys.stderr)
        sys.exit(1)

    status = data.get("query_status", "")
    if status != "ok":
        print(f"Error: API returned query_status={status}", file=sys.stderr)
        sys.exit(1)

    urls = data.get("urls", [])
    n = len(urls)

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"Success: retrieved {n} URLs")
    print(f"Saved to: {args.out}")
    if n > 0:
        print("First few entries:")
        for i, entry in enumerate(urls[:5], 1):
            print(f"  {i}. {entry.get('url', '')} ({entry.get('threat', '')})")
    return 0


if __name__ == "__main__":
    sys.exit(main() or 0)
