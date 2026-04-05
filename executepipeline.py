#!/usr/bin/env python3
"""
Run all data processing pipelines in sequence.
This script executes all the individual processing scripts to convert raw data to Parquet format.
"""

import subprocess
import sys
from pathlib import Path

# Define the processing scripts to run
PROCESSING_SCRIPTS = [
    "src/processing/auth_to_parquet.py",
    "src/processing/dns_to_parquet.py",
    "src/processing/flows_to_parquet.py",
    "src/processing/proc_to_parquet.py",
]

def run_script(script_path):
    """Run a single processing script and return success status."""
    print(f"\n{'='*60}")
    print(f"Running: {script_path}")
    print(f"{'='*60}")

    try:
        result = subprocess.run([sys.executable, script_path], check=True)
        print(f"✓ Successfully completed: {script_path}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ Failed: {script_path} (exit code: {e.returncode})")
        return False

def main():
    """Run all processing scripts in sequence."""
    print("Starting data processing pipeline...")
    print(f"Project root: {Path(__file__).parent}")

    success_count = 0
    total_scripts = len(PROCESSING_SCRIPTS)

    for script in PROCESSING_SCRIPTS:
        if run_script(script):
            success_count += 1
        else:
            print(f"\nStopping pipeline due to failure in: {script}")
            break

    print(f"\n{'='*60}")
    print(f"Pipeline completed: {success_count}/{total_scripts} scripts successful")

    if success_count == total_scripts:
        print("✓ All processing scripts completed successfully!")
        return 0
    else:
        print("✗ Some scripts failed. Check the output above for details.")
        return 1

if __name__ == "__main__":
    sys.exit(main())