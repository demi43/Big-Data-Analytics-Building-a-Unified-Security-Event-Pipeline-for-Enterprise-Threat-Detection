# Unified Security Event Pipeline for Enterprise Threat Detection

** Big Data Analytics **

## Overview

This project implements a scalable distributed data pipeline designed to ingest, process, and enrich massive enterprise-scale security telemetry. By correlating the **LANL Unified Host and Network Dataset** (~700M events) with real-time threat intelligence feeds (URLHaus), the system enables the detection of sophisticated cyber threats such as lateral movement and compromised credentials.

## The Big Data Stack

The architecture is designed to handle the **Volume, Variety, and Velocity** of security logs by leveraging a layered distributed system:

| Layer          | Technology             | Core Big Data Concept                                    |
| :------------- | :--------------------- | :------------------------------------------------------- |
| **Storage**    | HDFS                   | Distributed Block Storage (128MB) & Replication Factor 3 |
| **Processing** | Apache Spark on YARN   | Parallel DAG Execution, Map-Shuffle-Reduce Optimization  |
| **Data Store** | Apache HBase           | Wide-column Model, LSM-Trees, & Column Families          |
| **Syntax**     | Apache Parquet         | Columnar Storage with Snappy Compression                 |
| **Querying**   | Spark SQL / DataFrames | Distributed Execution & Partition-aware Querying         |

LANL User-Computer Auth logs (CSV) [https://csr.lanl.gov/data/cyber1/],
LANL Unified Host/Network logs (CSV) [https://csr.lanl.gov/data/auth/], and
URLHaus API threat feeds (JSON) [https://urlhaus.abuse.ch/api/].

## Architecture

**Pipeline flow (current vs. planned):**

```text
  Data Sources                    Pipeline Stages                    Storage / Output
  ─────────────                   ───────────────                    ────────────────

  LANL (auth, dns,                ┌─────────────────┐
  flows, proc)  ─────────────────►│  Ingestion      │   Planned: HDFS, load_lanl.py
  (file / API)                    │  (Bronze)       │  Current:  Local data/ + create_samples.py
                                  └────────┬────────┘
                                           │
  URLHaus (threat intel)  ────────►        ▼
  (JSON API)                      ┌─────────────────┐
                                  │  Normalization  │   Planned (M3): Spark DataFrames, Parquet
                                  │  (Silver)       │
                                  └────────┬────────┘
                                           │
                                           ▼
                                  ┌─────────────────┐     ┌─────────────────┐
                                  │  Enrichment      │────►│  Storage         │   Current: sample data/ (CSV)
                                  │  (Gold)          │     │  (indexed)       │   Planned: HBase, Parquet
                                  └─────────────────┘     └────────┬────────┘
                                                                   │
                                                                   ▼
                                  ┌─────────────────┐     ┌─────────────────┐
                                  │  Querying       │◄────│  Spark SQL /     │   Planned: notebooks, threat hunting
                                  │  (Analytics)    │     │  DataFrames      │
                                  └─────────────────┘     └─────────────────┘
```

**Stack layers — implementation status:**

| Layer      | Technology (target)       | M2 status | Notes                                                           |
| ---------- | ------------------------- | --------- | --------------------------------------------------------------- |
| Storage    | HDFS (128MB blocks, RF 3) | Planned   | Raw data currently in local `data/`; samples in `sample data/`. |
| Syntax     | Parquet + Snappy          | Planned   | To be used after normalization (M3).                            |
| Processing | Apache Spark on YARN      | Planned   | PySpark in `requirements.txt`; jobs to be added.                |
| Data store | Apache HBase              | Planned   | Schema in `src/stores/`; writes in later milestone.             |
| Querying   | Spark SQL / DataFrames    | Planned   | Partition pruning, distributed execution.                       |

No pivot from M1 design; current work is local proof-of-concept (samples, persistence, structure) before scaling to the full stack.

## Setup

- **Python:** 3.9+ recommended.
- **Dependencies:** From the project root, run:
  ```bash
  pip install -r requirements.txt
  ```
- **Optional:** Use a virtual environment (`python -m venv venv`, then activate and run the above).

## Environment

- Copy `.env.example` to `.env` and set `URLHAUS_API_KEY` for the URLHaus fetch script (free key at [auth.abuse.ch](https://auth.abuse.ch/)). Sample generation from local files does not require any keys.
- Do **not** commit `.env` or real credentials; `.env` is listed in `.gitignore`.

## Project Structure

```text
/
├── data/                   # Raw/compressed LANL-style files (not committed; place *.gz, *.bz2 here)
├── sample data/            # Generated CSV samples (committed): auth_sample.txt, dns_sample.txt, etc.
├── scripts/                # Standalone runnable scripts
│   ├── fetch_urlhaus.py    # Fetch recent URLs from URLHaus API → sample data/urlhaus_sample.json
│   └── create_samples.py   # Build samples from data/ → sample data/
├── src/                    # Core pipeline source code (planned)
│   ├── ingestion/          # PySpark jobs for moving raw logs into HDFS
│   ├── processing/         # Normalization, broadcast joins, enrichment
│   └── stores/             # HBase schema and write operations
├── docs/                   # Architecture diagrams and milestone PDF reports
├── notebooks/              # Jupyter notebooks for threat hunting (planned)
├── .env.example            # Template for environment variables (copy to .env)
├── .gitignore              # Excludes large data, .env, venv, Spark temp files
├── .gitattributes         # Line-ending normalization
├── requirements.txt        # Python dependencies (pyspark, pandas, requests, etc.)
└── README.md               # This file
```

## Storage

- **Raw / compressed data:** Place LANL-style files (e.g. `dns.txt.gz`, `flows.txt.gz`, `proc.txt.gz`, `lanl-auth-dataset-1.bz2`) in `data/`. These are not committed (see `.gitignore`).
- **Sample data:** Generated samples in `sample data/`: LANL-style CSVs (`auth_sample.txt`, `dns_sample.txt`, `flows_sample.txt`, `proc_sample.txt`) and URLHaus JSON (`urlhaus_sample.json`) from `scripts/fetch_urlhaus.py`. These are committed for pipeline testing and M2 evidence.

## Data Pipeline Workflow
Ingestion (Bronze): Raw CSV logs from Los Alamos National Lab (LANL) are loaded into HDFS blocks.
Normalization (Silver): Spark DataFrames enforce schema validation and normalize heterogeneous formats.
Enrichment (Gold): A Broadcast Hash Join correlates host activity with malicious URL feeds to identify Indicators of Compromise (IoCs).
Indexing: Enriched events are stored in HBase, utilizing a custom row-key design to prevent region hotspotting and enable low-latency lookups.

Creating data samples (local)
To build small CSV samples from the compressed data in `data/` (for local runs and notebooks), use:
```bash
python scripts/create_samples.py
```

Options: `--lines 10000` (default), `--random` for reservoir sampling, `--compress` to write `.gz`, `--seed 42` when using `--random`. Outputs go to `sample data/` (e.g. `dns_sample.txt`, `flows_sample.txt`, `proc_sample.txt`, `auth_sample.txt`).

## How to run

- **Fetch URLHaus threat feed (data acquisition):** Set `URLHAUS_API_KEY` in `.env` (get a free key at [auth.abuse.ch](https://auth.abuse.ch/)), then from project root:
  ```bash
  python scripts/fetch_urlhaus.py
  ```
  Saves `sample data/urlhaus_sample.json` (default 20 URLs). Use `--limit 50` for more. Console output and the saved file are evidence of working acquisition.
- **Create sample data (LANL-style local files):** From project root, run `python scripts/create_samples.py`. Use `--lines 1000` for a small sample suitable for git.
- **Full pipeline (planned):** Data ingestion via `src/ingestion/load_lanl.py`, processing via `src/processing/enrich_logs.py`, analysis in `notebooks/` — to be wired in later milestones.

## Current status

| Component                       | Status                                                                 |
| ------------------------------- | ---------------------------------------------------------------------- |
| Data acquisition (URLHaus API)  | Done (`scripts/fetch_urlhaus.py`, output `sample data/urlhaus_sample.json`) |
| Sample data generation          | Done (`scripts/create_samples.py`, output in `sample data/`)           |
| Persistent storage              | Done (sample data and raw data paths documented above)               |
| Pipeline orchestration          | Planned (M2)                                                           |
| Processing / enrichment         | Planned (M3)                                                           |

## Getting Started (full pipeline, future)

Data Ingestion: Execute src/ingestion/load_lanl.py to stage raw telemetry.
Pipeline Execution: Run src/processing/enrich_logs.py to trigger the distributed enrichment job.
Analysis: Utilize the notebooks in /notebooks to perform SQL-based threat hunting on the enriched HBase tables.

Metrics for Success
Throughput: Total events processed per second.
Latency: Total execution time for a 58-day log batch.
Data Fidelity: Percentage of records successfully correlated with threat intel feeds.
Author: Olaoluwa Adedamola Omodemi
Institution: Kennesaw State University
Department: College of Computing and Software Engineering
Course: CS 4265 - Big Data Analytics
Professor: Christopher Reagan
