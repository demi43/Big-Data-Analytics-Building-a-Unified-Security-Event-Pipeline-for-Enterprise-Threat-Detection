# Unified Security Event Pipeline for Enterprise Threat Detection

**CS 4265 — Big Data Analytics | Kennesaw State University**

## Overview

A scalable distributed data pipeline that ingests, processes, and enriches massive enterprise-scale security telemetry. By correlating the **LANL Unified Host and Network Dataset** (~700M events) with real-time threat intelligence from URLHaus, the system enables detection of sophisticated cyber threats such as lateral movement, compromised credentials, and connections to known malicious hosts.

---

## Quick Start

```powershell
# 1. Clone and set up the environment
py -3.12 -m venv venv312
.\venv312\Scripts\Activate.ps1
pip install -r requirements.txt

# 2. Configure credentials
copy .env.example .env
# Edit .env: set URLHAUS_API_KEY and (optionally) AWS credentials + S3 URIs

# 3. Place raw LANL data files in data/
#    lanl-auth-dataset-1.bz2, dns.txt.gz, flows.txt.gz, proc.txt.gz

# 4. Run the full pipeline
python executepipeline.py
```

---

## Running the Pipeline

`executepipeline.py` runs all five stages in sequence and prints a per-stage runtime summary at the end.

```powershell
python executepipeline.py                 # full pipeline
python executepipeline.py --skip-fetch    # use cached URLHaus data (no API call)
python executepipeline.py --skip-silver   # skip Silver stage (Parquet already exists)
python executepipeline.py --skip-gold     # skip Gold enrichment
python executepipeline.py --validate-only # validation and data quality report only
```

### Pipeline stages

| #   | Stage                 | Script(s)                        | Output                                 |
| --- | --------------------- | -------------------------------- | -------------------------------------- |
| 1   | **URLHaus fetch**     | `scripts/fetch_urlhaus.py`       | `sample data/urlhaus_sample.json`      |
| 2   | **URLHaus → Parquet** | inline (pandas + pyarrow)        | `Parquet/urlhaus/` or S3               |
| 3   | **Silver layer**      | `src/processing/*_to_parquet.py` | `Parquet/{auth,dns,flows,proc}/` or S3 |
| 4   | **Gold layer**        | `src/scripts/enrich.py`          | S3: `bucket/gold/user_activity_summary/` |
| 5   | **Validation**        | inline                           | record counts, sample rows, null rates |

### Example summary output

```
============================================================
Pipeline Complete
============================================================
  Stage                  Status         Time
  ------------------------------------------
  1  URLHaus fetch        OK            1.5s
  2  URLHaus parquet      OK            1.3s
  3  Silver layer         OK       29m 16.0s
  4  Gold layer           OK       10m 49.6s
  5  Validation           OK            8.8s
  ------------------------------------------
  TOTAL                            40m 17.2s

  Started: 11:06:54 | Finished: 11:47:11
```

---

## Architecture

```text
  Data Sources                    Pipeline Stages                    Output
  ────────────                    ───────────────                    ──────
                                  ┌─────────────────┐
  LANL (auth, dns,   ────────────►│  Bronze          │  data/*.bz2 / *.gz
  flows, proc)                    │  Raw ingestion   │  S3: bucket/bronze/
                                  └────────┬────────┘
                                           │
  URLHaus threat     ────────────►         ▼
  intel (JSON API)               ┌─────────────────┐
                                  │  Silver          │  Parquet/  (Snappy)
                                  │  Normalization  │  S3: bucket/silver/
                                  └────────┬────────┘
                                           │
                                           ▼
                                  ┌─────────────────┐
                                  │  Gold            │  S3: bucket/gold/
                                  │  Enrichment      │  user_activity_summary/
                                  └────────┬────────┘
                                           │
                                           ▼
                                  ┌─────────────────┐
                                  │  Analytics       │  notebooks/threat_hunting.ipynb
                                  │  Threat Hunting  │
                                  └─────────────────┘
```

### Technology stack

| Layer            | Technology              | Role                                                    |
| ---------------- | ----------------------- | ------------------------------------------------------- |
| **Storage**      | Amazon S3               | Durable object storage; bronze / silver / gold prefixes |
| **Processing**   | Apache Spark (PySpark)  | Parallel DAG execution; reads/writes local and S3       |
| **Format**       | Apache Parquet + Snappy | Columnar storage; predicate pushdown and compression    |
| **Threat Intel** | URLHaus API (abuse.ch)  | Real-time malicious URL feed                            |
| **Querying**     | Spark SQL / DataFrames  | Distributed execution, partition-aware queries          |
| **Analytics**    | Jupyter Notebooks       | Interactive threat hunting                              |

### Medallion layout

- **Bronze** — raw compressed LANL files (`data/`) or S3 `bucket/bronze/`
- **Silver** — schema-validated Parquet (`Parquet/` or S3 `bucket/silver/`)
- **Gold** — enriched aggregations joined with URLHaus IoCs (S3 `bucket/gold/`)

---

## Data Sources

| Dataset    | Format     | Size         | Description                         |
| ---------- | ---------- | ------------ | ----------------------------------- |
| LANL Auth  | `.bz2` CSV | ~708M rows   | User–computer authentication events |
| LANL DNS   | `.gz` CSV  | ~40.8M rows  | DNS resolution queries              |
| LANL Flows | `.gz` CSV  | —            | Network flow records (9 fields)     |
| LANL Proc  | `.gz` CSV  | ~426M rows   | Process start/end events            |
| URLHaus    | JSON API   | up to 1,000 URLs | Live malicious URL and host feed    |

LANL dataset: <https://csr.lanl.gov/data/cyber1/>  
URLHaus API: <https://urlhaus.abuse.ch/api/>

---

## Setup

### Requirements

- Python 3.12 (recommended; 3.13 also works — avoid pre-release versions)
- Java 11 or 17 (`JAVA_HOME` set, `%JAVA_HOME%\bin` on `PATH`)
- Hadoop (optional on Windows; set `HADOOP_HOME` if Spark reports it missing)

### Install dependencies

```powershell
pip install -r requirements.txt
```

### Configure environment

```powershell
copy .env.example .env
```

Key variables in `.env`:

| Variable                                      | Required       | Description                                           |
| --------------------------------------------- | -------------- | ----------------------------------------------------- |
| `URLHAUS_API_KEY`                             | For live fetch | Free key from [auth.abuse.ch](https://auth.abuse.ch/) |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | For S3         | AWS credentials                                       |
| `AWS_REGION`                                  | For S3         | e.g. `us-east-2`                                      |
| `AUTH_INPUT_URI` etc.                         | For S3 input   | `s3a://bucket/bronze/lanl-auth-dataset-1.bz2`         |
| `SILVER_PARQUET_URI`                          | For S3 output  | `s3a://bucket/silver`                                 |
| `S3_URLHAUS_BUCKET`                           | For S3 upload  | Bucket name for URLHaus Parquet                       |

When S3 variables are **not set**, the pipeline reads from `data/` and writes to `Parquet/` locally.

### Place raw data

Put LANL compressed files in the `data/` directory:

```
data/
├── lanl-auth-dataset-1.bz2
├── dns.txt.gz
├── flows.txt.gz
└── proc.txt.gz
```

These files are excluded from git (see `.gitignore`). Sample data for testing is committed under `sample data/`.

---

## Project Structure

```
/
├── executepipeline.py          # End-to-end pipeline runner (all 5 stages)
├── requirements.txt            # Pinned Python dependencies
├── LICENSE                     # MIT License
├── .env.example                # Credential and path template (copy to .env)
├── .gitignore
│
├── data/                       # Raw compressed LANL files (not committed)
├── sample data/                # Small samples committed for testing
│   ├── auth_sample.txt
│   ├── dns_sample.txt
│   ├── flows_sample.txt
│   ├── proc_sample.txt
│   └── urlhaus_sample.json
│
├── src/
│   ├── pipeline_paths.py       # Resolve bronze/silver/gold paths; s3:// → s3a://
│   ├── spark_bootstrap.py      # SparkSession factory (Windows + S3A fixes)
│   ├── ingestion/              # Bronze layer — read and validate raw data
│   │   ├── ingest_auth_logs.py
│   │   ├── ingest_dns.py
│   │   ├── ingest_flows.py
│   │   └── ingest_proc.py
│   ├── processing/             # Silver layer — CSV/bz2 → Parquet
│   │   ├── auth_to_parquet.py
│   │   ├── dns_to_parquet.py
│   │   ├── flows_to_parquet.py
│   │   └── proc_to_parquet.py
│   └── scripts/
│       └── enrich.py           # Gold layer — join flows + auth + proc + URLHaus IoCs
│
├── scripts/
│   ├── fetch_urlhaus.py        # Fetch threat intel from URLHaus API
│   ├── create_samples.py       # Build small CSV samples from compressed data
│   └── urlhaus_to_parquet.py   # Upload URLHaus JSON to S3 as Parquet (boto3)
│
├── Parquet/                    # Local fallback outputs (when S3 URIs are unset)
│   ├── auth/
│   ├── dns/
│   ├── flows/
│   ├── proc/
│   ├── urlhaus/
│   └── gold/user_activity_summary/
│
├── notebooks/
│   └── threat_hunting.ipynb        # Interactive threat detection queries
│
└── docs/
    ├── validation.md               # Data quality report, test cases, performance
    ├── data_dictionary.md          # Schema for all Bronze, Silver, and Gold datasets
    ├── CS4265_Olaoluwa_Omodemi_M4.tex  # Final report (LaTeX source)
    ├── CS42665_Olaoluwa_Omodemi_M1.pdf
    ├── CS4265_Olaoluwa_Omodemi_M2.pdf
    └── CS4265_Olaoluwa_Omodemi_M3.pdf
```

---

## Gold Layer Output Schema

The Gold layer produces `user_activity_summary` — one row per (user, computer, 1-hour bucket):

| Column             | Type   | Description                            |
| ------------------ | ------ | -------------------------------------- |
| `time_bucket`      | long   | Unix timestamp floored to the hour     |
| `user`             | string | Anonymized user from auth logs         |
| `src_computer`     | string | Source computer                        |
| `total_flows`      | long   | Network flow count in the bucket       |
| `total_bytes`      | long   | Total bytes transferred                |
| `total_packets`    | long   | Total packets transferred              |
| `active_processes` | long   | Distinct processes observed            |
| `malicious_hits`   | long   | Flows to URLHaus-listed hosts (0 or 1) |

---

## Generating Sample Data

To build small CSV samples from the compressed LANL files (useful for local testing):

```bash
python scripts/create_samples.py
python scripts/create_samples.py --lines 1000 --random --seed 42
```

Output goes to `sample data/`. The `--random` flag uses reservoir sampling for a representative subset.

---

## Running Individual Stages

If you need to run a single stage rather than the full pipeline:

```bash
# Threat intel
python scripts/fetch_urlhaus.py --limit 50

# Silver layer
python src/processing/auth_to_parquet.py
python src/processing/dns_to_parquet.py
python src/processing/flows_to_parquet.py
python src/processing/proc_to_parquet.py

# Gold layer
python src/scripts/enrich.py

# Ingestion (validation / exploration only)
python src/ingestion/ingest_auth_logs.py
```

---

## Common Issues

| Problem                        | Fix                                                                                            |
| ------------------------------ | ---------------------------------------------------------------------------------------------- |
| `java` not found               | Install JDK 11 or 17; set `JAVA_HOME` and add `%JAVA_HOME%\bin` to `PATH`                      |
| Spark session fails on Windows | Set `HADOOP_HOME=C:\hadoop` (no trailing `\bin`) and add `%HADOOP_HOME%\bin` to `PATH`         |
| `ModuleNotFoundError: dotenv`  | Run `pip install -r requirements.txt` inside your venv                                         |
| Missing data files             | Place LANL `.bz2` / `.gz` files in `data/`, or set S3 input URIs in `.env`                     |
| S3 access denied               | Verify AWS credentials in `.env`; ensure the IAM principal has `s3:GetObject` + `s3:PutObject` |
| URLHaus fetch fails            | Check `URLHAUS_API_KEY` in `.env`; the pipeline continues with cached `urlhaus_sample.json`    |
| Vectored I/O error             | Already handled in `spark_bootstrap.py`; clear `C:\tmp\spark\*` and retry                      |

---

## Project Status

| Component                                     | Status |
| --------------------------------------------- | ------ |
| Data acquisition (URLHaus API)                | Done   |
| Sample data generation                        | Done   |
| LANL auth ingestion (~708M rows)              | Done   |
| LANL DNS ingestion (~40.8M rows)              | Done   |
| LANL flows ingestion                          | Done   |
| LANL proc ingestion (~426M rows)              | Done   |
| Silver layer (Parquet, Snappy)                | Done   |
| Gold layer (enrichment + aggregation)         | Done   |
| S3 integration (bronze / silver / gold)       | Done   |
| Pipeline orchestration (`executepipeline.py`) | Done   |
| Per-stage runtime tracking                    | Done   |
| Analytics notebooks                           | Done   |

---

## Author

**Olaoluwa Adedamola Omodemi**  
Kennesaw State University — College of Computing and Software Engineering  
CS 4265 — Big Data Analytics | Professor Christopher Reagan
