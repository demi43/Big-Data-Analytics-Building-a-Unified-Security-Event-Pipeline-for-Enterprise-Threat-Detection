## Unified Security Event Pipeline for Enterprise Threat Detection

** Big Data Analytics **

## Overview

This project implements a scalable distributed data pipeline designed to ingest, process, and enrich massive enterprise-scale security telemetry. By correlating the **LANL Unified Host and Network Dataset** (~700M events) with real-time threat intelligence feeds (URLHaus), the system enables the detection of sophisticated cyber threats such as lateral movement and compromised credentials.

## The Big Data Stack

The architecture is designed to handle the **Volume, Variety, and Velocity** of security logs by leveraging a layered distributed system:

| Layer          | Technology                     | Core Big Data Concept                                              |
| :------------- | :----------------------------- | :----------------------------------------------------------------- |
| **Storage**    | Amazon S3                      | Durable object storage, prefixes per layer, lifecycle & encryption |
| **Processing** | Apache Spark on AWS (e.g. EMR) | Parallel DAG execution; S3 as read/write data lake                 |
| **Data Store** | Apache HBase                   | Wide-column Model, LSM-Trees, & Column Families                    |
| **Syntax**     | Apache Parquet                 | Columnar Storage with Snappy Compression                           |
| **Querying**   | Spark SQL / DataFrames         | Distributed Execution & Partition-aware Querying                   |

LANL User-Computer Auth logs (CSV) [https://csr.lanl.gov/data/cyber1/],
LANL Unified Host/Network logs (CSV) [https://csr.lanl.gov/data/auth/], and
URLHaus API threat feeds (JSON) [https://urlhaus.abuse.ch/api/].

## Architecture

**Pipeline flow (current implementation):**

```text
  Data Sources                    Pipeline Stages                    Storage / Output
  ─────────────                   ───────────────                    ────────────────

  LANL (auth, dns,                ┌─────────────────┐
  flows, proc)  ─────────────────►│  Ingestion      │   Local: data/ + create_samples.py
  (file / API)                    │  (Bronze)       │   S3: s3a://bucket/bronze (planned)
                                  └────────┬────────┘
                                           │
  URLHaus (threat intel)  ────────►        ▼
  (JSON API)                      ┌─────────────────┐
                                  │  Normalization  │   Spark DataFrames → Parquet
                                  │  (Silver)       │   Local: Parquet/ subdirs
                                  └────────┬────────┘
                                           │
                                           ▼
                                  ┌─────────────────┐     ┌─────────────────┐
                                  │  Enrichment      │────►│  Storage         │   Local: enriched data
                                  │  (Gold)          │     │  (CSV/Parquet)   │   HBase: planned
                                  └─────────────────┘     └────────┬────────┘
                                                                   │
                                                                   ▼
                                  ┌─────────────────┐     ┌─────────────────┐
                                  │  Querying       │◄────│  Spark SQL /     │   Notebooks: threat hunting
                                  │  (Analytics)    │     │  DataFrames      │
                                  └─────────────────┘     └─────────────────┘
```

**Stack layers — implementation status:**

| Layer      | Technology (target)         | Status      | Notes                                                                                |
| ---------- | --------------------------- | ----------- | ------------------------------------------------------------------------------------ |
| Storage    | Amazon S3 (bucket prefixes) | Done        | Configured for bronze, silver, and gold layers; Parquet files uploaded to S3 bucket. |
| Syntax     | Parquet + Snappy            | Done        | Implemented for silver layer normalization; outputs to `Parquet/` subdirs.           |
| Processing | Apache Spark (e.g. EMR)     | Done        | PySpark in `requirements.txt`; jobs read/write local and S3 paths.                   |
| Data store | Apache HBase                | in progress | Schema in `src/stores/`; writes in later milestone.                                  |
| Querying   | Spark SQL / DataFrames      | Done        | Partition pruning, distributed execution; notebooks for analytics.                   |

Medallion layout (bronze / silver / gold) is unchanged; **bronze and silver/gold Parquet layers target S3** instead of HDFS. Paths use **`s3a://`** for Spark (or `s3://` in `.env`, rewritten to `s3a://` in `pipeline_paths`).

## Setup

- **Python:** 3.9+ recommended.
- **Java:** Install JDK 11 or 17 and make sure `JAVA_HOME` is set and `%JAVA_HOME%\bin` is on `PATH`.
- **Hadoop / HADOOP_HOME:** If Spark reports Hadoop missing in the environment, install a compatible Hadoop distribution and set `HADOOP_HOME` to the Hadoop root directory, then add `%HADOOP_HOME%\bin` to `PATH`.
- **Dependencies:** From the project root, run:
  ```bash
  pip install -r requirements.txt
  ```
- **Spark / PySpark:** `pyspark` in `requirements.txt` installs the Python Spark bindings and Spark runtime wheel, but Spark still requires a working Java runtime.
- **Optional:** Use a virtual environment (`python -m venv venv`, then activate and run the above).

### Quick local setup

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

If you want to run the pipeline scripts from a fresh clone, you should also:

- Create `.env` from `.env.example`:
  ```powershell
  copy .env.example .env
  ```
- Fill in any required values for your environment.

## Environment

- Copy `.env.example` to `.env` and set `URLHAUS_API_KEY` for the URLHaus fetch script (free key at [auth.abuse.ch](https://auth.abuse.ch/)). Sample generation from local files does not require any keys.
- Do **not** commit `.env` or real credentials; `.env` is listed in `.gitignore`.
- **Pipeline paths:** Ingestion and `*_to_parquet.py` scripts load `.env` via `src/pipeline_paths.py`. Optional variables include per-dataset bronze URIs (`AUTH_INPUT_URI`, `DNS_INPUT_URI`, `FLOWS_INPUT_URI`, `PROC_INPUT_URI`), silver output (`SILVER_PARQUET_URI` or `PARQUET_OUTPUT_ROOT`), Spark tuning (`SPARK_MASTER`, `SPARK_DRIVER_MEMORY`, …), and URLHaus S3 upload (`S3_URLHAUS_BUCKET`, `S3_URLHAUS_KEY`, `AWS_REGION`). When unset, inputs default to `DATA_DIR` (default `data/`) and Parquet output to `Parquet/<dataset>/` under the project root.

## Common setup issues

- `java` not found: install JDK 11 or 17 and verify `JAVA_HOME` is set and `%JAVA_HOME%\bin` is on `PATH`.
- PySpark import or Spark session failure: make sure `pyspark` is installed and Java is available; the repo uses Spark through `pyspark`, not plain pandas.
- `.env` missing: copy `.env.example` to `.env` before running scripts that rely on environment variables.
- Missing data files: this repo does not commit raw LANL input data in `data/`; place your input files in `data/` or use the sample data generation script.
- S3 / URLHaus access: if you use cloud paths or URLHaus scripts, ensure valid AWS credentials and `URLHAUS_API_KEY` are configured.

## Project Structure

```text
/
├── data/                   # Raw/compressed LANL-style files (not committed; place *.gz, *.bz2 here)
├── sample data/            # Generated CSV samples (committed): auth_sample.txt, dns_sample.txt, etc.
├── scripts/                # Standalone runnable scripts
│   ├── fetch_urlhaus.py    # Fetch recent URLs from URLHaus API → sample data/urlhaus_sample.json
│   ├── create_samples.py   # Build samples from data/ → sample data/
│   └── urlhaus_to_parquet.py  # Example: DataFrame → Parquet → S3 (boto3; set S3_URLHAUS_BUCKET in .env)
├── src/
│   ├── pipeline_paths.py   # Resolve bronze/silver paths; normalizes s3:// → s3a:// for Spark
│   ├── spark_bootstrap.py  # SparkSession builder (Windows PySpark fix, optional S3A JARs)
│   ├── ingestion/          # PySpark read/bronze validation (local or S3 inputs)
│   │   ├── ingest_auth_logs.py
│   │   ├── ingest_dns.py
│   │   ├── ingest_flows.py
│   │   └── ingest_proc.py
│   ├── processing/         # LANL → Parquet silver
│   │   ├── auth_to_parquet.py
│   │   ├── dns_to_parquet.py
│   │   ├── flows_to_parquet.py
│   │   └── proc_to_parquet.py
│   ├── scripts/            # Enrichment and other scripts
│   │   └── enrich.py       # Gold layer enrichment with threat intelligence
│   └── stores/             # HBase schema and write operations (planned)
├── docs/                   # Architecture diagrams and milestone PDF reports
├── notebooks/              # Jupyter notebooks for threat hunting
│   └── threat_hunting.ipynb
├── Parquet/                # Silver layer Parquet outputs
│   ├── auth/
│   ├── dns/
│   ├── flows/
│   └── proc/
├── .env.example            # Template for environment variables (copy to .env)
├── .gitignore              # Excludes large data, .env, venv, Spark temp files
├── .gitattributes         # Line-ending normalization
├── requirements.txt        # Python dependencies (pyspark, pandas, requests, etc.)
├── README.md               # This file
├── documentation.tex       # LaTeX documentation compiled from README and notes
├── notes.txt               # Technical notes and troubleshooting
└── executepipeline.py      # Pipeline execution script
```

## Storage

- **Raw / compressed data:** Place LANL-style files (e.g. `dns.txt.gz`, `flows.txt.gz`, `proc.txt.gz`, `lanl-auth-dataset-1.bz2`) in `data/`. These are not committed (see `.gitignore`).
- **Sample data:** Generated samples in `sample data/`: LANL-style CSVs (`auth_sample.txt`, `dns_sample.txt`, `flows_sample.txt`, `proc_sample.txt`) and URLHaus JSON (`urlhaus_sample.json`) from `scripts/fetch_urlhaus.py`. These are committed for pipeline testing and M2 evidence.
- **Production (S3):** A single bucket can hold **bronze/** (raw), **silver/** (Parquet), and **gold/** (enriched or serving-ready) — the same layout as in the AWS console. In `.env`, use **`s3a://`** for Spark jobs (or `s3://`; `pipeline_paths` rewrites to `s3a://`). Upload with `aws s3 cp` / the console.

### Amazon S3 quick reference

1. **Create a bucket** in the same Region as your Spark cluster (e.g. EMR).
2. **Upload** raw files under a bronze prefix, e.g. `s3a://your-bucket/bronze/lanl-auth-dataset-1.bz2` in Spark (same bucket keys as in the S3 console).
3. **IAM:** Grant the cluster instance profile (or your laptop principal for tests) `s3:ListBucket` on the bucket and `s3:GetObject` / `s3:PutObject` on the relevant object prefixes.
4. **Spark on EMR:** `s3://` or `s3a://` both work; credentials come from the instance profile. Local PySpark: prefer **`s3a://`** (this repo normalizes `s3://` → `s3a://` when resolving `.env`).
5. **Local PySpark + S3:** Often needs **`s3a://`** plus **`SPARK_JARS_PACKAGES`** (Hadoop AWS + AWS SDK bundle versions matched to your Spark/Hadoop build). Configure credentials via the [default AWS credential chain](https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html) (`aws configure`, env vars, or SSO). See also [EMR and file systems](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-plan-file-systems.html).

## Data Pipeline Workflow

Ingestion (Bronze): Raw LANL logs are landed in **S3** under a bronze prefix (future full run); for local development they are ingested directly from `data/` using PySpark.
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
- **Ingest LANL auth dataset (PySpark):** Ensure `data/lanl-auth-dataset-1.bz2` is present, then from project root:

  ```bash
  # PySpark via Python
  python src/ingestion/ingest_auth_logs.py

  # or, using spark-submit if Spark is installed system-wide
  spark-submit src/ingestion/ingest_auth_logs.py
  ```

  This script reads the compressed auth file with a simple schema:
  - `time` (integer, event index)
  - `source_user` (string)
  - `source_computer` (string)

  Example output:

  ```text
  root
   |-- time: integer (nullable = true)
   |-- source_user: string (nullable = true)
   |-- source_computer: string (nullable = true)

  +----+-----------+---------------+
  |time|source_user|source_computer|
  +----+-----------+---------------+
  |1   |U1         |C1             |
  |1   |U1         |C2             |
  |2   |U2         |C3             |
  |3   |U3         |C4             |
  |6   |U4         |C5             |
  |7   |U4         |C5             |
  |7   |U5         |C6             |
  |8   |U6         |C7             |
  |11  |U7         |C8             |
  |12  |U8         |C9             |
  +----+-----------+---------------+
  only showing top 10 rows

  Total rows: 708304516
  Partitions: 18
  ```

  This demonstrates that the full LANL auth dataset (~708M rows) can be ingested and distributed across Spark partitions for downstream processing.

- **Ingest LANL DNS dataset (PySpark):** Ensure `data/dns.txt.gz` is present, then from project root:

  ```bash
  # PySpark via Python
  python src/ingestion/ingest_dns.py

  # or, using spark-submit if Spark is installed system-wide
  spark-submit src/ingestion/ingest_dns.py
  ```

  This script reads the compressed DNS file with a simple schema:
  - `time` (integer, event time index)
  - `SourceComputer` (string, source computer making the DNS query)
  - `ComputerResolved` (string, de-identified computer/hostname resolved by DNS)

  Example output:

  ```text
  root
   |-- time: integer (nullable = true)
   |-- SourceComputer: string (nullable = true)
   |-- ComputerResolved: string (nullable = true)

  +----+--------------+----------------+
  |time|SourceComputer|ComputerResolved|
  +----+--------------+----------------+
  |2   |C4653         |C5030           |
  |2   |C5782         |C16712          |
  |6   |C1191         |C419            |
  |15  |C3380         |C22841          |
  |18  |C2436         |C5030           |
  |31  |C161          |C2109           |
  |35  |C5642         |C528            |
  |38  |C3380         |C22841          |
  |42  |C2428         |C1065           |
  |42  |C2428         |C2109           |
  +----+--------------+----------------+
  only showing top 10 rows

  Total rows: 40821591
  Partitions: 1
  ```

  This demonstrates that the full LANL DNS dataset (~40.8M rows) can be ingested and distributed across Spark partitions for downstream processing.

- **Ingest LANL flows dataset (PySpark):** Ensure `data/flows.txt.gz` is present, then from project root:

  ```bash
  # PySpark via Python
  python src/ingestion/ingest_flows.py

  # or, using spark-submit if Spark is installed system-wide
  spark-submit src/ingestion/ingest_flows.py
  ```

  This script reads the compressed flows file with a schema:
  - `time` (integer)
  - `duration` (integer)
  - `src_computer` (string)
  - `src_port` (string)
  - `dst_computer` (string)
  - `dst_port` (string)
  - `protocol` (string)
  - `packets_count` (integer)
  - `bytes_count` (integer)

  Example output:

  ```text
  root
   |-- time: integer (nullable = true)
   |-- duration: integer (nullable = true)
   |-- src_computer: string (nullable = true)
   |-- src_port: string (nullable = true)
   |-- dst_computer: string (nullable = true)
   |-- dst_port: string (nullable = true)
   |-- protocol: string (nullable = true)
   |-- packets_count: integer (nullable = true)
   |-- bytes_count: integer (nullable = true)

  +----+--------+------------+--------+------------+--------+--------+-------------+-----------+
  |time|duration|src_computer|src_port|dst_computer|dst_port|protocol|packets_count|bytes_count|
  +----+--------+------------+--------+------------+--------+--------+-------------+-----------+
  |1   |0       |C1065       |389     |C3799       |N10451  |6       |10           |5323       |
  |1   |0       |C1423       |N1136   |C1707       |N1      |6       |5            |847        |
  ...  |...     |...         |...     |...         |...     |...     |...          |...        |
  +----+--------+------------+--------+------------+--------+--------+-------------+-----------+
  only showing top 10 rows
  ```

  This demonstrates that the LANL flows dataset can be ingested and structured for downstream processing.

- **Ingest LANL proc dataset (PySpark):** Ensure `data/proc.txt.gz` is present, then from project root:

  ```bash
  # PySpark via Python
  python src/ingestion/ingest_proc.py

  # or, using spark-submit if Spark is installed system-wide
  spark-submit src/ingestion/ingest_proc.py
  ```

  This script reads the compressed proc file with a schema:
  - `time` (integer)
  - `user@domain` (string)
  - `computer` (string)
  - `processname` (string)
  - `Start/End` (string)

  Example output:

  ```text
  root
   |-- time: integer (nullable = true)
   |-- user@domain: string (nullable = true)
   |-- computer: string (nullable = true)
   |-- processname: string (nullable = true)
   |-- Start/End: string (nullable = true)

  +----+-----------+--------+-----------+---------+
  |time|user@domain|computer|processname|Start/End|
  +----+-----------+--------+-----------+---------+
  |1   |C1$@DOM1   |C1      |P16        |Start    |
  |1   |C1001$@DOM1|C1001   |P4         |Start    |
  |1   |C1002$@DOM1|C1002   |P4         |Start    |
  |1   |C1004$@DOM1|C1004   |P4         |Start    |
  |1   |C1017$@DOM1|C1017   |P4         |Start    |
  |1   |C1018$@DOM1|C1018   |P4         |Start    |
  |1   |C1020$@DOM1|C1020   |P3         |Start    |
  |1   |C1020$@DOM1|C1020   |P4         |Start    |
  |1   |C1028$@DOM1|C1028   |P16        |End      |
  |1   |C1029$@DOM1|C1029   |P4         |Start    |
  +----+-----------+--------+-----------+---------+
  only showing top 10 rows

  Total rows: 426045096
  Partitions: 1
  ```

  This demonstrates that the LANL proc dataset (~426M process events) can be ingested and structured for downstream processing.

- **Write LANL silver (Parquet):** With the same raw files (or S3 URIs in `.env`), from project root:

  ```bash
  python src/processing/auth_to_parquet.py
  python src/processing/dns_to_parquet.py
  python src/processing/flows_to_parquet.py
  python src/processing/proc_to_parquet.py
  ```

  Outputs go to `Parquet/auth`, `Parquet/dns`, etc., unless `SILVER_PARQUET_URI` is set (e.g. `s3a://your-bucket/silver` → `s3a://your-bucket/silver/auth`, …).

- **URLHaus rows → S3 Parquet (boto3 example):** Set `S3_URLHAUS_BUCKET` (and optionally `S3_URLHAUS_KEY`, `AWS_REGION`) in `.env`, ensure AWS credentials are available, then run `python scripts/urlhaus_to_parquet.py`.

- **Enrichment (Gold layer):** Correlate LANL data with URLHaus threat intelligence. Ensure Parquet silver layers exist, then from project root:

  ```bash
  python src/scripts/enrich.py
  ```

  This script performs a broadcast hash join to enrich events with threat indicators, outputting enriched data for analysis.

- **Full pipeline (planned):** Additional ingestion jobs (e.g. `load_lanl.py`), processing via `src/processing/enrich_logs.py`, and analysis in `notebooks/` — to be wired in later milestones.

## Current status

| Component                      | Status                                                                      |
| ------------------------------ | --------------------------------------------------------------------------- |
| Data acquisition (URLHaus API) | Done (`scripts/fetch_urlhaus.py`, output `sample data/urlhaus_sample.json`) |
| Sample data generation         | Done (`scripts/create_samples.py`, output in `sample data/`)                |
| Persistent storage             | Done (sample data and raw data paths documented above)                      |
| LANL auth ingestion (PySpark)  | Done (`src/ingestion/ingest_auth_logs.py`, ~708M rows, 18 partitions)       |
| LANL DNS ingestion (PySpark)   | Done (`src/ingestion/ingest_dns.py`, ~40.8M rows, 1 partition)              |
| LANL flows ingestion (PySpark) | Done (`src/ingestion/ingest_flows.py`, reads `data/flows.txt.gz`)           |
| LANL proc ingestion (PySpark)  | Done (`src/ingestion/ingest_proc.py`, ~426M rows, 1 partition)              |
| LANL → Parquet (silver)        | Done (`src/processing/*_to_parquet.py`; paths configurable via `.env`)      |
| Enrichment (gold)              | Done (`src/scripts/enrich.py`; correlates with URLHaus)                     |
| Pipeline orchestration         | Done (M2)                                                                   |
| Processing / enrichment        | Done (M3)                                                                   |
| Analytics / querying           | Done (notebooks for threat hunting)                                         |

## Pipeline Execution Sequence

The pipeline follows a medallion architecture: Bronze (raw ingestion), Silver (normalized Parquet), Gold (enriched analytics-ready data). Here's the logical sequence to run the full pipeline:

1. **Data Preparation**
   - Ensure raw LANL files are in `data/` (e.g., `lanl-auth-dataset-1.bz2`, `dns.txt.gz`, etc.)
   - Fetch threat intelligence: `python scripts/fetch_urlhaus.py`
   - (Optional) Generate samples: `python scripts/create_samples.py`

2. **Bronze Layer: Ingestion**
   - Load LANL auth data: `python src/ingestion/ingest_auth_logs.py`
   - Load LANL DNS data: `python src/ingestion/ingest_dns.py`
   - Load LANL flows data: `python src/ingestion/ingest_flows.py`
   - Load LANL proc data: `python src/ingestion/ingest_proc.py`

3. **Silver Layer: Normalization**
   - Convert auth to Parquet: `python src/processing/auth_to_parquet.py`
   - Convert DNS to Parquet: `python src/processing/dns_to_parquet.py`
   - Convert flows to Parquet: `python src/processing/flows_to_parquet.py`
   - Convert proc to Parquet: `python src/processing/proc_to_parquet.py`

4. **Gold Layer: Enrichment**
   - Correlate with threats: `python src/scripts/enrich.py`

5. **Analytics: Querying**
   - Open `notebooks/threat_hunting.ipynb` for analysis and threat detection queries.

**Notes:**

- Each step depends on the previous; ensure DataFrames are created before Parquet writes, and Parquet exists before enrichment.
- For large datasets, run on a cluster (e.g., EMR) with S3 paths configured in `.env`.
- Monitor logs for errors; refer to `notes.txt` for troubleshooting.

Metrics for Success
Throughput: Total events processed per second.
Latency: Total execution time for a 58-day log batch.
Data Fidelity: Percentage of records successfully correlated with threat intel feeds.
Author: Olaoluwa Adedamola Omodemi
Institution: Kennesaw State University
Department: College of Computing and Software Engineering
Course: CS 4265 - Big Data Analytics
Professor: Christopher Reagan
