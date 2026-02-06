# Unified Security Event Pipeline for Enterprise Threat Detection
**CS 4265: Big Data Analytics — Milestone 1 Submission**

## 📝 Overview
This project implements a scalable distributed data pipeline designed to ingest, process, and enrich massive enterprise-scale security telemetry. By correlating the **LANL Unified Host and Network Dataset** (~700M events) with real-time threat intelligence feeds (URLHaus), the system enables the detection of sophisticated cyber threats such as lateral movement and compromised credentials.

## 🛠 The Big Data Stack
The architecture is designed to handle the **Volume, Variety, and Velocity** of security logs by leveraging a layered distributed system:

| Layer | Technology | Core Big Data Concept |
| :--- | :--- | :--- |
| **Storage** | HDFS | Distributed Block Storage (128MB) & Replication Factor 3 |
| **Processing** | Apache Spark on YARN | Parallel DAG Execution, Map-Shuffle-Reduce Optimization |
| **Data Store** | Apache HBase | Wide-column Model, LSM-Trees, & Column Families |
| **Syntax** | Apache Parquet | Columnar Storage with Snappy Compression |
| **Querying** | Spark SQL / DataFrames | Distributed Execution & Partition-aware Querying |

## 📂 Project Structure
```text
/
├── data/               # Metadata and small threat feed samples (Local)
├── docs/               # Architecture diagrams and Milestone PDF reports
├── notebooks/          # Jupyter notebooks for threat hunting and visualization
├── src/                # Core pipeline source code
│   ├── ingestion/      # PySpark jobs for moving raw logs into HDFS
│   ├── processing/     # Normalization, Broadcast Joins, and Enrichment logic
│   └── stores/         # HBase schema definitions and write operations
├── .gitignore          # Excludes large logs and temporary Spark files
└── README.md           # Project overview and documentation
