# Big-Data-Analytics-Building-a-Unified-Security-Event-Pipeline-for-Enterprise-Threat-Detection
# Unified Security Event Pipeline for Enterprise Threat Detection

## Overview
This project builds a scalable Big Data pipeline to detect cyber threats (lateral movement and compromised credentials) within enterprise-scale telemetry. We utilize the **LANL Unified Host and Network Dataset** (700M+ events) and enrich it with external threat intelligence.

## The Big Data Stack
* **Storage:** HDFS (Distributed block storage)
* **Processing:** Apache Spark on YARN (Parallel execution)
* **Data Store:** Apache HBase (Wide-column store for enriched logs)
* **Syntax:** Apache Parquet (Columnar encoding)
* **Querying:** Spark SQL & PySpark DataFrames

## Project Structure
* `/src`: Contains PySpark jobs for data normalization and enrichment.
* `/docs`: Architecture diagrams and milestone documentation.
* `/notebooks`: Analytics and visualization of threat detection results.

## Getting Started
1. **Data Ingestion:** Run `src/ingestion/load_lanl.py` to move raw logs into HDFS.
2. **Processing:** Execute `src/processing/enrich_logs.py` to perform the Broadcast Join with threat feeds.
3. **Analysis:** Open `notebooks/threat_hunting.ipynb` to query the enriched HBase table.

## Author
* **Olaoluwa Adedamola Omodemi** - Kennesaw State University
