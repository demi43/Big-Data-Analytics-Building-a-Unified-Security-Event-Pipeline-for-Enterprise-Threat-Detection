Unified Security Event Pipeline for Enterprise Threat Detection
Overview
This project builds a scalable Big Data pipeline to detect cyber threats (lateral movement and compromised credentials) within enterprise-scale telemetry. We utilize the LANL Unified Host and Network Dataset (700M+ events) and enrich it with external threat intelligence to identify Indicators of Compromise (IoCs).
The Big Data Stack
Storage: HDFS (Distributed block storage with Replication Factor 3)
Processing: Apache Spark on YARN (Parallel execution with Shuffle/Reduce optimization)
Data Store: Apache HBase (Wide-column store for low-latency indexed lookups)
Syntax: Parquet (Processed columnar storage) and CSV/JSON (Raw ingestion)
Querying: Spark SQL & PySpark DataFrames (Partition-aware querying)
Project Structure
/src: Contains PySpark jobs for data normalization and enrichment.
/docs: Architecture diagrams and milestone documentation.
/data: Staging area for local metadata and small threat feed samples.
/notebooks: Analytics and visualization of threat detection results.
Getting Started
Data Ingestion: Run src/ingestion/load_lanl.py to move raw logs into HDFS blocks.
Processing: Execute src/processing/enrich_logs.py to perform the Broadcast Hash Join between logs and threat feeds.
Analysis: Open notebooks/threat_hunting.ipynb to query the enriched HBase table for malicious activity.
Author
Olaoluwa Adedamola Omodemi - Kennesaw State University
Course: CS 4265 - Big Data Analytics
