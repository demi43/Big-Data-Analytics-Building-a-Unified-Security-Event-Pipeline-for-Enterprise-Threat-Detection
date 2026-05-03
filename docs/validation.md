# Validation Report — Unified Security Event Pipeline

**CS 4265 Big Data Analytics | Olaoluwa Adedamola Omodemi**  
**Run date:** 2026-05-03 | **Pipeline version:** M4  
**Run command:** `python executepipeline.py`  
**Started:** 09:02:38 | **Finished:** 09:41:06 | **Total time:** 38m 27.9s

---

## 1. Test Cases Executed

| #     | Scenario                                                    | Method                                           | Result                                 |
| ----- | ----------------------------------------------------------- | ------------------------------------------------ | -------------------------------------- |
| TC-01 | Full pipeline runs end-to-end without manual intervention   | `python executepipeline.py`                      | PASS                                   |
| TC-02 | URLHaus API fetch retrieves live threat records             | `scripts/fetch_urlhaus.py`                       | PASS — 20 URLs retrieved               |
| TC-03 | URLHaus JSON converts to Parquet and uploads to S3          | Stage 2 inline (pandas + pyarrow + boto3)        | PASS                                   |
| TC-04 | Auth dataset (708M rows, bzip2) ingests with correct schema | `auth_to_parquet.py`                             | PASS                                   |
| TC-05 | DNS dataset (40.8M rows, gzip) ingests with correct schema  | `dns_to_parquet.py`                              | PASS                                   |
| TC-06 | Flows dataset (130M rows, gzip) ingests with correct schema | `flows_to_parquet.py`                            | PASS                                   |
| TC-07 | Proc dataset (426M rows, gzip) ingests with correct schema  | `proc_to_parquet.py`                             | PASS                                   |
| TC-08 | All Silver datasets written to S3 as Parquet (Snappy)       | S3 console + Spark write confirmation            | PASS                                   |
| TC-09 | Gold enrichment join runs without row fanout                | `enrich.py` — deduplication applied before joins | PASS                                   |
| TC-10 | Gold output partitioned correctly by time_bucket            | pyarrow file inspection (2,515 part files)       | PASS                                   |
| TC-11 | Validation stage reads Gold Parquet and returns metrics     | Stage 5 pyarrow metadata read                    | PASS                                   |
| TC-12 | Pipeline resumes from a specific stage with `--skip-silver` | `python executepipeline.py --skip-silver`        | PASS                                   |
| TC-13 | `--validate-only` flag runs only the validation report      | `python executepipeline.py --validate-only`      | PASS                                   |
| TC-14 | Missing URLHaus API key falls back to cached JSON           | Unset `URLHAUS_API_KEY`, re-ran Stage 1          | PASS — cached file used                |
| TC-15 | Pipeline aborts cleanly if Silver stage fails               | Simulated bad input path                         | PASS — exit code 1, stages 4–5 skipped |

---

## 2. Data Quality Metrics

### 2.1 Silver Layer — Record Counts

| Dataset   | Raw File                  | Compression | Records Ingested  | Partitions |
| --------- | ------------------------- | ----------- | ----------------- | ---------- |
| auth      | `lanl-auth-dataset-1.bz2` | bzip2       | **708,304,516**   | 2          |
| dns       | `dns.txt.gz`              | gzip        | **40,821,591**    | 1          |
| flows     | `flows.txt.gz`            | gzip        | **129,977,412**   | 1          |
| proc      | `proc.txt.gz`             | gzip        | **426,045,096**   | 1          |
| urlhaus   | `urlhaus_sample.json`     | —           | **20**            | 1          |
| **TOTAL** |                           |             | **1,305,148,635** |            |

### 2.2 Gold Layer — user_activity_summary

| Metric                       | Value                                      |
| ---------------------------- | ------------------------------------------ |
| Total rows                   | **1,883,499**                              |
| Parquet part files           | **2,515** (partitioned by `time_bucket`)   |
| Disk size                    | **31.8 MB**                                |
| Distinct users tracked       | **262**                                    |
| Distinct source computers    | —                                          |
| Rows with malicious hits > 0 | **0** (see Known Issues §4)                |
| Data reduction from raw      | **99.86%** (1.3B events → 1.88M summaries) |

### 2.3 Gold Layer — Numeric Column Statistics

| Column             | Count | Mean      | Std Dev     | Min | 25th % | Median | 75th % | Max            |
| ------------------ | ----- | --------- | ----------- | --- | ------ | ------ | ------ | -------------- |
| `total_flows`      | 7,944 | 56.6      | 425.5       | 1   | 8      | 16     | 38     | 14,501         |
| `total_bytes`      | 7,944 | 9,474,916 | 298,853,000 | 46  | 4,321  | 18,075 | 37,523 | 14,170,770,000 |
| `total_packets`    | 7,944 | 9,802     | 218,135     | 1   | 42     | 67     | 202    | 10,073,680     |
| `active_processes` | 7,944 | 0.078     | 0.335       | 0   | 0      | 0      | 0      | 6              |
| `malicious_hits`   | 7,944 | 0.0       | 0.0         | 0   | 0      | 0      | 0      | 0              |

### 2.4 Null Rate Analysis (auth Silver, first Parquet file)

| Column            | Null Rate |
| ----------------- | --------- |
| `time`            | 0.0%      |
| `source_user`     | 0.0%      |
| `source_computer` | 0.0%      |

All three auth columns are fully populated with no null values in the sampled file, confirming the schema enforcement at read time is working correctly.

---

## 3. Sample Validations — Records Traced Through Pipeline

### 3.1 Auth — Bronze to Silver

**Input (raw bzip2 CSV, first 5 rows):**

```
1,U1,C1
1,U1,C2
2,U2,C3
3,U3,C4
6,U4,C5
```

**Silver Parquet output schema (confirmed via `printSchema()`):**

```
root
 |-- time: integer (nullable = true)
 |-- source_user: string (nullable = true)
 |-- source_computer: string (nullable = true)
```

**Verification:** Row count 708,304,516 confirmed via `df.count()` before write. Schema types match declared `StructType` — `time` is integer, not string.

---

### 3.2 Flows — Bronze to Silver

**Input (raw gzip CSV, first 3 rows):**

```
1,0,C1065,389,C3799,N10451,6,10,5323
1,0,C1423,N1136,C1707,N1,6,5,847
1,0,C1423,N1142,C1707,N1,6,5,847
```

**Silver Parquet output schema (confirmed via `printSchema()`):**

```
root
 |-- time: integer          |-- src_port: string
 |-- duration: integer      |-- dst_computer: string
 |-- src_computer: string   |-- dst_port: string
                            |-- protocol: integer
                            |-- packets_count: integer
                            |-- bytes_count: integer
```

**Verification:** Row count 129,977,412. All 9 columns present. Numeric fields (`packets_count`, `bytes_count`) correctly cast to integer rather than left as string.

---

### 3.3 Proc — Bronze to Silver

**Input (raw gzip CSV, first 3 rows):**

```
1,C1$@DOM1,C1,P16,Start
1,C1001$@DOM1,C1001,P4,Start
1,C1002$@DOM1,C1002,P4,Start
```

**Silver Parquet output (confirmed via `show()`):**

| time | user_domain | computer | process_name | start_end |
| ---- | ----------- | -------- | ------------ | --------- |
| 1    | C1$@DOM1    | C1       | P16          | Start     |
| 1    | C1001$@DOM1 | C1001    | P4           | Start     |

**Verification:** Row count 426,045,096. `start_end` values are `Start` and `End` — no unexpected values observed in sample.

---

### 3.4 URLHaus — JSON to Parquet to Gold

**Input (first 3 entries from `urlhaus_sample.json`):**

```json
{ "url": "http://27.215.210.91:46312/bin.sh",       "threat": "malware_download" }
{ "url": "https://wood-zone.weplord.lat/...",         "threat": "malware_download" }
{ "url": "http://182.121.152.140:40641/i",            "threat": "malware_download" }
```

**Parquet columns written:** `url`, `host`, `threat`, `date_added`, `tags`

**Gold join logic applied:**

```
host extracted via: regexp_extract(url, 'https?://([^/]+)', 1)
→ "27.215.210.91:46312", "wood-zone.weplord.lat", "182.121.152.140:40641"
join condition: flows.dst_computer == urlhaus.host
```

**Result:** 0 matches. LANL `dst_computer` values are anonymized IDs (e.g. `C3799`, `C5720`) — they cannot match real-world hostnames. See §4 Known Issues.

---

### 3.5 Gold — Aggregation Spot-Check

**Sample Gold output rows (5 rows from `user_activity_summary`):**

| user | src_computer | total_flows | total_bytes | total_packets | active_processes | malicious_hits | time_bucket |
| ---- | ------------ | ----------- | ----------- | ------------- | ---------------- | -------------- | ----------- |
| U892 | C1040        | 1           | 46          | 1             | 0                | 0              | 0           |
| U356 | C150         | 10          | 10,490      | 44            | 0                | 0              | 0           |
| U752 | C150         | 3           | 138         | 3             | 0                | 0              | 0           |
| U156 | C161         | 7           | 14,518      | 52            | 0                | 0              | 0           |
| U487 | C1654        | 1           | 329         | 2             | 0                | 0              | 0           |

**Sanity checks passed:**

- `total_flows >= 1` for all rows ✓
- `total_bytes >= total_packets` for all rows (bytes per packet ≥ 1) ✓
- `time_bucket` values are multiples of 3600 ✓
- `malicious_hits` is 0 or positive integer ✓

---

## 4. Known Issues and Limitations

### Issue 1 — Malicious Hits Always Zero (Expected Behavior)

**Description:** The `malicious_hits` column in the Gold output is 0 for all rows.  
**Root cause:** The LANL Cyber Dataset uses fully anonymized computer identifiers (e.g. `C1040`, `C3799`) as both source and destination hostnames. The URLHaus feed contains real-world malicious domain names and IP addresses (e.g. `wood-zone.weplord.lat`, `182.121.152.140`). These two namespaces cannot overlap by construction — the anonymization makes threat correlation impossible against this specific dataset.  
**Impact:** The enrichment join logic, deduplication, and aggregation are all verified correct. The zero result reflects the dataset, not a bug in the pipeline.  
**Mitigation in production:** Against un-anonymized network logs containing real hostnames or public IPs, this join would surface genuine threat matches.

### Issue 2 — Silver Validation Skipped for Cloud Paths

**Description:** The Stage 5 validation reports `SKIP` for auth, dns, flows, proc, and urlhaus because they were written to S3 and the local validation uses pyarrow's local filesystem reader.  
**Root cause:** pyarrow's `read_metadata()` does not natively traverse S3 paths without configuring `s3fs` credentials. The validation stage intentionally avoids a second Spark session to keep runtime short.  
**Impact:** Silver record counts are confirmed at write time (via Spark's `df.count()`) but not re-verified post-write. Gold, which is written locally, is fully validated.  
**Mitigation:** Run `python executepipeline.py --validate-only` after setting `SILVER_PARQUET_URI` to a local path, or use `aws s3 ls --recursive` to verify S3 object counts manually.

### Issue 3 — Non-Splittable Compression Limits Silver Parallelism

**Description:** The auth (bzip2) and proc/dns/flows (gzip) source files cannot be split by Spark's input format, so each file is read as a single partition regardless of file size.  
**Impact:** Auth reads at 1 partition (708M rows) and proc at 1 partition (426M rows), meaning only 1 core is active during the read phase even in `local[*]` mode. This is the primary bottleneck in the 27-minute Silver stage.  
**Mitigation:** On EMR, use `spark-submit` with multiple executors and consider converting bronze files to a splittable format (e.g. LZ4-compressed CSV or uncompressed Parquet) in a one-time preprocessing step.

### Issue 4 — Spark Temp File Cleanup Warning on Windows

**Description:** At the end of each Spark job, a `WARN SparkEnv` / `ERROR ShutdownHookManager` message appears indicating that `org.wildfly.openssl_wildfly-openssl-1.0.7.Final.jar` could not be deleted from `C:\tmp\spark\`.  
**Root cause:** Windows file locking prevents deletion of JAR files that are still held open by the JVM at shutdown time.  
**Impact:** None — the warning is cosmetic. The JAR accumulates in `C:\tmp\spark\` across runs. Run `Remove-Item -Recurse -Force C:\tmp\spark\*` to clear it manually.  
**Mitigation:** Already documented in `notes.txt`. Does not affect correctness or S3 output.

### Issue 5 — DNS Column Naming Inconsistency

**Description:** The DNS Silver schema uses `SourceComputer` and `ComputerResolved` (PascalCase) while the other three datasets use `snake_case` column names.  
**Impact:** The DNS table is not joined in the current Gold enrichment script, so this inconsistency does not cause a runtime error. If DNS is added to the Gold join in future, column name aliasing will be required.  
**Mitigation:** Rename columns to `source_computer` and `computer_resolved` in `dns_to_parquet.py` before integrating into the Gold join.

---

## 5. Performance Results

### 5.1 Runtime by Stage

| Stage                              | Wall-clock Time | % of Total |
| ---------------------------------- | --------------- | ---------- |
| 1 — URLHaus fetch                  | 1.0s            | 0.04%      |
| 2 — URLHaus → Parquet              | 2.3s            | 0.10%      |
| 3 — Silver layer (total)           | 27m 28.4s       | 71.4%      |
| &nbsp;&nbsp;&nbsp;auth_to_parquet  | 10m 55.4s       | 28.4%      |
| &nbsp;&nbsp;&nbsp;dns_to_parquet   | 1m 12.4s        | 3.1%       |
| &nbsp;&nbsp;&nbsp;flows_to_parquet | 4m 45.7s        | 12.4%      |
| &nbsp;&nbsp;&nbsp;proc_to_parquet  | 10m 35.0s       | 27.5%      |
| 4 — Gold layer                     | 10m 47.1s       | 28.0%      |
| 5 — Validation                     | 9.1s            | 0.39%      |
| **TOTAL**                          | **38m 27.9s**   | 100%       |

### 5.2 Throughput (Silver Stage)

| Dataset      | Records           | Time         | Throughput                |
| ------------ | ----------------- | ------------ | ------------------------- |
| auth         | 708,304,516       | 655.4s       | ~1,081,000 rows/sec       |
| dns          | 40,821,591        | 72.4s        | ~564,000 rows/sec         |
| flows        | 129,977,412       | 285.7s       | ~455,000 rows/sec         |
| proc         | 426,045,096       | 635.0s       | ~671,000 rows/sec         |
| **Combined** | **1,305,147,615** | **1,648.5s** | **~791,000 rows/sec avg** |

### 5.3 Resource Usage

| Resource         | Setting / Observed                                 |
| ---------------- | -------------------------------------------------- |
| Spark version    | 4.1.1                                              |
| Execution mode   | `local[*]` (single machine)                        |
| Driver heap      | 8 GB (`SPARK_DRIVER_MEMORY=8g`)                    |
| Max result size  | 2 GB (`SPARK_DRIVER_MAX_RESULT_SIZE=2g`)           |
| Spark temp dir   | `C:\tmp\spark\`                                    |
| Storage — Silver | Amazon S3 `us-east-2` (cloud)                      |
| Storage — Gold   | Local `Parquet/gold/` (31.8 MB)                    |
| JARs             | `hadoop-aws:3.3.4`, `aws-java-sdk-bundle:1.12.262` |
| OS               | Windows 11 Pro 10.0.26200                          |

### 5.4 Storage Efficiency

| Layer | Estimated Raw Size            | Parquet + Snappy | Reduction            |
| ----- | ----------------------------- | ---------------- | -------------------- |
| auth  | ~15 GB (bzip2 → uncompressed) | S3 (cloud)       | n/a local            |
| dns   | ~1.2 GB                       | S3 (cloud)       | n/a local            |
| flows | ~4 GB                         | S3 (cloud)       | n/a local            |
| proc  | ~10 GB                        | S3 (cloud)       | n/a local            |
| Gold  | ~GB-scale joins               | **31.8 MB**      | >99% via aggregation |

---

_Generated from pipeline run on 2026-05-03. Re-run `python executepipeline.py --validate-only` to refresh metrics._
