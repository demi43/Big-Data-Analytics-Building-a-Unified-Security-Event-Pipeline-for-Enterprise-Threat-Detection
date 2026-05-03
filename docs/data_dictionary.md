# Data Dictionary — Unified Security Event Pipeline

**CS 4265 Big Data Analytics | Olaoluwa Adedamola Omodemi**

This document describes the schema for every dataset at each Medallion layer.
Bronze is the raw compressed source. Silver is schema-enforced Parquet. Gold is
the enriched, aggregated analytics output.

---

## Bronze Layer (Raw Sources)

All Bronze files are stored in S3 under `s3://security-pipeline-lanl/bronze/`.

| File | Format | Compression | Size |
|------|--------|-------------|------|
| `lanl-auth-dataset-1.bz2` | CSV, no header | bzip2 | ~708M rows |
| `dns.txt.gz` | CSV, no header | gzip | ~40.8M rows |
| `flows.txt.gz` | CSV, no header | gzip | ~130M rows |
| `proc.txt.gz` | CSV, no header | gzip | ~426M rows |
| `urlhaus_sample.json` | JSON | none | up to 1,000 records |

---

## Silver Layer — Auth (`silver/auth/`)

**Source:** `lanl-auth-dataset-1.bz2`  
**Script:** `src/processing/auth_to_parquet.py`  
**Records:** 708,304,516

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `time` | integer | yes | Event index (seconds from start of observation window) |
| `source_user` | string | yes | Anonymized user identifier (e.g. `U1`, `U892`) |
| `source_computer` | string | yes | Anonymized source computer (e.g. `C1`, `C1040`) |

**Notes:** All identifiers are anonymized by LANL. `time` is a sequential
integer index, not a Unix timestamp. No header row in the raw file.

---

## Silver Layer — DNS (`silver/dns/`)

**Source:** `dns.txt.gz`  
**Script:** `src/processing/dns_to_parquet.py`  
**Records:** 40,821,591

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `time` | integer | yes | Event time index |
| `SourceComputer` | string | yes | Anonymized computer making the DNS query |
| `ComputerResolved` | string | yes | Anonymized hostname returned by DNS resolution |

**Notes:** Column names use PascalCase (original LANL convention). This dataset
is not currently joined in the Gold layer. If added in future, columns should be
aliased to `source_computer` and `computer_resolved` to match the pipeline's
snake_case convention.

---

## Silver Layer — Flows (`silver/flows/`)

**Source:** `flows.txt.gz`  
**Script:** `src/processing/flows_to_parquet.py`  
**Records:** 129,977,412

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `time` | integer | yes | Event time index |
| `duration` | integer | yes | Flow duration in seconds |
| `src_computer` | string | yes | Anonymized source computer |
| `src_port` | string | yes | Source port (numeric string or named port, e.g. `389`, `N1136`) |
| `dst_computer` | string | yes | Anonymized destination computer |
| `dst_port` | string | yes | Destination port |
| `protocol` | integer | yes | IP protocol number (e.g. `6` = TCP) |
| `packets_count` | integer | yes | Number of packets in the flow |
| `bytes_count` | integer | yes | Total bytes transferred |

**Notes:** `src_port` and `dst_port` may be numeric strings or LANL-assigned
named port identifiers (e.g. `N10451`). The `flows` table is the driving table
in the Gold enrichment join.

---

## Silver Layer — Proc (`silver/proc/`)

**Source:** `proc.txt.gz`  
**Script:** `src/processing/proc_to_parquet.py`  
**Records:** 426,045,096

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `time` | integer | yes | Event time index |
| `user_domain` | string | yes | Anonymized user and domain (e.g. `C1$@DOM1`, `U5@DOM1`) |
| `computer` | string | yes | Anonymized computer where the process ran |
| `process_name` | string | yes | Anonymized process identifier (e.g. `P4`, `P16`) |
| `start_end` | string | yes | Process lifecycle event: `Start` or `End` |

**Notes:** `user_domain` combines user and domain in a single field. Both
process names and computer names are anonymized. Only `Start` and `End` values
observed in `start_end`.

---

## Silver Layer — URLHaus (`silver/urlhaus/`)

**Source:** URLHaus API (`https://urlhaus-api.abuse.ch/v1/urls/recent/`)  
**Script:** Stage 2 inline (`executepipeline.py`) via pandas + pyarrow  
**Records:** up to 1,000 per fetch (1,000 in final run)

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `url` | string | yes | Full malicious URL (e.g. `https://wood-zone.weplord.lat/...`) |
| `host` | string | yes | Hostname or IP extracted from `url` by the API |
| `threat` | string | yes | Threat category (e.g. `malware_download`, `phishing`) |
| `date_added` | string | yes | UTC timestamp when the URL was added to URLHaus |
| `tags` | string | yes | Comma-separated classification tags (e.g. `ClearFake`) |

**Notes:** The `host` column is also computed in `enrich.py` via
`regexp_extract(url, 'https?://([^/]+)', 1)` before the Gold join, in case
the raw `host` field contains a port number (e.g. `182.121.152.140:40641`).

---

## Gold Layer — `user_activity_summary` (`gold/user_activity_summary/`)

**Source:** Join of all five Silver tables  
**Script:** `src/scripts/enrich.py`  
**Records:** 1,883,499  
**Partition key:** `time_bucket`  
**S3 path:** `s3://security-pipeline-lanl/gold/user_activity_summary/`

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `time_bucket` | long | no | Unix epoch floored to 1-hour window: `floor(time / 3600) * 3600` |
| `user` | string | yes | Anonymized user from auth join (`source_user`) |
| `src_computer` | string | no | Anonymized source computer from flows |
| `total_flows` | long | no | Count of network flow events in this bucket |
| `total_bytes` | long | yes | Sum of `bytes_count` across all flows in bucket |
| `total_packets` | long | yes | Sum of `packets_count` across all flows in bucket |
| `active_processes` | long | no | Count of distinct process names observed on `src_computer` in bucket |
| `malicious_hits` | long | no | Sum of `malicious_hit` flags (1 if `dst_computer` matched a URLHaus host, else 0) |

**Partition structure:**
```
gold/user_activity_summary/
├── time_bucket=0/
│   └── part-00000-*.snappy.parquet
├── time_bucket=3600/
│   └── part-00000-*.snappy.parquet
└── ...  (2,515 files total across all time buckets)
```

**Join logic summary:**
```
flows
  LEFT JOIN auth_dedup  ON (src_computer == source_computer AND time == time)
  LEFT JOIN proc_dedup  ON (src_computer == computer        AND time == time)
  LEFT JOIN urlhaus     ON (dst_computer == host)            [broadcast join]
```

**Known limitation:** `malicious_hits` is 0 for all rows when run against the
LANL dataset because LANL uses anonymized computer identifiers (e.g. `C3799`)
that cannot match real URLHaus hostnames. In a production deployment against
un-anonymized logs, this column surfaces real threat matches.

---

## Field Value Ranges (Gold Layer, measured run 2026-05-03)

| Column | Min | 25th % | Median | 75th % | Max |
|--------|-----|--------|--------|--------|-----|
| `total_flows` | 1 | 8 | 16 | 38 | 14,501 |
| `total_bytes` | 46 | 4,321 | 18,075 | 37,523 | 14,170,770,000 |
| `total_packets` | 1 | 42 | 67 | 202 | 10,073,680 |
| `active_processes` | 0 | 0 | 0 | 0 | 6 |
| `malicious_hits` | 0 | 0 | 0 | 0 | 0 |

---

*Last updated: 2026-05-03. Re-run `python executepipeline.py --validate-only` to refresh statistics.*
