# MongoDB Replica Set — S3 Migration & Integrity Validation

`Python` `MongoDB` `Docker` `AWS S3` `boto3` `PyMongo`

---

## Overview

Fully containerized MongoDB Replica Set with automated data migration from AWS S3 and end-to-end integrity validation.

The goal was to simulate a production-like distributed database environment — covering not just the happy path (data gets in) but the full operational surface: cluster authentication, replication consistency, post-migration validation, and query performance baseline.

---

## Architecture decisions

**Odd node count (1 primary + 2 secondaries), no arbiter.**  
Designed for deployment across 3 separate datacenters (cloud or standard). A single node failure is recoverable; simultaneous failure of 2 nodes in independent datacenters is statistically unlikely; all 3 failing simultaneously is not a realistic operational scenario. This topology reaches quorum on its own — no arbiter needed, no extra failure point.

**keyFile-based intra-cluster authentication.**  
Internal MongoDB communication is secured via a shared keyFile rather than open-trust networking. Necessary in any environment where containers could be exposed, and a prerequisite for production-grade replica set configuration.

**Fully automated bootstrap sequence.**  
The primary container handles the entire initialization in order: standalone start → admin user creation → S3 migration → replica set restart → rs.initiate(). Zero manual intervention required. The sequence is idempotent — subsequent restarts skip initialization if the data directory already exists.

**Post-migration integrity validation as a first-class step.**  
Migration success isn't declared on insert count alone. A dedicated validation script re-reads S3 source files and compares against MongoDB: schema coverage, field types, missing values, duplicate detection, and replication consistency across all 3 nodes. Output: a structured `integrity_report.json`.

---

## Pipeline

```
AWS S3 (JSON files)
    │
    ▼
boto3 ingestion → normalization → datetime conversion
    │
    ▼
MongoDB Primary (rs0)
    │
    ├── Replica → mongo2
    └── Replica → mongo3
    │
    ▼
Integrity validation (schema · types · duplicates · replication)
    │
    ▼
integrity_report.json + query performance benchmark
```

---

## Tech stack

| Layer | Tools |
|---|---|
| Database | MongoDB Replica Set (rs0) |
| Infra | Docker · Docker Compose |
| Ingestion | boto3 · pandas |
| Validation | PyMongo · custom validation scripts |
| Cloud storage | AWS S3 |

---

## Running the project

### Prerequisites

- Docker & Docker Compose
- AWS S3 bucket with JSON files
- Credentials via environment variables or `.env` (see `.env.example`)

```bash
cp .env.example .env
# fill in MONGO credentials + AWS credentials
docker compose up -d
```

On first launch, the primary container runs the full bootstrap automatically.  
Subsequent restarts are no-ops if the data directory exists.

### Validation scripts

```bash
# Replication consistency across all 3 nodes
python scripts/test_replication.py --u user --p pass \
  --adress1 localhost:27017 --adress2 localhost:27018 --adress3 localhost:27019

# Integrity check: S3 source vs MongoDB (generates integrity_report.json)
python scripts/test_integrity.py --mongo-uri "mongodb://..." \
  --bucket your_bucket --prefix your_prefix

# Query performance baseline
python scripts/test_response_time.py --uri "mongodb://..." \
  --db your_db --collection your_collection --find_query '{...}'
```

---

## What's not in scope (and why)

- **Indexing strategy** — deliberately excluded to keep the performance baseline clean. Adding indexes before benchmarking would conflate migration performance with query optimization.
- **Structured logging** — print statements kept for readability in a POC context. A production setup would use a structured logger with log levels and correlation IDs.
- **Monitoring stack** (Prometheus + Grafana) — listed as a natural extension; Prometheus metrics exposure is implemented in the [streaming lakehouse project](https://github.com/SebM42/POC-Employees-sport-event-Streaming).

---

## Possible extensions

- Index strategy + re-benchmark to quantify impact
- Retry & resilience logic on S3 ingestion failures
- MongoDB Exporter + Prometheus + Grafana monitoring stack
- CI/CD pipeline for automated integrity testing on schema changes
