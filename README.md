# MongoDB Replica Set with S3 Migration & Integrity Checks

## Table of contents

- [Project Goal](#-project-goal)
- [What This POC Demonstrates](#-what-this-poc-demonstrates)
- [Pipeline Architecture and Workflow](#-pipeline-architecture-and-workflow)
  - [Main steps](#main-steps)
  - [Pipeline Diagram](#pipeline-diagram)
- [Tech Stack](#-tech-stack)
- [Running the Project](#-running-the-project)
  - [Prerequisites](#prerequisites)
  - [Start the services](#start-the-services)
  - [Run validation scripts](#run-validation-scripts)
- [Outputs and Results](#-outputs-and-results)
- [Possible Improvements](#-possible-improvements)

---

## 🎯 Project Goal

This project demonstrates how to deploy and initialize a **MongoDB Replica Set** in a fully containerized environment, while automatically migrating JSON data from **AWS S3** into MongoDB and validating data integrity.

The objective is to simulate a production-like MongoDB architecture including:

- Replica Set configuration
- Internal authentication via keyFile
- Automated initialization process
- Data migration from cloud storage
- Post-migration integrity validation
- Replication and performance testing

---

## 🧠 What This POC Demonstrates

- Automated MongoDB Replica Set initialization (1 primary, 2 secondaries)
- Secure intra-cluster communication using a keyFile
- Root user creation and authentication enforcement
- JSON data ingestion from AWS S3
- Data normalization and date conversion
- Integrity checks between S3 and MongoDB
- Replica set validation
- Query response time measurement

---

## 🗂️ Pipeline Architecture and Workflow

### Main steps

1. **Replica Set Initialization**
   - `mongo1` starts in standalone mode
   - Admin user is created
   - Migration script is executed
   - MongoDB restarts in replicaSet mode (`rs0`)
   - Replica set is initiated with 3 members

2. **Data Migration from S3**
   - JSON files are fetched from S3 using `boto3`
   - Files are parsed and normalized
   - Date fields are converted to `datetime`
   - Data is inserted into MongoDB collections
   - Insert results are logged

3. **Integrity Analysis**
   - Schema and field comparison (S3 vs MongoDB)
   - Data type comparison
   - Missing values analysis
   - Duplicate detection on key fields
   - Integrity report generation (`integrity_report.json`)

4. **Replication & Performance Validation**
   - Test database creation across replica members
   - Verification of replication consistency
   - Query response time measurement

---

### Pipeline Diagram

<img src="./doc/architecture.png" alt="Architecture diagram" width="700"/>

---

## ⚙️ Tech Stack

- **MongoDB** (Replica Set)
- **Docker & Docker Compose**
- **Python 3**
- **PyMongo**
- **boto3**
- **pandas**
- **AWS S3**

---

## 🚀 Running the Project

### Prerequisites

- Docker & Docker Compose installed
- AWS account with S3 bucket containing JSON files
- Python 3.11+ (for running test scripts locally)

Either:

Set environment variables:

```bash
export MONGO_INITDB_ROOT_USERNAME="your_admin_username"
export MONGO_INITDB_ROOT_PASSWORD="your_admin_password"

export AWS_ACCESS_KEY_ID="your_access_key"
export AWS_SECRET_ACCESS_KEY="your_secret_key"
export AWS_REGION="your_region"
export BUCKET="your_bucket_name"
export PREFIX="your/prefix/"
```

Or:

Copy .env.example to .env
Replace placeholders with your own values

### Start the services

```bash
docker compose up -d
```

On first launch, the primary container automatically:
- Starts MongoDB in standalone mode
- Creates the admin user
- Migrates JSON files from S3
- Restarts MongoDB in replica set mode
- Initiates the replica set (rs0)

Subsequent restarts skip initialization if the data directory already exists.

### Run validation scripts
1. Test replica set replication

```bash
python ./scripts/test_replication.py \
  --u your_user \
  --p your_password \
  --adress1 localhost:27017 \
  --adress2 localhost:27018 \
  --adress3 localhost:27019 \
  --db database_test
```

This script:
- Connects using the replica set URI
- Creates a temporary database
- Verifies visibility across all nodes
- Deletes the test database

2. Test data integrity (S3 vs MongoDB)

```bash
python ./scripts/test_integrity.py \
  --aws-access-key your_key \
  --aws-secret-key your_secret \
  --region your_region \
  --bucket your_bucket \
  --prefix your_prefix \
  --mongo-uri "mongodb://user:password@localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0" \
```

This script:
- Re-analyzes S3 JSON files
- Compares MongoDB collections
- Detects schema differences
- Checks missing values
- Detects duplicates
- Generates integrity_report.json

3. Test query response time

```bash
python /scripts/test_response_time.py \
  --uri "mongodb://user:password@localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0" \
  --db weather_records \
  --collection data \
  --find_query '{\"id_station\": \"ILAMAD25\", \"dh_utc\": {\"$gte\": \"2024-10-03T00:00:00Z\", \"$lt\": \"2024-10-04T00:00:00Z\"}}' # custom query
```

This script:
- Executes a query
- Measures execution time in milliseconds
- Displays number of returned documents

## 📊 Outputs and Results
- MongoDB Replica Set running (rs0)
- JSON data migrated into MongoDB collections
- Integrity report generated (integrity_report.json)
- Verified replication across 3 nodes
- Measured query performance

## 🔍 Possible Improvements
- Add indexing strategy for performance optimization
- Add retry & resilience logic during S3 ingestion
- Implement structured logging instead of print statements
- Add monitoring (MongoDB Exporter + Prometheus + Grafana)
- Add CI/CD pipeline for automated testing