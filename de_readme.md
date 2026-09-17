# DataPilot AI

## Agentic Data Engineering & DataOps Platform

DataPilot AI is a configuration-driven data engineering and DataOps platform designed to evolve into an agentic system capable of pipeline orchestration, data quality monitoring, root-cause analysis, and automated remediation.

The project currently focuses on building the **core Data Engineering foundation** using Apache Airflow, Apache Spark, AWS S3, and a Medallion Architecture.


# Current Project Status

### Completed

* Local containerized Data Engineering environment
* Docker + Colima setup on macOS
* Apache Airflow orchestration
* Apache Spark standalone cluster
* AWS S3 data lake
* Configuration-driven Spark framework
* Centralized `variables.yaml`
* Bronze layer
* Silver layer
* Data Quality framework
* DQ result persistence
* Gold business transformation
* Airflow DAG orchestration
* DQ failure detection and pipeline blocking

### Upcoming

* RCA Agent
* MCP Server
* Metadata and lineage
* RAG-based investigation
* Supervisor Agent
* Automated remediation
* GitHub PR automation
* Evaluation and observability

---

# Architecture — Current Data Engineering Layer

```text
                         DataPilot AI
                              │
                         Airflow DAG
                              │
                    ┌─────────┴─────────┐
                    │                   │
                 Bronze               Silver
                    │                   │
                    ▼                   ▼
                 Apache Spark        Apache Spark
                    │                   │
                    └─────────┬─────────┘
                              │
                              ▼
                       Data Quality
                              │
                    ┌─────────┴─────────┐
                    │                   │
                  PASS                 FAIL
                    │                   │
                    ▼                   ▼
                   Gold            Pipeline Blocked
                    │
                    ▼
                  AWS S3
```

---

# Technology Stack

| Area              | Technology             |
| ----------------- | ---------------------- |
| Container Runtime | Docker + Colima        |
| Orchestration     | Apache Airflow 3.3.1   |
| Processing        | Apache Spark 4.1.3     |
| Programming       | Python                 |
| Storage           | Amazon S3              |
| Data Format       | CSV / Parquet          |
| Architecture      | Medallion Architecture |
| Database          | PostgreSQL             |
| Configuration     | YAML                   |
| Cloud             | AWS                    |
| Version Control   | Git / GitHub           |
| Local Platform    | macOS Apple Silicon    |

---

# Infrastructure Setup

The project runs locally using **Colima** as the Docker runtime.

The local environment contains:

```text
Airflow
   │
   ├── Scheduler
   ├── DAG Processor
   ├── Triggerer
   └── Web/API
        │
        ▼
   Spark Cluster
        │
        ├── Spark Master
        └── Spark Worker
        │
        ▼
       AWS S3
```

PostgreSQL is used by Airflow for metadata storage.

---

# AWS S3 Data Lake

The project uses an S3 bucket for the DataPilot AI data lake.

```text
s3://datapilot-ai-data-practice/
│
├── raw/
│
└── processed/
    │
    ├── bronze/
    │
    ├── silver/
    │
    └── gold/
```

## Data Layers

### Raw

Original source CSV files are stored without transformation.

### Bronze

Raw datasets are converted into Parquet format.

### Silver

Silver applies standard data engineering transformations such as:

* Column standardization
* Duplicate removal
* Timestamp conversion
* Data type casting
* String trimming
* Null handling
* Row filtering
* Column removal

### Gold

Gold contains business-oriented datasets designed for analytics and downstream consumption.

---

# Olist Dataset

The current implementation uses the Brazilian Olist e-commerce dataset.

The following datasets are processed:

```text
olist_customers
olist_geolocation
olist_order_items
olist_order_payments
olist_order_reviews
olist_orders
olist_products
olist_sellers
olist_product_category_translation
```

---

# Configuration-Driven Framework

A major design principle of DataPilot AI is to avoid hardcoding infrastructure paths inside individual Spark jobs.

The centralized configuration is maintained in:

```text
config/variables.yaml
```

Example:

```yaml
project:
  name: datapilot-ai
  environment: dev

aws:
  region: us-east-1

storage:
  type: s3
  bucket: datapilot-ai-data-practice

  layers:
    raw: raw
    bronze: processed/bronze
    silver: processed/silver
    gold: processed/gold

  formats:
    raw: csv
    bronze: parquet
    silver: parquet
    gold: parquet
```

This allows the framework to resolve paths dynamically.

For example:

```text
Raw
s3://datapilot-ai-data-practice/raw/

Bronze
s3://datapilot-ai-data-practice/processed/bronze/

Silver
s3://datapilot-ai-data-practice/processed/silver/

Gold
s3://datapilot-ai-data-practice/processed/gold/
```

Individual layer configurations therefore do not need to contain hardcoded S3 bucket paths.

---

# Project Structure

Current structure:

```text
DataPilot AI/
│
├── config/
│   ├── variables.yaml
│   │
│   ├── bronze/
│   │   ├── olist_customers.yaml
│   │   ├── olist_geolocation.yaml
│   │   ├── olist_order_items.yaml
│   │   ├── olist_order_payments.yaml
│   │   ├── olist_order_reviews.yaml
│   │   ├── olist_orders.yaml
│   │   ├── olist_products.yaml
│   │   ├── olist_product_category_translation.yaml
│   │   └── olist_sellers.yaml
│   │
│   ├── silver/
│   │   └── <dataset configurations>
│   │
│   ├── gold/
│   │   └── <gold configurations>
│   │
│   └── dq/
│       └── <DQ configurations>
│
├── dags/
│   └── datapilot_olist_pipeline.py
│
├── spark/
│   │
│   ├── framework/
│   │   ├── transformer.py
│   │   ├── writer.py
│   │   ├── pipeline.py
│   │   ├── business_transformer.py
│   │   │
│   │   └── dq/
│   │       ├── __init__.py
│   │       ├── checks.py
│   │       └── engine.py
│   │
│   └── jobs/
│       ├── run_pipeline.py
│       ├── run_dq.py
│       └── run_gold.py
│
├── results/
│   └── dq/
│
├── docker/
│   └── spark/
│       └── Dockerfile
│
├── docker-compose.yml
│
└── README.md
```

---

# Bronze Layer

The Bronze layer converts raw CSV data into Parquet.

Generic processing flow:

```text
S3 Raw CSV
    │
    ▼
Spark Reader
    │
    ▼
Configuration
    │
    ▼
DataFrame
    │
    ▼
Parquet Writer
    │
    ▼
S3 Bronze
```

Example Bronze configuration:

```yaml
source:
  type: raw
  file: olist_orders_dataset.csv

read:
  header: true
  inferSchema: true

target:
  type: bronze

write:
  mode: overwrite
  partition_strategy: coalesce
  partitions: 1
```

The framework dynamically resolves the source and target locations.

---

# Silver Layer

Silver processing uses a reusable transformation framework.

Supported transformations currently include:

```text
Standardize column names
        ↓
Remove duplicates
        ↓
Timestamp conversion
        ↓
Data type casting
        ↓
Trim strings
        ↓
Null handling
        ↓
Row filtering
        ↓
Drop columns
```

This keeps transformation logic reusable instead of creating separate hardcoded Spark applications for every dataset.

---

# Special CSV Handling

The Olist review dataset required additional CSV parsing configuration because some fields contain quoted/multiline content.

The configuration supports:

```yaml
read:
  header: true
  inferSchema: true
  quote: '"'
  escape: '"'
  multiLine: true
```

This prevented malformed parsing and allowed the Silver timestamp transformations to execute correctly.

---

# Data Quality Framework

A configuration-driven Data Quality framework has been implemented.

Current checks:

```text
Row Count
Required Columns
Null Check
Duplicate Check
Range Check
```

Example:

```yaml
checks:
  row_count:
    enabled: true
    min: 1

  required_columns:
    enabled: true
    columns:
      - order_id
      - payment_sequential
      - payment_type
      - payment_installments
      - payment_value

  null_checks:
    enabled: true
    columns:
      - order_id
      - payment_type
      - payment_value

  duplicate_check:
    enabled: true
    keys:
      - order_id
      - payment_sequential

  range_checks:
    enabled: true
    payment_value:
      min: 0
      max: 100000

    payment_installments:
      min: 1
      max: 100
```

---

# DQ Failure Detection

The DQ framework successfully detected a real data-quality anomaly in:

```text
olist_order_payments
```

Failed check:

```text
range_check:payment_installments
```

Expected:

```text
1 <= payment_installments <= 100
```

Detected:

```text
Invalid Count: 2
Invalid Values: [0, 0]
```

The framework correctly marks the dataset as:

```text
OVERALL RESULT: FAIL
```

The DQ result is persisted as JSON:

```text
results/dq/olist_order_payments.json
```

Example processing flow:

```text
Silver Dataset
      │
      ▼
DQ Engine
      │
      ├── Row Count              PASS
      ├── Required Columns       PASS
      ├── Null Checks            PASS
      ├── Duplicate Check        PASS
      ├── Payment Value Range    PASS
      └── Installments Range     FAIL
                                  │
                                  ▼
                            DQ Result JSON
                                  │
                                  ▼
                            Pipeline Failure
```

The pipeline does not modify the source data to make the test pass.

This failure will later become an input to the **RCA Agent**.

---

# Gold Layer

A business-level Gold dataset has been implemented:

```text
olist_order_summary
```

The Gold transformation combines:

```text
Orders
   +
Order Items
   +
Payments
   +
Customers
```

The resulting dataset contains:

```text
order_id
customer_id
order_status
order_purchase_timestamp
order_delivered_customer_date
customer_city
customer_state
product_count
total_price
total_freight
total_payment
```

The Gold dataset currently contains approximately:

```text
99,441 orders
```

The transformation includes aggregations such as:

```text
Product Count
Total Price
Total Freight
Total Payment
```

---

# Airflow Orchestration

The complete data pipeline is orchestrated through Airflow.

Current DAG:

```text
datapilot_olist_order_pipeline
```

The DAG dynamically creates tasks for the Olist datasets.

Conceptually:

```text
Bronze
  │
  ▼
Silver
  │
  ▼
DQ
  │
  ├──────── PASS ────────► Gold
  │
  └──────── FAIL ────────► Pipeline Blocked
```

The current implementation uses Airflow's `SparkSubmitOperator` to submit Spark applications to the Spark standalone cluster.

---

# Failure Handling

DQ failures intentionally propagate back to Airflow.

For example:

```text
olist_order_payments
        │
        ▼
Data Quality
        │
        ▼
FAIL
        │
        ▼
Airflow Task FAILED
        │
        ▼
Gold task blocked
```

This provides an important foundation for future automated incident investigation.

---

# Runtime Results

DQ results are intentionally stored outside the read-only configuration directory.

```text
config/
    └── read-only configuration

results/
    └── dq/
        └── <dataset>.json
```

Docker mounts:

```text
./config
      ↓
/opt/datapilot/config:ro

./results
      ↓
/opt/datapilot/results
```

This separates:

```text
Configuration
```

from:

```text
Runtime Results
```

and prevents Spark jobs from modifying configuration files.

---

# Engineering Design Principles

DataPilot AI currently follows these principles:

### 1. Configuration Driven

Infrastructure and storage paths are centralized.

### 2. Reusable Framework

Generic Spark framework components are separated from individual jobs.

### 3. Layer Separation

Bronze, Silver, Gold, and DQ responsibilities are separated.

### 4. Immutable Raw Data

Raw source data is preserved.

### 5. Data Quality Gates

DQ failures can prevent downstream publishing.

### 6. Runtime Results Separation

Configuration is read-only while execution results are stored separately.

### 7. Orchestration Decoupling

Airflow orchestrates Spark jobs rather than embedding transformation logic inside DAG code.

### 8. Business Logic Separation

Business transformations are implemented separately from generic framework functionality.

---

# Current End-to-End Flow

The Data Engineering foundation currently works as:

```text
Olist CSV
   │
   ▼
AWS S3 Raw
   │
   ▼
Airflow
   │
   ▼
Spark Bronze
   │
   ▼
S3 Bronze Parquet
   │
   ▼
Spark Silver
   │
   ▼
S3 Silver Parquet
   │
   ▼
Data Quality
   │
   ├── PASS
   │
   └── FAIL
        │
        ▼
   DQ JSON Results
   │
   ▼
Airflow Failure
   │
   ▼
Gold Publishing Blocked
```

The Gold transformation itself has also been successfully validated manually and produces:

```text
S3 Gold
    └── olist_order_summary
```

---

# Next Phase — Agentic Data Engineering

The Data Engineering foundation is now ready for the agentic layer.

Planned architecture:

```text
                       Supervisor Agent
                              │
          ┌───────────────────┼───────────────────┐
          ▼                   ▼                   ▼
    Pipeline Agent       DQ Agent            RCA Agent
          │                   │                   │
          └───────────────────┼───────────────────┘
                              ▼
                         MCP Server
                              │
             ┌────────────────┼────────────────┐
             ▼                ▼                ▼
          Airflow         Databricks           S3
             │                │
             ▼                ▼
         Pipelines          Spark
                              │
                              ▼
                         Data Catalog
                              │
                              ▼
                             RAG
                              │
                              ▼
                        Human Approval
                              │
                              ▼
                          GitHub PR
```

Planned capabilities include:

* RCA Agent
* Data Quality Agent
* Pipeline Agent
* Supervisor Agent
* MCP-based tool execution
* Metadata and lineage
* RAG-based incident investigation
* Automated remediation
* Human approval workflows
* GitHub PR generation
* Agent evaluation
* Observability and audit logging

---

# Project Objective

The long-term objective of DataPilot AI is to demonstrate how traditional Data Engineering workflows can be enhanced with Agentic AI.

The current implementation establishes the reliable data platform foundation first:

```text
Reliable Data Engineering
          +
Data Quality
          +
Orchestration
          ↓
Agentic DataOps
```

The next phase will focus on allowing AI agents to **observe, investigate, reason about, and safely act on data engineering incidents**.
