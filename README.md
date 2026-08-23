# 🛢️ Malaysia Offshore Gas SCADA Telemetry & Anomaly Pipeline

An end-to-end batch data engineering pipeline built with **Apache Airflow, Python, Pandas, PyArrow, Amazon S3, and Amazon Athena** to ingest, validate, partition, and query offshore gas platform SCADA telemetry (modeled after Sarawak's Central Luconia basin fields like **Kasawari & E11**).

---

## 🏗️ Architecture Overview
                 [ OpenDOSM API ]
         (Malaysia Monthly Gas Production)
                         │
                         ▼
┌───────────────────────────────────────────────────────────────┐
│                  APACHE AIRFLOW (Docker)                      │
│                                                               │
│   ┌───────────────────────────┐   ┌────────────────────────┐  │
│   │ Task 1: SCADA Generator   │   │ Task 2: Ingest DOSM    │  │
│   │ (Kasawari & E11 Fields)   │   │ (National Benchmarks)  │  │
│   └─────────────┬─────────────┘   └───────────┬────────────┘  │
│                 │                             │               │
│                 ▼                             │               │
│   ┌───────────────────────────────────────────┴────────────┐  │
│   │ Task 3: Data Quality & Anomaly Detection Logic         │  │
│   │ (Pressure Drop & Toxic H2S Gas Leak Alerts)            │  │
│   └───────────────────────────┬────────────────────────────┘  │
│                               │                               │
│                               ▼                               │
│   ┌────────────────────────────────────────────────────────┐  │
│   │ Task 4: Export Partitioned Parquet to S3               │  │
│   └───────────────────────────┬────────────────────────────┘  │
└───────────────────────────────┼───────────────────────────────┘
                                │
                                ▼
┌───────────────────────────────────────────────────────────┐
│                  AMAZON S3 DATA LAKE                      │
│                                                           │
│  s3://malaysia-offshore-scada-erlangga/gold/scada/        │
│  └── field_location=Kasawari/year=2026/month=08/*.parquet │
└─────────────────────────────┬─────────────────────────────┘
                              │
                              ▼
                  ┌───────────────────────┐
                  │    AMAZON ATHENA      │
                  │  (Serverless SQL)     │
                  └───────────────────────┘

---

## 🛠️ Tech Stack & Key Concepts

* **Orchestration:** Apache Airflow (Docker Compose)
* **Data Ingestion:** Python time-series SCADA telemetry simulator + OpenDOSM API
* **Processing Engine:** Pandas & PyArrow (Columnar Parquet conversion)
* **Cloud Storage:** Amazon S3 (Data Lake)
* **Design Pattern:** Hive-Style Partitioning (`field_location=.../year=.../month=...`) & Snappy Compression
* **Analytics Layer:** Amazon Athena (Trino / Serverless SQL Engine)

---

## 📊 Pipeline Workflow

1. **SCADA Simulation & Macro Ingestion:** Generates realistic offshore well readings (casing pressure PSI, gas flow in MMCFD, wellhead temperature, and $\text{H}_2\text{S}$ gas concentration in ppm) alongside official OpenDOSM national benchmark metrics.
2. **Data Quality & Anomaly Gate:** Identifies wellhead pressure drops ($< 1,200 \text{ PSI}$) indicating potential valve shut-ins or line leaks, and checks for dangerous $\text{H}_2\text{S}$ toxic gas spikes ($> 10.0 \text{ ppm}$).
3. **Partitioned Storage (S3 Lake):** Converts processed telemetry into compressed Parquet files and stores them in S3 following Hive-style partition paths.
4. **Serverless Analytics (Athena):** Registers partitions via DDL SQL and enables ad-hoc queries over millions of telemetry points without managing database servers.

---

## 🔍 Sample Athena SQL Analytics

```sql
-- Find High-Risk Offshore Wells (Pressure Drop or Toxic H2S Spike)
SELECT 
    field_location,
    well_id,
    casing_pressure_psi,
    gas_flow_mmcfd,
    h2s_concentration_ppm,
    operational_status,
    timestamp
FROM scada_db.offshore_gas_telemetry
WHERE operational_status != 'NORMAL'
ORDER BY casing_pressure_psi ASC;
```

🚀 How to Run Locally
1. Clone this repository:
git clone [https://github.com/5ftwt82www-oss/malaysia-offshore-scada-pipeline.git](https://github.com/5ftwt82www-oss/malaysia-offshore-scada-pipeline.git)
cd malaysia-offshore-scada-pipeline

2. Start Apache Airflow using Docker Compose:
docker compose up -d

3. Open http://localhost:8080 (Credentials: airflow / airflow) and trigger the malaysia_offshore_scada_pipeline DAG.

4. Export Parquet to S3:
docker compose exec webserver python /opt/airflow/plugins/s3_parquet_exporter.py

5. Register partitions in Amazon Athena:
MSCK REPAIR TABLE scada_db.offshore_gas_telemetry;



                  
