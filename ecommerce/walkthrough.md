# E-commerce Data Pipeline: Phase 1 Walkthrough

This document summarizes the progress made so far in building the real-time e-commerce data lakehouse. We have successfully implemented the **Source**, **Ingestion**, and **Bronze (Raw) Storage** layers.

## 🏗️ Architecture Progress

Current state of the pipeline:
1.  **Source**: Python Event Generator (Active)
2.  **Ingestion**: Kafka Cluster (Active)
3.  **Processing**: Spark Structured Streaming (Active)
4.  **Storage**: MinIO S3-Compatible Storage with Apache Iceberg (Data Landing)

## 🛠️ Implementation Highlights

### 1. Environment (Docker)
We've spun up a multi-container environment:
- **Kafka & Zookeeper**: For message queuing.
- **Spark (Master/Worker)**: For distributed processing.
- **MinIO**: Our S3-compatible data lake.
- **Airflow & Postgres**: Ready for future orchestration.

### 2. Event Generator
Created a Python script `src/generator/event_generator.py` that simulates user traffic:
- **Schema**: Unique IDs, timestamps, user/session tracking, and multiple event types (`view`, `add_to_cart`, `purchase`).

### 3. Bronze Streaming Job
The script `src/streaming/bronze_streaming.py` performs the primary ingestion:
- Reads live JSON from Kafka.
- Writes to **Apache Iceberg** tables in MinIO.
- Uses **toTable** for schema-enforced landing.

---

## 🛑 Troubleshooting & Resolutions

During development, we encountered and resolved several technical challenges:

| Problem | Symptom | Resolution |
| :--- | :--- | :--- |
| **Spark Image Crash** | `bitnami/spark:3.4.1 not found` | Bitnami migrated tags. Switched to the rolling `bitnami/spark:3` tag. |
| **Empty Bucket** | `localhost:9000` connection refused in Spark | Spark runs *inside* Docker. Changed endpoints to internal names like `ecommerce-minio:9000`. |
| **Path Missing** | `File not found` inside container | Added volume mapping (`..:/opt/bitnami/spark/app`) to `docker-compose.yml` to share local code. |
| **Version Mismatch** | JAR download failures / Spark errors | Container uses Spark **3.3.0**. Downgraded script packages from `3.5.0` to `3.3.0`. |
| **Silent Failures** | Multiple Spark processes conflicting | Used `docker-compose restart` to clear the environment and ensure a single clean stream. |

## ✅ Proof of Work

### Data Landing in MinIO
As of the latest check, the `ecommerce-bucket` successfully contains:
- `checkpoints/`: Spark streaming state.
- `warehouse/demo/default/bronze_events/`: Actual data partitioned in Parquet/Iceberg format.

![MinIO Verification Screenshot](/C:/Users/SANJAY/.gemini/antigravity/brain/76a80018-f3ee-43df-b324-fdd02b4aad9f/uploaded_media_1769430960067.png)

---

## 🚀 Next Steps
- Implement the **Silver Layer** (Spark Batch/Streaming) for data cleaning and deduplication.
- Set up **Airflow DAGs** to automate the Medallion flow.
