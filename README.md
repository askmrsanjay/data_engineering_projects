# 🛒 E-commerce Medallion Data Lakehouse

A production-grade Data Engineering project implementing a **Medallion Architecture** (Bronze, Silver, Gold) using Apache Spark, Kafka, Iceberg, and Airflow.

## 🏗️ Architecture
- **Source**: Python Event Generator producing realistic e-commerce events.
- **Ingestion**: Apache Kafka (Distributed Streaming).
- **Processing**: Apache Spark (Structured Streaming & Batch).
- **Storage**: MinIO (S3-compatible) with **Apache Iceberg** table format.
- **Orchestration**: Apache Airflow.
- **Quality Gate**: Great Expectations.
- **Visualization**: Streamlit Dashboard.
- **Monitoring**: Prometheus & Grafana.

---

## 🚦 How to Test & Run

### 1. Ingestion & Storage
- **Kafka**: Check live events at `localhost:29092`.
- **MinIO**: View Iceberg files at `http://localhost:9001` (Admin/password123).

### 2. Processing (Medallion)
- **Monitoring**: Check the Spark Master UI at `http://localhost:8080`.
- **Streaming**: Data flows automatically from Kafka -> Bronze -> Silver -> Gold.

### 3. Orchestration & Quality
- **Airflow**: Open `http://localhost:8082`. Trigger the `ecommerce_medallion_pipeline` DAG.
- **Quality**: View task logs in Airflow to see the **Great Expectations** validation results.

### 4. Visualization (Business Dashboard)
Run the following in a new terminal:
```bash
streamlit run src/visualization/dashboard.py
```
View at `http://localhost:8501`.

### 5. Monitoring & Observability
- **Grafana**: Open `http://localhost:3000` (admin/admin).
- **Prometheus**: Open `http://localhost:9090` to query system metrics.

### 6. Governance & CI/CD
- **Unit Tests**: Run `python tests/test_pipeline.py` to verify transformation logic.
- **Maintenance**: Run `docker exec ecommerce-spark-master spark-submit src/utils/iceberg_governance.py` to clean up old snapshots and compact files.

---

## 🛠️ Project Structure
- `src/streaming/`: Continuous ingestion and live transformations.
- `src/batch/`: Scheduled reconciliation and complex aggregations.
- `src/quality/`: Data validation rules (GE).
- `src/visualization/`: Streamlit dashboard code.
- `ecommerce_docker/`: Infrastructure configuration.
