# Resilient Logistics 4.0 Big Data Pipeline

A modern, real-time data engineering platform for logistics monitoring. This project implements a **Medallion Architecture** (Bronze & Silver) to ingest, process, and visualize supply chain data with a built-in "Chaos Injector" for resilience testing.

---

## ⚡ How to Start (One-Command Launch)

To launch the entire infrastructure (Kafka, Spark, Postgres, Airflow, MinIO, Grafana), simply run:

```bash
docker-compose up -d
```

> [!NOTE]
> Please wait **~2 minutes** after the command completes for all services (especially Airflow and Spark) to fully initialize.

---

## 🛠️ Service URLs & Credentials

| Service | URL | Credentials (User / Pass) |
| :--- | :--- | :--- |
| **Airflow** | [localhost:8080](http://localhost:8080) | `admin` / `admin` |
| **Adminer (DB)** | [localhost:8081](http://localhost:8081) | Server: `postgres` <br> User: `admin_logistics` <br> Pass: `securepassword123` |
| **MinIO Console** | [localhost:9001](http://localhost:9001) | `admin` / `password` |
| **Grafana** | [localhost:3000](http://localhost:3000) | `admin` / `admin` |
| **Spark Master** | [localhost:8082](http://localhost:8082) | N/A |

---

## 🚀 Running the Data Stream

Once the infrastructure is up, you can simulate data flow using the following scripts:

### 1. Standard Live Stream
Simulates real-time logistics events (projected to March 2026).
```bash
python ingestion_stream.py
```

### 2. Chaos Injection (Resilience Testing)
Simulates a "hostile" data environment by injecting corrupted records (negative totals, missing IDs, 1000-year time travel) to test the pipeline's robustness.
```bash
python chaos_injector.py
```

---

## 🏗️ Technical Architecture

- **Ingestion**: Kafka (Zookeeper) for message queuing.
- **Processing**: PySpark (Master/Worker) for streaming transformations.
- **Storage**:
    - **Bronze**: MinIO (S3-compatible) for raw JSON archival.
    - **Silver**: PostgreSQL for structured, clean logistics data.
- **Orchestration**: Apache Airflow for DAG management.
- **Observability**: Grafana & Loki for real-time monitoring and log aggregation.

---

## 📝 Prerequisites

- **Docker & Docker Compose**: To run the infrastructure.
- **Python 3.10+**: To run the ingestion/chaos scripts.
- **Dataset**: Ensure `DataCoSupplyChainDataset.csv` is present in the parent directory (or update the path in scripts).

---

Developed as part of the **Logistics 4.0 PFA Project**.
