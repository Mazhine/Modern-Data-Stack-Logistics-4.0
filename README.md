# Modern-Data-Stack-Logistics-4.0
### Resilient Data Platform for Logistics 4.0

This project implements an industrial-grade, resilient **Modern Data Stack** designed to solve the "Black Box" problem in global supply chains. By shifting from reactive to Predictive Logistics, the platform anticipates delivery delays before they occur and ensures data integrity through **Chaos Engineering**.

---

## 🏗️ Architecture (Medallion & MLOps)
The platform follows the Medallion Architecture (Bronze, Silver, Gold) to ensure a clean, auditable data flow:

1. **Ingestion & Chaos**: 
    * **Apache Kafka** handles high-throughput streaming of logistics events.
    * A custom **Chaos Injector (Python)** simulates data corruption and network latency to test system reliability.
2. **Storage & Processing**:
    * **MinIO (S3-Compatible)**: Acts as the Data Lake (Bronze Layer) for raw JSON persistence.
    * **Apache Spark (MLlib)**: Performs real-time cleaning and runs a Predictive Model to calculate risk for every shipment.
3. **Transformation & Quality**:
    * **PostgreSQL**: Stores the structured Silver and Gold layers.
    * **dbt (Data Build Tool)**: Manages SQL transformations and enforces strict data quality using `dbt-expectations`.
4. **Orchestration & Observability**:
    * **Apache Airflow**: Orchestrates the end-to-end DAG (Sensor -> dbt Run -> dbt Test).
    * **Grafana & Loki**: Centralized logging and real-time monitoring of infrastructure health and "Chaos" events.

---

## ⚡ How to Start (One-Command Launch)

To launch the entire infrastructure (Kafka, Spark, Postgres, Airflow, MinIO, Grafana), simply run:

```bash
docker-compose up -d
```

> [!NOTE]
> Please wait **~2 minutes** after the command completes for all services to fully initialize.

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
Simulates a "hostile" data environment by injecting corrupted records.
```bash
python chaos_injector.py
```

---

## 📝 Prerequisites

- **Docker & Docker Compose**
- **Python 3.10+**
- **Dataset**: Ensure `DataCoSupplyChainDataset.csv` is present in the parent directory.

---

Developed as part of the **Logistics 4.0 PFA Project**.
