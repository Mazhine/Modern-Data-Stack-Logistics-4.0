# Logistics 4.0 Big Data Medallion Architecture

![Architecture](https://img.shields.io/badge/Architecture-Medallion-blue)
![Streaming](https://img.shields.io/badge/Streaming-Kafka%20%7C%20PySpark-orange)
![MLOps](https://img.shields.io/badge/MLOps-MLflow-green)
![CI/CD](https://img.shields.io/badge/Status-CI%2FCD%20Ready-brightgreen)

An enterprise-grade, real-time Machine Learning and Big Data streaming pipeline built for modern **Logistics 4.0** analytics. This pipeline ingests e-commerce delivery streams, natively predicts shipping delays through a stochastic Machine Learning engine, and builds a robust, three-tiered data warehouse using the Medallion Architecture.

---

## 🛠️ Technology Stack

### 1. Ingestion & Streaming
* **Apache Kafka & Zookeeper:** High-throughput streaming message broker cluster.
* **Kafka-UI:** Live operational dashboard for Kafka topic monitoring.
* **Python (Pandas):** Emulates live transactional e-commerce activity natively pushing to Kafka.

### 2. Processing & Storage (Medallion Architecture)
* **Apache PySpark (MLlib):** Real-time backend data processor. Computes predictive validations on micro-batches.
* **MinIO (S3-Compatible):** On-premise Object Storage handling the **Bronze Layer** (Raw payload dump).
* **PostgreSQL:** Handles the highly structured internal SQL data warehouse for **Silver and Gold** layers.

### 3. Orchestration & Machine Learning
* **Apache Airflow:** Automates workflow triggering (S3KeySensors tracking incoming Bronze packets).
* **dbt (Data Build Tool):** Executes SQL semantic transformations building analytical Gold Layers.
* **MLflow:** MLOps tracking server standardizing RandomForest metrics (Accuracy, F1-Score) and artifacts.

### 4. Observability & CI/CD
* **Grafana & Loki:** Production observability stack indexing internal pipeline logs.
* **GitHub Actions:** CI/CD tunnel enforcing `Flake8` PEP-8 standards and validating Docker deployments on commits.

---

## ⚙️ Prerequisites & Installation

To run this pipeline locally, ensure the following dependencies are installed on your machine:
* **Docker Engine** & **Docker Desktop** (or Compose V2).
* **Git** for version control.
* **Python 3.10+**.

The historical dataset (`DataCoSupplyChainDataset.csv`) must be placed in the parent directory of this project (`../DataCoSupplyChainDataset.csv`) to enable raw model training.

---

## 🚀 Execution & Command Reference

### Starting the Infrastructure
```powershell
docker-compose up -d --build
```
*Spins up all 14 background services (Brokers, Data Lakes, MLflow, Warehouses).*

### Training the Machine Learning Classifier (MLOps)
Before launching predictions, train the Random Forest AI on the local dataset:
```powershell
docker exec -it 03_spark_master spark-submit --master spark://spark-master:7077 /opt/bitnami/spark/jobs/train_model.py
```
*Once successful, the algorithmic weights are saved for real-time PySpark streaming inference.*

### Initiating the Live Kafka Stream
Simulate ongoing business transactions dynamically:
```powershell
# Nominal Stream (Healthy data evaluation)
python stream_manager.py --mode sain

# Stochastic Corruption (Injecting nulls, time travels, and extreme bounds)
python stream_manager.py --mode chaos

# Pure Machine Learning Focus 
python stream_manager.py --mode ia
```

### Terminating the Cluster
To kill all services securely without frying processing power:
```powershell
docker-compose down
```

---

## 🌐 Web Interfaces & Credentials

| Service | Port / URL | Username | Password | Purpose |
|---------|------------|----------|----------|---------|
| **Apache Airflow** | [localhost:8080](http://localhost:8080) | `admin` | `admin` | Dag Orchestration |
| **Adminer (PostgreSQL)** | [localhost:8081](http://localhost:8081) | `admin_logistics` | `securepassword123` | DWH Viewer |
| **Kafka UI** | [localhost:8083](http://localhost:8083) | *None* | *None* | Streaming Dashboards |
| **MinIO Console** | [localhost:9001](http://localhost:9001) | `admin` | `password` | Raw S3 Data Lake |
| **Grafana** | [localhost:3000](http://localhost:3000) | `admin` | `admin` | Loki Log Aggregation |
| **MLflow Server** | [localhost:5000](http://localhost:5000) | *None* | *None* | Trained Model Metrics |

*(Database System defaults to `postgres` and DB matches `logistics_db`)*

---

## 📁 Repository Structure Overview

* **`docker-compose.yml`**: The foundational orchestrator instantiating the entire cloud architecture footprint constraint to local limits.
* **`stream_manager.py`**: Python generative script. Simulates upstream legacy applications flushing user orders iteratively.
* **`spark_processor.py`**: Complex PySpark Consumer. Translates JSON Kafka payloads, infers Late Delivery probability via MLlib, and executes dual outputs to MinIO (Bronze) and Postgres (Silver).
* **`train_model.py`**: Batch processing algorithm. Teaches the internal classifier what constitutes a logistical delay using Big Data regressions.
* **`.github/workflows/data_pipeline_ci.yml`**: Automated auditor robot analyzing every code push to maintain Senior-level engineering standards.
* **`dags/`**: Contains Python directed acyclic graphs executed autonomously by Airflow.
* **`dbt_logistics/`**: Semantic structure holding schema `.yml` rules and SQL transformations scaling Silver streams to dimensional Gold datamarts.
* **`postgres-init/`**: Bootstrapping `.sql` scripts executed organically on the first volume instantiation of the Warehouse.
* **`spark_models/`**: State-storage folder for the trained ML binary (prevents model retraining on every reboot).
