# Modern-Data-Stack-Logistics-4.0
Modern Data Stack for Logistics 4.0: An industrial-grade, resilient Data Platform. Features Real-time Streaming, Predictive AI, and Automated Orchestration :  Kafka | Spark MLlib | Airflow | dbt-expectations | MinIO | Grafana Loki

Executive Summary
This project implements a Resilient Data Platform designed to solve the "Black Box" problem in global supply chains. By shifting from reactive to Predictive Logistics, the platform anticipates delivery delays before they occur and ensures data integrity through Chaos Engineering.

The Architecture (Medallion & MLOps)
The platform follows the Medallion Architecture (Bronze, Silver, Gold) to ensure a clean, auditable data flow:

Ingestion & Chaos (The Source): * Apache Kafka handles high-throughput streaming of logistics events.

A custom Chaos Injector (Python) simulates data corruption and network latency to test system reliability.

Storage & Processing (The Brain):

MinIO (S3-Compatible): Acts as the Data Lake (Bronze Layer) for raw JSON persistence.

Apache Spark (MLlib): Performs real-time cleaning and runs a Predictive Model to calculate delay_probability for every shipment.

Transformation & Quality (The Gatekeeper):

PostgreSQL: Stores the structured Silver and Gold layers.

dbt (Data Build Tool): Manages SQL transformations and enforces strict data quality using dbt-expectations (blocking outliers and corrupted schemas).

Orchestration & Observability (The Control Tower):

Apache Airflow: Orchestrates the end-to-end DAG (Sensor -> dbt Run -> dbt Test).

OpenTelemetry & Grafana/Loki: Centralized logging and real-time monitoring of infrastructure health and "Chaos" events.

Key Technical Highlights for Recruiters
Infrastructure as Code: Entire stack is containerized and portable via Docker Compose.

Predictive Analytics: Real-time delay forecasting using Spark MLlib features.

Data Reliability: Implementation of Chaos Engineering to validate pipeline resilience.

Modern Governance: Full data lineage and automated testing with dbt.
