# NATIONAL-GRID-INTELLIGENCE-REAL-TIME-CARBON-MONITOR

## 📖 Executive Summary
This project implements a scalable, fault-tolerant data engineering pipeline capable of ingesting, processing, and visualizing real-time energy grid data. Traditional batch-based ETL pipelines often suffer from high data latency; this solution replaces them with a modern **Event-Driven Architecture (ELT)** to reduce latency from hours to seconds.

The system streams live carbon intensity data from the **National Grid API** into **Apache Kafka**, archives raw payloads in a **MinIO Data Lake**, warehouses the data in **Snowflake**, and visualizes grid performance in a real-time **Power BI** dashboard.

## 🏗️ System Architecture
The pipeline follows a microservices architecture orchestrated by Docker Containers.



### Data Flow Breakdown
* **Extraction:** A custom Python Producer polls the National Grid API every 10 seconds for real-time carbon intensity and forecast data.
* **Buffering:** Data is pushed to **Apache Kafka**, acting as a fault-tolerant buffer to prevent data loss during API rate-limiting or network spikes.
* **Staging (Bronze):** A Python Consumer retrieves messages from Kafka and stores them as immutable raw JSON files in **MinIO** (S3-compatible storage).
* **Loading:** A migration utility transfers the JSON files from MinIO to **Snowflake** using internal stages and the `COPY INTO` command.
* **Reporting:** **Power BI** queries the Snowflake tables via **DirectQuery** to ensure zero-latency visualization of the live grid status.

## 📂 Project Structure
```text
├── greenpulse_pipeline/    
│   ├── producer.py           # Script 1: National Grid API -> Kafka Ingestion
│   ├── consumer.py           # Script 2: Kafka -> MinIO Storage (The Consumer)
│   ├── snowflake_uploader.py # Script 3: MinIO -> Snowflake Migration Utility
│   ├── docker-compose.yml    # Infrastructure (Kafka, Zookeeper, and MinIO)
│   └── silver.sql            # SQL Transformation logic

🚀 Getting Started
1. Prerequisites
Ensure you have the following installed and configured:

Docker Desktop: Allocated with at least 4GB RAM.

Python 3.9+: For running the ingestion and migration services.

Snowflake Account: A trial account with ACCOUNTADMIN privileges.

2. Infrastructure Setup
Navigate to the project directory and spin up the containerized environment:

docker-compose up -d

Note: Access the MinIO browser at http://localhost:9001 and manually create a bucket named greenpulse-bronze.

3. Snowflake Configuration
Log into your Snowflake console and execute the following SQL to prepare the environment:

USE ROLE ACCOUNTADMIN;
CREATE DATABASE IF NOT EXISTS ENERGY_PROJECT;
CREATE SCHEMA IF NOT EXISTS ENERGY_PROJECT.BRONZE;

-- Create Internal Stage and Raw Table
CREATE OR REPLACE STAGE energy_stage;
CREATE TABLE IF NOT EXISTS RAW_INTENSITY (V VARIANT);

4. Running the Data Pipeline
Execute these scripts in separate terminal windows to maintain the live stream:

Start Ingestion: python producer.py

Start Consumption: python consumer.py

Trigger Migration: python snowflake_uploader.py

🛠️ Tech Stack Details
Language: Python 3.9 (Boto3, Requests, Kafka-Python).

Containerization: Docker (Microservices orchestration).

Streaming: Apache Kafka (Distributed event streaming).

Storage: MinIO (S3-compatible local Object Storage).

Warehouse: Snowflake (Cloud Data Warehouse with VARIANT JSON support).

BI: Power BI Desktop (DirectQuery & Real-time dashboarding).

💡 Troubleshooting & Key Learnings
Decoupling Services: Using Kafka ensures the Producer remains active even if the database migration script is temporarily paused.

Schema-on-Read: Leveraging Snowflake's VARIANT type allows for immediate ingestion of nested JSON without upfront schema definitions.

Real-Time Visualization: Using DirectQuery in Power BI is essential for bypassing data caching to show the "heartbeat" of the grid.

📝 License
This project is open-source and free to use under the MIT License.
