# Bodybuilding Data Warehouse ETL Pipeline

This project implements a hybrid Data Engineering pipeline that orchestrates data from local storage and AWS S3 into a relational SQLite Data Warehouse. Using **Prefect** for orchestration, the system automates the extraction, loading, and transformation (ETL) of bodybuilding nutrition and recovery data into a Star Schema for analytical reporting.

---

## Overview
This project simulates a real-world enterprise scenario where biometric user data is stored locally while high-volume activity logs are hosted in the cloud. The pipeline automates the extraction of these disparate sources, enforces referential integrity via SQL foreign keys, and builds a reporting layer to analyze the correlation between protein intake and muscle recovery.

---

## System Architecture
The pipeline is built on a modular architecture to ensure scalability and fault tolerance:

* **Extraction:** Concurrent data retrieval from local CSVs and AWS S3 using `boto3`.
* **Transformation:** Data cleaning and type-mapping via `pandas`.
* **Loading:** Star Schema insertion into `SQLite` with explicit relationship mapping (Foreign Keys).
* **Orchestration:** Automated scheduling and failure retries managed by **Prefect**.

---

## Data Warehouse Schema
The warehouse follows a **Star Schema** design, optimized for join-heavy analytical queries.

* **Fact Table:** `fact_nutrition_recovery`
    * Metrics: `protein_intake`, `muscle_growth`
    * Foreign Keys: `user_id`, `source_id`, `time_id`
* **Dimension Tables:**
    * `dim_users`: Demographic data (Local Source).
    * `dim_protein_source`: Categorical protein types (S3 Source).
    * `dim_time`: Temporal attributes for time-series analysis (S3 Source).

---

## Setup and Installation

### 1. Clone and Install
```bash
git clone [https://github.com/kaushik776/Data-Engineering-project.git](https://github.com/kaushik776/Data-Engineering-project.git)
cd Data-Engineering-project
pip install -r requirements.txt
```
