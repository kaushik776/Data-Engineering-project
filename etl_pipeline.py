"""
Bodybuilding Data Warehouse ETL Pipeline.
Orchestrates a Star Schema (Local + AWS S3) into a SQLite Warehouse with 
explicit Foreign Key relationships for visual table linking.
"""

import os
import boto3
import pandas as pd
import sqlite3
import io
from pathlib import Path
from dotenv import load_dotenv
from prefect import task, flow
from datetime import timedelta

# --- 0. Setup and Security ---
env_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path)

# --- 1. Define Tasks ---

@task(retries=2, retry_delay_seconds=3)
def extract_local_data(file_path: str) -> pd.DataFrame:
    """Extracts local CSV data."""
    return pd.read_csv(file_path)

@task(retries=3, retry_delay_seconds=5)
def extract_s3_data(bucket: str, key: str) -> pd.DataFrame:
    """Extracts data from S3 using secure credentials from .env."""
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_DEFAULT_REGION', 'eu-central-1')
    )
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(obj['Body'].read()))

@task
def load_to_sqlite(df_users, df_fact, df_source, df_time, db_name):
    """
    Sets up the database schema with explicit FOREIGN KEY constraints 
    to enable table linking in visualization tools.
    """
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # 1. Enable Foreign Key support in SQLite
    cursor.execute("PRAGMA foreign_keys = ON;")

    # 2. Drop existing tables to allow a clean schema recreation
    cursor.execute("DROP TABLE IF EXISTS fact_nutrition_recovery;")
    cursor.execute("DROP TABLE IF EXISTS dim_users;")
    cursor.execute("DROP TABLE IF EXISTS dim_protein_source;")
    cursor.execute("DROP TABLE IF EXISTS dim_time;")

    # 3. Create Dimension Tables (The 'Parents')
    cursor.execute("""
        CREATE TABLE dim_users (
            user_id INTEGER PRIMARY KEY,
            age INTEGER
        );
    """)

    cursor.execute("""
        CREATE TABLE dim_protein_source (
            source_id INTEGER PRIMARY KEY,
            source_name TEXT
        );
    """)

    cursor.execute("""
        CREATE TABLE dim_time (
            time_id INTEGER PRIMARY KEY,
            log_date TEXT,
            log_month INTEGER,
            log_year INTEGER
        );
    """)

    # 4. Create Fact Table (The 'Child') with explicit connections
    cursor.execute("""
        CREATE TABLE fact_nutrition_recovery (
            user_id INTEGER,
            source_id INTEGER,
            time_id INTEGER,
            protein_intake REAL,
            muscle_growth REAL,
            FOREIGN KEY (user_id) REFERENCES dim_users (user_id),
            FOREIGN KEY (source_id) REFERENCES dim_protein_source (source_id),
            FOREIGN KEY (time_id) REFERENCES dim_time (time_id)
        );
    """)

    conn.commit()

    # 5. Insert Data into the prepared schema
    df_users.to_sql('dim_users', conn, if_exists='append', index=False)
    df_source.to_sql('dim_protein_source', conn, if_exists='append', index=False)
    df_time.to_sql('dim_time', conn, if_exists='append', index=False)
    df_fact.to_sql('fact_nutrition_recovery', conn, if_exists='append', index=False)

    conn.close()

@task
def create_analytical_views(db_name: str):
    """Generates the reporting layer views."""
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    cursor.execute("DROP VIEW IF EXISTS v_source_vs_muscle_growth;")
    cursor.execute("""
        CREATE VIEW v_source_vs_muscle_growth AS
        SELECT p.source_name, AVG(f.muscle_growth) AS avg_muscle_growth
        FROM fact_nutrition_recovery f
        JOIN dim_protein_source p ON f.source_id = p.source_id
        GROUP BY p.source_name;
    """)

    cursor.execute("DROP VIEW IF EXISTS v_intake_vs_muscle_growth_monthly;")
    cursor.execute("""
        CREATE VIEW v_intake_vs_muscle_growth_monthly AS
        SELECT t.log_month, SUM(f.protein_intake) AS total_protein_intake, AVG(f.muscle_growth) AS avg_muscle_growth
        FROM fact_nutrition_recovery f
        JOIN dim_time t ON f.time_id = t.time_id
        GROUP BY t.log_month;
    """)
    
    conn.commit()
    conn.close()

# --- 2. Define the Flow ---

@flow(name="Bodybuilding Warehouse ETL Pipeline", log_prints=True)
def run_etl_pipeline():
    # Configuration
    bucket = 'project-bucket-job'
    db_name = 'bodybuilding_dw.db'
    
    print("🚀 Starting Hybrid Extraction...")
    df_users = extract_local_data('data/dim_users.csv')
    df_fact = extract_s3_data(bucket, 'fact_nutrition_recovery.csv')
    df_source = extract_s3_data(bucket, 'dim_protein_source.csv')
    df_time = extract_s3_data(bucket, 'dim_time.csv')
    
    print("💾 Loading into Star Schema (Linking Tables)...")
    load_to_sqlite(df_users, df_fact, df_source, df_time, db_name)
    
    print("📊 Generating Analytical Views...")
    create_analytical_views(db_name)
    
    print("✅ Pipeline Success!")

# --- 3. Execution Block ---

if __name__ == "__main__":
    # Runs the pipeline immediately, then every 5 minutes
    run_etl_pipeline.serve(
        name="bodybuilding-etl-deployment",
        interval=timedelta(seconds=10)
    )