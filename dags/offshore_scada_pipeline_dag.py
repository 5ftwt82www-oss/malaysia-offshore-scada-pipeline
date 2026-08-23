from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys

# Add plugins directory to Python path so Airflow can load custom functions
sys.path.append("/opt/airflow/plugins")

from scada_simulator import generate_scada_data
from dosm_ingestor import fetch_dosm_gas_benchmark
from scada_processor import process_scada_telemetry

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='malaysia_offshore_scada_pipeline',
    default_args=default_args,
    description='Offshore Gas SCADA Telemetry & Anomaly Processing (Central Luconia)',
    schedule_interval='@hourly',
    start_date=datetime(2026, 8, 20),
    catchup=False,
    tags=['offshore', 'scada', 'sarawak', 'gas_pipeline'],
) as dag:

    # Task 1: Generate Wellhead Telemetry Logs
    task_generate_scada = PythonOperator(
        task_id='simulate_scada_telemetry',
        python_callable=generate_scada_data,
    )

    # Task 2: Fetch OpenDOSM Macro Benchmarks
    task_fetch_dosm = PythonOperator(
        task_id='fetch_open_dosm_benchmark',
        python_callable=fetch_dosm_gas_benchmark,
    )

    # Task 3: Run Data Quality Checks & Anomaly Detection
    task_process_anomalies = PythonOperator(
        task_id='run_scada_data_quality_and_anomalies',
        python_callable=process_scada_telemetry,
    )

    # DAG Dependency Flow:
    # Run Task 1 & Task 2 in parallel, then run Task 3
    [task_generate_scada, task_fetch_dosm] >> task_process_anomalies