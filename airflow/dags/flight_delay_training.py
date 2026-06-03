from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess


def run_ingestion():
    """Ingest flight data into MinIO/Iceberg lakehouse."""
    subprocess.run(["python", "/app/resources/ingest.py"], check=True)


def run_training():
    """Train the RandomForest model and log results to MLflow."""
    subprocess.run(
        ["python", "/app/resources/train_spark_mllib_model.py", "."],
        check=True
    )


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'flight_delay_training',
    default_args=default_args,
    description='Pipeline de entrenamiento del modelo de retraso de vuelos',
    schedule_interval='@weekly',
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    ingest = PythonOperator(
        task_id='ingest_data',
        python_callable=run_ingestion,
    )

    train = PythonOperator(
        task_id='train_model',
        python_callable=run_training,
    )

    ingest >> train  # ingest primero, luego entrenamiento
