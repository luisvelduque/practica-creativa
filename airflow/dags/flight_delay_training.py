import os
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
from docker.types import Mount

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

    # Ejecuta ingest.py dentro del contenedor Flask que ya tiene PySpark y Java
    ingest = DockerOperator(
        task_id='ingest_data',
        image='practica_creativa-flask',
        command='python resources/ingest.py',
        network_mode='practica_creativa_default',
        auto_remove='success',
        mount_tmp_dir=False,
        docker_url='unix://var/run/docker.sock',
        mounts=[
            Mount(
                source=os.environ.get('PROJECT_DATA_PATH', '/app/data'),
                target='/app/data',
                type='bind'
            )
        ],
        environment={
            'CASSANDRA_HOST': 'cassandra',
            'KAFKA_HOST': 'kafka',
            'MONGO_HOST': 'mongodb',
            'MONGO_USER': 'admin',
            'MONGO_PASSWORD': os.environ.get('MONGO_PASSWORD', ''),
            'PROJECT_HOME': '/app',
        },
    )

    # Ejecuta train_spark_mllib_model.py dentro del contenedor Flask
    train = DockerOperator(
        task_id='train_model',
        image='practica_creativa-flask',
        command='python resources/train_spark_mllib_model.py .',
        network_mode='practica_creativa_default',
        auto_remove='success',
        mount_tmp_dir=False,
        docker_url='unix://var/run/docker.sock',
        environment={
            'CASSANDRA_HOST': 'cassandra',
            'KAFKA_HOST': 'kafka',
            'MONGO_HOST': 'mongodb',
            'MONGO_USER': 'admin',
            'MONGO_PASSWORD': os.environ.get('MONGO_PASSWORD', ''),
            'PROJECT_HOME': '/app',
            'MLFLOW_TRACKING_URI': 'http://mlflow:5000',
        },
    )

    ingest >> train
