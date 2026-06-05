# Flight Delay Prediction - Big Data Practice

A real-time application that predicts flight delays using a Random Forest model trained on 2015 US flight data. Users submit flight details through a Flask web interface, which sends the request via Kafka to a Spark Streaming job that applies the model and returns the prediction via Kafka WebSockets, also storing it in Cassandra.

## Based on
This project is based on [practica_creativa](https://github.com/Big-Data-ETSIT/practica_creativa) 
from Big-Data-ETSIT, which is itself based on 
[Agile Data Science 2.0](https://github.com/rjurney/Agile_Data_Code_2) 
by Russell Jurney.

---

## Part 1 — Local Deployment

The first part of the project runs all services locally. The following tools are required:

- Java 17 (via SDKMAN)
- Scala 2.12.10 (via SDKMAN)
- Apache Spark 3.5.3 (via SDKMAN)
- Apache Kafka 2.12-3.9.0
- MongoDB 7.0
- Python 3.12

<div align="center">
  <img src="images/arquitectura.png" alt="Architecture"/>
  <p><em>Figure 1: Architecture diagram created with <a href="https://app.diagrams.net/">Draw.io</a>. [Only valid for Part 1]</em></p>
</div>


> [!WARNING]
> **NOTE: This section is no longer maintained**

---

## Part 2.1 — Docker Deployment (Mid Version - 6 First Points)

The second part deploys the full architecture using Docker Compose. All services run in containers, so no local installation of Spark, Kafka, or MongoDB is required beyond Docker itself.

### Prerequisites

- Docker Engine 24+
- Docker Compose v2
- sbt 1.10.7 (to compile the Spark job JAR)
- Java 17 (required by sbt)

### Service versions (deployed via Docker Compose)

| Service | Image | Version |
|---|---|---|
| Flask (web server) | python | 3.12 |
| Apache Spark | apache/spark | 3.5.3 |
| Apache Kafka | apache/kafka | 3.8.0 |
| MongoDB | mongo | 7.0 |
| Cassandra | cassandra | 4.1 |
| MinIO (S3-compatible storage) | quay.io/minio/minio | latest |
| Prometheus | prom/prometheus | latest |
| Grafana | grafana/grafana | latest |
| Kafka Exporter | danielqsj/kafka-exporter | latest |
| Cassandra Exporter | criteord/cassandra_exporter | latest |

### Architecture overview

The full pipeline works as follows:

1. Flight data is ingested into **MinIO** using **Apache Iceberg** as a Data Lakehouse
2. A **PySpark** job trains a **Random Forest** model and saves it to MinIO
3. The **Spark Streaming** job (`MakePrediction.scala`) loads the model from MinIO and listens on Kafka
4. The user submits a flight via the **Flask** web interface
5. Flask sends the request to Kafka (`flight-delay-ml-request` topic)
6. Spark processes the request, makes the prediction and writes to:
   - Kafka (`flight-delay-ml-response` topic)
   - Cassandra (`flight_delay_ml_response` table)
7. Flask receives the result via Kafka and pushes it to the browser using **WebSockets**
8. **Prometheus** scrapes metrics from Flask, Kafka and Cassandra
9. **Grafana** displays the metrics in a pre-provisioned dashboard

### Deployment steps

**1. Compile the Spark job JAR**
```bash
cd flight_prediction
sbt assembly
cd ..
```

**2. Set environment variables**

Create a `.env` file in the project root:
```
MONGO_PASSWORD=your_password
```

**3. Download training data**
```bash
bash resources/download_data.sh
```

**4. Start all services**
```bash
docker compose up -d --build
```

**5. Create MinIO buckets(first time only)**
```bash
docker compose exec minio mc alias set local http://localhost:9000 minioadmin minioadmin
docker compose exec minio mc mb local/lakehouse
docker compose exec minio mc mb local/mlflow
```

**6. Ingest training data into MinIO/Iceberg**
```bash
docker compose exec flask python resources/ingest.py
```

**7. Train the model**
```bash
docker compose exec flask python resources/train_spark_mllib_model.py .
```

**8. Restart spark-submit to load the trained models**
```bash
docker compose restart spark-submit
```
**9. Trigger the training pipeline via Airflow (optional - runs automatically weekly)**
```bash
docker compose exec airflow-webserver airflow dags trigger flight_delay_training
```



### Accessing the services

| Service | URL | Credentials |
|---|---|---|
| Flight prediction UI | http://localhost:5001/flights/delays/predict_kafka | — |
| Spark Master UI | http://localhost:8080 | — |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Prometheus | http://localhost:9090 | — |
| Grafana | http://localhost:3000 | admin / admin |

