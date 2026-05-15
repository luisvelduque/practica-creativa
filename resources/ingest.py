import pyspark
import pyspark.sql
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType, DateType, TimestampType

import findspark
findspark.init()

APP_NAME = "ingest"

# Copia y pega de train_spark_mllib_model
schema = StructType([
    StructField("ArrDelay", DoubleType(), True),     # "ArrDelay":5.0
    StructField("CRSArrTime", TimestampType(), True),    # "CRSArrTime":"2015-12-31T03:20:00.000-08:00"
    StructField("CRSDepTime", TimestampType(), True),    # "CRSDepTime":"2015-12-31T03:05:00.000-08:00"
    StructField("Carrier", StringType(), True),     # "Carrier":"WN"
    StructField("DayOfMonth", IntegerType(), True), # "DayOfMonth":31
    StructField("DayOfWeek", IntegerType(), True),  # "DayOfWeek":4
    StructField("DayOfYear", IntegerType(), True),  # "DayOfYear":365
    StructField("DepDelay", DoubleType(), True),     # "DepDelay":14.0
    StructField("Dest", StringType(), True),        # "Dest":"SAN"
    StructField("Distance", DoubleType(), True),     # "Distance":368.0
    StructField("FlightDate", DateType(), True),    # "FlightDate":"2015-12-30T16:00:00.000-08:00"
    StructField("FlightNum", StringType(), True),   # "FlightNum":"6109"
    StructField("Origin", StringType(), True),      # "Origin":"TUS"
  ])

# Paso1: Crear la sesión de SparkSession
spark = pyspark.sql.SparkSession.builder \
        .appName(APP_NAME) \
        .config("spark.jars.packages",
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.lakehouse",
                "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.lakehouse.type", "hadoop") \
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://lakehouse/") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

# Paso2: Leer el fichero local
df = spark.read.json("data/simple_flight_delay_features.jsonl.bz2", schema=schema)

# Paso3: Crear la tabla Iceberg si no existe
spark.sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.flights (
        ArrDelay DOUBLE,
        CRSArrTime TIMESTAMP,
        CRSDepTime TIMESTAMP,
        Carrier STRING,
        DayOfMonth INT,
        DayOfWeek INT,
        DayOfYear INT,
        DepDelay DOUBLE,
        Dest STRING,
        Distance DOUBLE,
        FlightDate DATE,
        FlightNum STRING,
        Origin STRING
    )
    USING iceberg
""")


# Paso4: Escribir el DataFrame en la tabla Iceberg
df.writeTo("lakehouse.flights").append()
