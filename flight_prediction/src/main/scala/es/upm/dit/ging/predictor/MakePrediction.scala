package es.upm.dit.ging.predictor
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.feature.{Bucketizer, StringIndexerModel, VectorAssembler}
import org.apache.spark.sql.functions.{concat, from_json, lit}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.to_json
import org.apache.spark.sql.functions.struct
import com.datastax.spark.connector.streaming._


object MakePrediction {

  def main(args: Array[String]): Unit = {
    println("Fligth predictor starting...")

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local[*]")
      .config("spark.cassandra.connection.host", sys.env.getOrElse("CASSANDRA_HOST", "cassandra"))
      .config("spark.hadoop.fs.s3a.endpoint", sys.env.getOrElse("MINIO_ENDPOINT", "http://minio:9000"))
      .config("spark.hadoop.fs.s3a.access.key", sys.env.getOrElse("MINIO_ACCESS_KEY", "minioadmin"))
      .config("spark.hadoop.fs.s3a.secret.key", sys.env.getOrElse("MINIO_SECRET_KEY", "minioadmin"))
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .getOrCreate()
    import spark.implicits._

    //Load the arrival delay bucketizer
    val models_path = sys.env.getOrElse("MODELS_PATH", "s3a://lakehouse/models")
    val arrivalBucketizerPath = "%s/arrival_bucketizer_2.0.bin".format(models_path)
    print(arrivalBucketizerPath.toString())
    val arrivalBucketizer = Bucketizer.load(arrivalBucketizerPath)
    val columns= Seq("Carrier","Origin","Dest","Route")

    //Load all the string field vectorizer pipelines into a dict
    val stringIndexerModelPath = columns.map(n => "%s/string_indexer_model_%s.bin".format(models_path, n))
    val stringIndexerModel = stringIndexerModelPath.map(n => StringIndexerModel.load(n))
    val stringIndexerModels  = (columns zip stringIndexerModel).toMap

    // Load the numeric vector assembler
    val vectorAssemblerPath = "%s/numeric_vector_assembler_2.0.bin".format(models_path)
    val vectorAssembler = VectorAssembler.load(vectorAssemblerPath)

    // Load the classifier model
    val randomForestModelPath = "%s/spark_random_forest_classifier_2.0.bin".format(models_path)
    val rfc = RandomForestClassificationModel.load(randomForestModelPath)

    //Process Prediction Requests in Streaming
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("subscribe", "flight-delay-ml-request")
      .load()
    df.printSchema()

    val flightJsonDf = df.selectExpr("CAST(value AS STRING)")

    val struct = new StructType()
      .add("Origin", DataTypes.StringType)
      .add("FlightNum", DataTypes.StringType)
      .add("DayOfWeek", DataTypes.IntegerType)
      .add("DayOfYear", DataTypes.IntegerType)
      .add("DayOfMonth", DataTypes.IntegerType)
      .add("Dest", DataTypes.StringType)
      .add("DepDelay", DataTypes.DoubleType)
      .add("Prediction", DataTypes.StringType)
      .add("Timestamp", DataTypes.TimestampType)
      .add("FlightDate", DataTypes.DateType)
      .add("Carrier", DataTypes.StringType)
      .add("UUID", DataTypes.StringType)
      .add("Distance", DataTypes.DoubleType)
      .add("Carrier_index", DataTypes.DoubleType)
      .add("Origin_index", DataTypes.DoubleType)
      .add("Dest_index", DataTypes.DoubleType)
      .add("Route_index", DataTypes.DoubleType)

    val flightNestedDf = flightJsonDf.select(from_json($"value", struct).as("flight"))
    flightNestedDf.printSchema()

    // DataFrame for Vectorizing string fields with the corresponding pipeline for that column
    val flightFlattenedDf = flightNestedDf.selectExpr("flight.Origin",
      "flight.DayOfWeek","flight.DayOfYear","flight.DayOfMonth","flight.Dest",
      "flight.DepDelay","flight.Timestamp","flight.FlightDate",
      "flight.Carrier","flight.UUID","flight.Distance")
    flightFlattenedDf.printSchema()

    val predictionRequestsWithRouteMod = flightFlattenedDf.withColumn(
      "Route",
                concat(
                  flightFlattenedDf("Origin"),
                  lit('-'),
                  flightFlattenedDf("Dest")
                )
    )

    // Dataframe for Vectorizing numeric columns
    val flightFlattenedDf2 = flightNestedDf.selectExpr("flight.Origin",
      "flight.DayOfWeek","flight.DayOfYear","flight.DayOfMonth","flight.Dest",
      "flight.DepDelay","flight.Timestamp","flight.FlightDate",
      "flight.Carrier","flight.UUID","flight.Distance",
      "flight.Carrier_index","flight.Origin_index","flight.Dest_index","flight.Route_index")
    flightFlattenedDf2.printSchema()

    val predictionRequestsWithRouteMod2 = flightFlattenedDf2.withColumn(
      "Route",
      concat(
        flightFlattenedDf2("Origin"),
        lit('-'),
        flightFlattenedDf2("Dest")
      )
    )

    // Vectorize string fields with the corresponding pipeline for that column
    // Turn category fields into categoric feature vectors, then drop intermediate fields
    val predictionRequestsWithRoute = stringIndexerModel.map(n=>n.transform(predictionRequestsWithRouteMod))

    //Vectorize numeric columns: DepDelay, Distance and index columns
    val vectorizedFeatures = vectorAssembler.setHandleInvalid("keep").transform(predictionRequestsWithRouteMod2)

    // Inspect the vectors
    vectorizedFeatures.printSchema()

    // Drop the individual index columns
    val finalVectorizedFeatures = vectorizedFeatures
        .drop("Carrier_index")
        .drop("Origin_index")
        .drop("Dest_index")
        .drop("Route_index")

    // Inspect the finalized features
    finalVectorizedFeatures.printSchema()

    // Make the prediction
    val predictions = rfc.transform(finalVectorizedFeatures)
      .drop("Features_vec")

    // Drop the features vector and prediction metadata to give the original fields
    val finalPredictions = predictions.drop("indices").drop("values").drop("rawPrediction").drop("probability")

    // Inspect the output
    finalPredictions.printSchema()

    // Escribir en Kafka
    val kafkaOutput = finalPredictions
      .select(to_json(org.apache.spark.sql.functions.struct(finalPredictions.columns.map(finalPredictions(_)): _*)).as("value"))
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("topic", "flight-delay-ml-response")
      .option("checkpointLocation", "/tmp/kafka-checkpoint")
      .outputMode("append")
      .start()

    // Escribir en Cassandra

    val cassandraOutput = finalPredictions
      .select(
        finalPredictions("UUID").as("uuid"),
        finalPredictions("Origin").as("origin"),
        finalPredictions("Dest").as("dest"),
        finalPredictions("Carrier").as("carrier"),
        finalPredictions("DepDelay").as("depdelay"),
        finalPredictions("Prediction").as("prediction"),
        finalPredictions("Timestamp").as("timestamp")
      )
      .writeStream
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", "agile_data_science")
      .option("table", "flight_delay_ml_response")
      .option("checkpointLocation", "/tmp/cassandra-checkpoint")
      .outputMode("append")
      .start()

    kafkaOutput.awaitTermination()
    cassandraOutput.awaitTermination()
  }

}
