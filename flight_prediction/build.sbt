

name := "flight_prediction"

version := "0.1"

scalaVersion := "2.12.10"

val sparkVersion = "3.5.3"

mainClass in Compile := Some("es.upm.dit.ging.predictor.MakePrediction")

resolvers ++= Seq(
  "apache-snapshots" at "https://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.mongodb.spark" %% "mongo-spark-connector" % "10.4.1",
  "com.datastax.spark" %% "spark-cassandra-connector" % "3.5.0",
  "org.apache.hadoop" % "hadoop-aws" % "3.3.4",
  "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.262"
)

// conflictos de clases duplicadas entre JARs distintos al empaquetar. 

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "versions", "9", "module-info.class") => MergeStrategy.first
  case PathList("META-INF", "io.netty.versions.properties")       => MergeStrategy.first
  case PathList("META-INF", "org", "apache", "logging", _*)       => MergeStrategy.first
  case PathList("META-INF", "native-image", _*)                   => MergeStrategy.first
  case PathList("javax", "jdo", _*)                               => MergeStrategy.first
  case PathList("org", "apache", "hadoop", "hive", _*)            => MergeStrategy.first
  case PathList("google", "protobuf", _*)                         => MergeStrategy.first
  case x =>
    val oldStrategy = (assembly / assemblyMergeStrategy).value
    oldStrategy(x)
}
