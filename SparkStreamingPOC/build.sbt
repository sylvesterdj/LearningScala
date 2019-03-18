name := "SparkStreamingSample"

version := "0.1"

scalaVersion := "2.11.11"

// grading libraries
libraryDependencies ++= Seq(
  "log4j" % "log4j" % "1.2.14",
  "org.apache.kafka" % "kafka-clients" % "0.8.2.0",
  "org.apache.kafka" % "kafka-clients" % "0.8.2.0",
  "org.apache.spark" %% "spark-core" % "2.2.1",
  "org.apache.spark" %% "spark-sql" % "2.2.1",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.2.1"

)