package streaming.kafka

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object Rate2Kafka extends App {

  val spark: SparkSession = SparkSession.builder()
    .appName("Rate2Kafka")
    .master("local[*]")
    .getOrCreate()

  val df = spark.readStream
    .format("rate")
    .option("rowsPerSecond", 2)
    .option("numPartitions", 2)
    .option("rampUpTime", 1)
    .load()

  val rateRawData = df.selectExpr("CAST(timestamp AS STRING)", "CAST(value AS string)")
  val kafkaStream = rateRawData.writeStream
    .format("kafka")
    .queryName("First Kafka Stream")
    .option("topic", "test2")
    .option("checkpointLocation", "sparkCheckPoint\\Rate2Kafka\\cp1").option("kafka.bootstrap.servers", "localhost:9092")
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .start()

  kafkaStream.awaitTermination()
}
