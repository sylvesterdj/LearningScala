package streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{ProcessingTime, Trigger}

object Rate2Kafka extends App {

  val spark: SparkSession = SparkSession.builder()
    .appName("Rate2Kafka")
    .master("local[*]")
    .getOrCreate()

  val df = spark.readStream
    .format("rate")
    .option("rowsPerSecond", 150000)
    .option("numPartitions", 10)
    .option("rampUpTime", 1)
    .load()

  val rateRawData = df.selectExpr("CAST(timestamp AS STRING)", "CAST(value AS string)")
  val kafkaWriteStream1 = rateRawData.writeStream
    .format("kafka")
    .queryName("First Kafka Stream")
    .option("topic", "test2")
    .option("checkpointLocation", "C:\\sparkCheckPoint\\Rate2Kafa\\cp1").option("kafka.bootstrap.servers", "localhost:9092")
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .start()

  val kafkaWriteStream2 = rateRawData.writeStream
    .format("kafka")
    .queryName("Second Kafka Stream")
    .option("topic", "test2")
    .option("checkpointLocation", "C:\\sparkCheckPoint\\Rate2Kafa\\cp2").option("kafka.bootstrap.servers", "localhost:9092")
    .trigger(ProcessingTime("20 seconds"))
    .start()

  spark.streams.awaitAnyTermination()
}
