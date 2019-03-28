package streaming.kafka

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object Rate2KafkaMultiStream extends App {

  val spark: SparkSession = SparkSession.builder()
    .appName("Rate2KafkaMultiStream")
    .master("local[*]")
    .getOrCreate()

  val df = spark.readStream
    .format("rate")
    .option("rowsPerSecond", 20000)
    .option("numPartitions", 2)
    .option("rampUpTime", 1)
    .load()

  val rateRawData = df.selectExpr("CAST(timestamp AS STRING)", "CAST(value AS string)")
  val kafkaWriteStream1 = rateRawData.writeStream
    .queryName("First Kafka Stream")
    .format("kafka")
    .option("topic", "test2")
    .option("checkpointLocation", "sparkCheckPoint\\Rate2KafkaMultiStream\\cp1").option("kafka.bootstrap.servers", "localhost:9092")
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .start()

  val kafkaWriteStream2 = rateRawData.writeStream
    .queryName("Second Kafka Stream")
    .format("kafka")
    .option("topic", "test2")
    .option("checkpointLocation", "sparkCheckPoint\\Rate2KafkaMultiStream\\cp2")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .start()

  spark.streams.awaitAnyTermination()
}
