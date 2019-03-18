package streaming

import java.sql.Timestamp

import listener.KafkaMetrics
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object SparkListenerKafkaNotifier extends App {

  val spark: SparkSession = SparkSession.builder()
    .appName("StreamingListenerKafkaNotifier")
    .master("local[*]")
    .config("spark.sql.streaming.metricsEnabled", true)
    .getOrCreate()

  case class RateData(timestamp: Timestamp, value: Long)

  val rawDF = spark.readStream
    .format("rate")
    .option("rowsPerSecond", 1)
    .option("numPartitions", 1)
    .option("rampUpTime", 2)
    .load()

  val rateDF = rawDF.selectExpr("CAST(timestamp AS String)", "CAST(value AS String)")

  val kafkaWriteStream = rateDF.writeStream
    .format("kafka")
    .queryName("First Kafka Stream")
    .option("topic", "test2")
    .option("checkpointLocation", "C:\\sparkCheckPoint\\StreamingListenerKafkaNotifier\\cp1")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .start()

  spark.streams.addListener(new KafkaMetrics("localhost:9092", "streamingMetrics", "streamingTermination"))
  spark.streams.awaitAnyTermination()
}