package streaming

import java.sql.Timestamp

import listener.KafkaMetrics
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger


object MultiStreamTODO extends App {

  val spark: SparkSession = SparkSession.builder()
    .appName("StreamingListenerKafkaNotifier")
    .master("local[*]")
    .config("spark.sql.streaming.metricsEnabled", true)
    .getOrCreate()

  val logger = LogManager.getRootLogger
  logger.setLevel(Level.ERROR)

  case class RateData(timestamp: Timestamp, value: Long)

  val df = spark.readStream
    .format("rate")
    .option("rowsPerSecond", 1)
    .option("numPartitions", 1)
    .option("rampUpTime", 2)
    .load()

  import spark.implicits._

  val rateData = df.as[RateData]
  val filteredDS = rateData.where("value < 20")
  val greaterThanDS = rateData.where("value > 21")

  val errorDS = greaterThanDS.where("value > 30")
    .map(triggerException(_))

  val stringData = filteredDS.selectExpr("CAST(timestamp AS String)", "CAST(value AS String)")

  val kafkaWriteStream1 = stringData.writeStream
    .format("kafka")
    .queryName("First Kafka Stream")
    .option("topic", "test2")
    .option("checkpointLocation", "sparkCheckPoint\\StreamingListenerKafkaNotifier\\cp1")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .start()

  val consoleDS = errorDS.selectExpr("CAST(timestamp AS String)", "CAST(value AS String)")

  consoleDS.writeStream.format("console")
    .queryName("Console stream")
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .start()

  spark.streams.addListener(new KafkaMetrics("localhost:9092", "streamingMetrics", "streamingTermination"))
  spark.streams.awaitAnyTermination()

  def triggerException(rateData: RateData): RateData = {
    throw new Exception()
    rateData
  }
}