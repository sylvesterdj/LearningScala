package streaming.mode

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object Rate2ConsoleDefaultMode extends App {

  val spark: SparkSession = SparkSession.builder()
    .appName("Rate2ConsoleDefaultMode")
    .master("local[*]")
    .getOrCreate()

  val logger = LogManager.getRootLogger
  logger.setLevel(Level.ERROR)

  val df = spark.readStream
    .format("rate")
    .option("rowsPerSecond", 1)
//    .option("numPartitions", 1)
    .option("rampUpTime", 1)
    .load()

  val rateRawData = df.selectExpr("CAST(timestamp AS STRING)", "CAST(value AS string)")
  val defaultStream = rateRawData.writeStream
    .format("console")
    .queryName("Default Mode")
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .option("checkpointLocation", "sparkCheckPoint\\Rate2ConsoleDefaultMode\\cp1")
    .start()

  spark.streams.awaitAnyTermination()
}
