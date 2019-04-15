package streaming.unsupported

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object Rate2ConsoleMultiStream extends App {

  val spark: SparkSession = SparkSession.builder()
    .appName("Rate2ConsoleMultiStream")
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

  val transformedData = rateRawData.withColumn("key", rateRawData.col("timestamp").substr(15,2))
  val groupedData = transformedData.groupBy("key").count()
  groupedData.createTempView("countTable")
  val countData = spark.sql("select count(*) from countTable")

  val defaultStream = countData.writeStream
    .format("console")
    .queryName("Complete Mode")
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .outputMode("complete")
    .option("checkpointLocation", "sparkCheckPoint\\Rate2ConsoleMultiStream\\cp1")
    .start()

  spark.streams.awaitAnyTermination()
}
