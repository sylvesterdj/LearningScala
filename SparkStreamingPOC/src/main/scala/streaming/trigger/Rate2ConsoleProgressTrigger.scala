package streaming.trigger

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object Rate2ConsoleProgressTrigger extends App {

  val spark: SparkSession = SparkSession.builder()
    .appName("Rate2ConsoleProgressTrigger")
    .master("local[*]")
    .getOrCreate()

  val df = spark.readStream
    .format("rate")
    .option("rowsPerSecond", 90000)
//    .option("numPartitions", 1)
    .option("rampUpTime", 1)
    .load()

  val rateRawData = df.selectExpr("CAST(timestamp AS STRING)", "CAST(value AS string)")
  val processingTimeStream = rateRawData.writeStream
    .format("console")
    .queryName("Micro Batch")
    .trigger(Trigger.ProcessingTime("20 seconds"))
    .option("checkpointLocation", "sparkCheckPoint\\Rate2ConsoleProgressTrigger\\cp1")
    .start()

  spark.streams.awaitAnyTermination()
}
