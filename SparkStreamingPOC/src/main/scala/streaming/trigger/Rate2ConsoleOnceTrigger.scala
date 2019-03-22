package streaming.trigger

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object Rate2ConsoleOnceTrigger extends App {

  val spark: SparkSession = SparkSession.builder()
    .appName("Rate2ConsoleOnceTrigger")
    .master("local[*]")
    .getOrCreate()

  val df = spark.readStream
    .format("rate")
    .option("rowsPerSecond", 1000)
    .option("numPartitions", 4)
    .option("rampUpTime", 1)
    .load()

  val rateRawData = df.selectExpr("CAST(timestamp AS STRING)", "CAST(value AS string)")
  val onceStream = rateRawData.writeStream
    .format("console")
    .queryName("Once")
    .trigger(Trigger.Once())
    .option("checkpointLocation", "sparkCheckPoint\\Rate2ConsoleOnceTrigger\\cp1")
    .start()


  spark.streams.awaitAnyTermination(100000)
}
