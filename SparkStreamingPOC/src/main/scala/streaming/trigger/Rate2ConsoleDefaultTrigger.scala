package streaming.trigger

import org.apache.spark.sql.SparkSession

object Rate2ConsoleDefaultTrigger extends App {

  val spark: SparkSession = SparkSession.builder()
    .appName("Rate2ConsoleDefaultTrigger")
    .master("local[*]")
    .getOrCreate()

  val df = spark.readStream
    .format("rate")
    .option("rowsPerSecond", 90000)
//    .option("numPartitions", 1)
    .option("rampUpTime", 1)
    .load()

  val rateRawData = df.selectExpr("CAST(timestamp AS STRING)", "CAST(value AS string)")
  val defaultStream = rateRawData.writeStream
    .format("console")
    .queryName("Default")
    .option("checkpointLocation", "sparkCheckPoint\\Rate2ConsoleDefaultTrigger\\cp1")
    .start()

  spark.streams.awaitAnyTermination()
}
