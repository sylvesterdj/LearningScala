package streaming.trigger

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object Rate2ConsoleContinuousTrigger extends App {

  val spark: SparkSession = SparkSession.builder()
    .appName("Rate2ConsoleContinuousTrigger")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val df = spark.readStream
    .format("rate")
    .option("rowsPerSecond", 1)
    .option("numPartitions", 1)
    .option("rampUpTime", 1)
    .load()

  val rateRawData = df.selectExpr("CAST(timestamp AS STRING)", "CAST(value AS string)")
  val firstContinuousStream = rateRawData.writeStream
    .format("console")
    .queryName("First Continuous Stream ")
    .trigger(Trigger.Continuous("1 seconds"))
    .option("checkpointLocation", "sparkCheckPoint\\Rate2ConsoleContinuousTrigger\\cp1")
    .start()

  val secondContinuousStream = rateRawData.writeStream
    .format("console")
    .queryName("Second Continuous Stream ")
    .trigger(Trigger.Continuous("1 seconds"))
    .option("checkpointLocation", "sparkCheckPoint\\Rate2ConsoleContinuousTrigger\\cp2")
    .start()

  spark.streams.awaitAnyTermination()
}
