package streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object Rate2Console extends App {

  val spark: SparkSession = SparkSession.builder()
    .appName("Rate2Console")
    .master("local[*]")
    .getOrCreate()

  val df = spark.readStream
    .format("rate")
    .option("rowsPerSecond", 2)
    .option("numPartitions", 2)
    .option("rampUpTime", 1)
    .load()

  val rateRawData = df.selectExpr("CAST(timestamp AS STRING)", "CAST(value AS string)")

  val processingTimeStream = rateRawData.writeStream
    .format("console")
    .queryName("Micro Batch")
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .option("checkpointLocation", "sparkCheckPoint\\Rate2Console\\cp1")
    .start()

  spark.streams.awaitAnyTermination()
}
