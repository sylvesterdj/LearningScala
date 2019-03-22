package streaming.trigger

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object Rate2ConsoleTriggerOptions extends App {

  val spark: SparkSession = SparkSession.builder()
    .appName("Rate2ConsoleTriggerOptions")
    .master("local[*]")
    .getOrCreate()

  val df = spark.readStream
    .format("rate")
    .option("rowsPerSecond", 100000)
    .option("numPartitions", 1)
    .option("rampUpTime", 1)
    .load()

  val rateRawData = df.selectExpr("CAST(timestamp AS STRING)", "CAST(value AS string)")
  val defaultStream = rateRawData.writeStream
    .format("console")
    .queryName("Default")
    .option("checkpointLocation", "sparkCheckPoint\\Rate2ConsoleTriggerOptions\\cp1")
    .start()

  val onceStream = rateRawData.writeStream
    .format("console")
    .queryName("Once")
    .trigger(Trigger.Once())
    .option("checkpointLocation", "sparkCheckPoint\\Rate2ConsoleTriggerOptions\\cp2")
    .start()

  val processingTimeStream = rateRawData.writeStream
    .format("console")
    .queryName("Micro Batch")
    .trigger(Trigger.ProcessingTime("20 seconds"))
    .option("checkpointLocation", "sparkCheckPoint\\Rate2ConsoleTriggerOptions\\cp3")
    .start()

  val countinuousTimeStream = rateRawData.writeStream
    .format("console")
    .queryName("Micro Batch")
//    .trigger(Trigger.C("20 seconds"))
    .option("checkpointLocation", "sparkCheckPoint\\Rate2ConsoleTriggerOptions\\cp3")
    .start()

  defaultStream.awaitTermination()
  onceStream.awaitTermination()
  processingTimeStream.awaitTermination()

//  spark.streams.awaitAnyTermination()
}
