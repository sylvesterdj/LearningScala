package streaming.state

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupStateTimeout, Trigger}

case class InputValue(timestamp: String, value: String, key: String,
                      data1: String, data2: String, data3: String)

object Rate2SparkState extends App with manageStateHelper {


  val spark: SparkSession = SparkSession.builder()
    .appName("Rate2SparkState")
    .master("local[*]")
    .getOrCreate()

  val df = spark.readStream
    .format("rate")
    .option("rowsPerSecond", 2)
    .option("numPartitions", 2)
    .option("rampUpTime", 1)
    .load()

  val rateRawData = df.selectExpr("CAST(timestamp AS STRING)", "CAST(value AS string)")

  import spark.implicits._

  val transformedData = rateRawData
    .withColumn("key", lit(1))
    .withColumn("data1", lit(constant.sampleJSON))
    .withColumn("data2", lit(constant.sampleJSON))
    .withColumn("data3", lit(constant.sampleJSON)).as[InputValue]

  import spark.implicits._

  val stateManagement = transformedData
    .groupByKey(_.key)
    .mapGroupsWithState(GroupStateTimeout.NoTimeout())(manageState)
  val processingTimeStream = stateManagement.writeStream
    .format("console")
    .outputMode("update")
    .queryName("Micro Batch")
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .option("checkpointLocation", "sparkCheckPoint\\Rate2SparkState\\cp1")
    .option("truncate", false)
    .start()

  spark.streams.awaitAnyTermination()

}
