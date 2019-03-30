package streaming.join.streamstream

import entity.RateData
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object streamStreamFullOuterJoin extends App {

  val spark: SparkSession = SparkSession.builder()
    .appName("streamStreamFullOuterJoin")
    .master("local[*]")
    .getOrCreate()

  spark.conf.set("spark.sql.shuffle.partitions", "1")

  val logger = LogManager.getRootLogger
  logger.setLevel(Level.ERROR)

  val df = spark.readStream
    .format("rate")
    .option("rowsPerSecond", 1)
    .option("numPartitions", 1)
    .option("rampUpTime", 1)
    .load()

  import spark.implicits._

  val rateData = df.as[RateData]
  val employeeStreamDS = rateData.where("value % 10 != 0")
    .withColumn("firstName",  concat(lit("firstName"),rateData.col("value")))
    .withColumn("lastName",  concat(lit("lastName"),rateData.col("value")))
    .withColumn("departmentId", lit(floor(rateData.col("value")/10)))
    .withColumnRenamed("timestamp", "empTimestamp")
    .withWatermark("empTimestamp", "10 seconds")
//    .withColumnRenamed("value", "id")

  val departmentStreamDS = rateData.where("value % 10 == 0")
    .withColumn("name", concat(lit("name"),floor(rateData.col("value")/10)))
    .withColumn("Id", lit(floor(rateData.col("value")/10)))
    .drop("value")
    .withColumnRenamed("timestamp", "depTimestamp")
    .withWatermark("depTimestamp", "10 seconds")

  val joinedDS  =  departmentStreamDS
    .join(employeeStreamDS, expr("""
      id = departmentId AND
      empTimestamp >= depTimestamp - interval 1 minutes AND
      empTimestamp <= depTimestamp + interval 1 minutes
      """
    ), "full_outer")

  val joinedStream = joinedDS.writeStream
    .format("console")
    .queryName("joinedTable")
    .option("checkpointLocation", "sparkCheckPoint\\streamStreamFullOuterJoin\\joinedTable")
    .trigger(Trigger.ProcessingTime("5 seconds"))
    .start()

  spark.streams.awaitAnyTermination()
}
