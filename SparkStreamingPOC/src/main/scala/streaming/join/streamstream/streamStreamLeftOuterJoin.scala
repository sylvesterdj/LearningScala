package streaming.join.streamstream

import entity.RateData
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object streamStreamLeftOuterJoin extends App {

  val spark: SparkSession = SparkSession.builder()
    .appName("streamStreamLeftOuterJoin")
    .master("local[*]")
    .getOrCreate()

  spark.conf.set("spark.sql.shuffle.partitions", "1")

  val logger = LogManager.getRootLogger
  logger.setLevel(Level.ERROR)

  val rateSource = spark.readStream
    .format("rate")
    .option("rowsPerSecond", 10000)
    .option("numPartitions", 1)
    .option("rampUpTime", 1)
    .load()

  import spark.implicits._

  val rateSourceData = rateSource.as[RateData]
  val employeeStreamDS = rateSourceData.where("value % 10 != 0")
    .withColumn("firstName",  concat(lit("firstName"),rateSourceData.col("value")))
    .withColumn("lastName",  concat(lit("lastName"),rateSourceData.col("value")))
    .withColumn("departmentId", lit(floor(rateSourceData.col("value")/10)))
    .withColumnRenamed("timestamp", "empTimestamp")
    .withWatermark("empTimestamp", "10 seconds")
//    .withColumnRenamed("value", "id")

  val departmentStreamDS = rateSourceData.where("value % 10 == 0")
    .withColumn("name", concat(lit("name"),floor(rateSourceData.col("value")/10)))
    .withColumn("Id", lit(floor(rateSourceData.col("value")/10)))
    .drop("value")
    .withColumnRenamed("timestamp", "depTimestamp")
//    .withWatermark("depTimestamp", "10 seconds")

  val joinedDS  =  departmentStreamDS
    .join(employeeStreamDS, expr("""
      id = departmentId AND
      empTimestamp >= depTimestamp - interval 1 minutes AND
      empTimestamp <= depTimestamp + interval 1 minutes
      """
    ), "left_outer")

  val joinedStream = joinedDS.writeStream
    .format("console")
    .queryName("joinedTable")
    .option("checkpointLocation", "sparkCheckPoint\\streamStreamLeftOuterJoin\\joinedTable")
    .trigger(Trigger.ProcessingTime("20 seconds"))
    .start()

  spark.streams.awaitAnyTermination()
}
