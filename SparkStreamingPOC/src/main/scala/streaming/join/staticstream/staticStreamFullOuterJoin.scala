package streaming.join.staticstream

import entity.RateData
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object staticStreamFullOuterJoin extends App {

  val spark: SparkSession = SparkSession.builder()
    .appName("StaticStreamFullOuterJoin")
    .master("local[*]")
    .getOrCreate()

  val df = spark.readStream
    .format("rate")
    .option("rowsPerSecond", 1)
    .option("numPartitions", 1)
    .option("rampUpTime", 1)
    .load()

  import spark.implicits._

  val rateData = df.as[RateData]
  val streamingEmployeeDS = rateData.where("value % 10 != 0")
    .withColumn("firstName", concat(lit("firstName"), rateData.col("value")))
    .withColumn("lastName", concat(lit("lastName"), rateData.col("value")))
    .withColumn("departmentId", lit(floor(rateData.col("value") / 10)))
  //    .withColumnRenamed("value", "id")

  val staticDepartmentDS = spark.read.format("csv").option("header", "true").load("src/main/resources/department.csv")

  val fullOuterJoinDS = staticDepartmentDS.join(streamingEmployeeDS, $"id" === $"departmentId", "full_outer")

  val fullOuterJoinStream = fullOuterJoinDS.writeStream
    .format("console")
    .queryName("InnerJoin")
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .option("checkpointLocation", "sparkCheckPoint\\StaticStreamFullOuterJoin\\cp1")
    .start()
}
