package streaming.join.staticstream

import entity.RateData
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import streaming.join.streamstream.streamStreamLeftOuterJoin.spark

object staticStreamInnerJoin extends App {

  val spark: SparkSession = SparkSession.builder()
    .appName("StaticStreamInnerJoin")
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
  val streamingEmployeeDS = rateData.where("value % 10 != 0")
    .withColumn("firstName",  concat(lit("firstName"),rateData.col("value")))
    .withColumn("lastName",  concat(lit("lastName"),rateData.col("value")))
    .withColumn("departmentId", lit(floor(rateData.col("value")/10)))

  val staticDepartmentDS = spark.read.format("csv").option("header","true").load("src/main/resources/department.csv")

  val innerJoinDS =  staticDepartmentDS.join(streamingEmployeeDS, $"id" === $"departmentId")

  val innerJoinStream = innerJoinDS.writeStream
    .format("console")
    .queryName("InnerJoin")
    .trigger(Trigger.ProcessingTime("20 seconds"))
    .option("checkpointLocation", "sparkCheckPoint\\StaticStreamInnerJoin\\cp1")
    .start()

  spark.streams.awaitAnyTermination()
}
