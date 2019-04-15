package streaming.unsupported

import entity.RateData
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object UnsupportedFeatures extends App {

  val spark: SparkSession = SparkSession.builder()
    .appName("UnsupportedFeatures")
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
  val employeeDS = rateData.where("value % 10 != 0")
    .withColumn("firstName",  concat(lit("firstName"),rateData.col("value")))
    .withColumn("lastName",  concat(lit("lastName"),rateData.col("value")))
    .withColumn("departmentId", lit(floor(rateData.col("value")/10)))
//    .withColumnRenamed("value", "id")

  val departmentDS = rateData.where("value % 10 == 0")
    .withColumn("name", concat(lit("name"),floor(rateData.col("value")/10)))
    .withColumn("id", lit(floor(rateData.col("value")/10)))
    .drop("value")

  val targetDS =  departmentDS.join(employeeDS, $"id" === $"departmentId")

//  val targetDS =  departmentDS.join(employeeDS)

  val employeeStream = employeeDS.writeStream
    .format("console")
    .queryName("Employee")
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .option("checkpointLocation", "sparkCheckPoint\\UnsupportedFeatures\\employee")
    .start()

  val departmentStream = departmentDS.writeStream
    .format("console")
    .queryName("Department")
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .option("checkpointLocation", "sparkCheckPoint\\UnsupportedFeatures\\department")
    .start()


  val targetStream = targetDS.writeStream
    .format("console")
    .queryName("joinedTable")
    .trigger(Trigger.ProcessingTime("15 seconds"))
    .option("checkpointLocation", "sparkCheckPoint\\UnsupportedFeatures\\joinedTable")
    .start()

  spark.streams.awaitAnyTermination()
}
