package streaming.jdbc

import org.apache.spark.sql.SparkSession
import sink.JDBCSink

object WriteToPostgreSQL extends App{



 case class InputData(name:String, department:String, mail:String)

  val spark : SparkSession = SparkSession.builder()
    .appName("WriteToPostgresSQL")
    .master("local[*]")
    .getOrCreate()

  val df = spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "ci.etl.currencyCode")
    .load()

  def splitfunc(input:String):String ={
    input
  }

  val records=df.selectExpr("CAST(value AS string)")

  //println("Sassas" + records)

  val driver = ""
  val url: String="jdbc:postgresql://localhost:5432/sathish"
  val userName:String="postgres"
  val passWord:String="root"
  val writer = new JDBCSink(driver, url, userName, passWord)

  records.writeStream.foreach(writer).start().awaitTermination()

}
