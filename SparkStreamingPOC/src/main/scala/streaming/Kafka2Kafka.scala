package streaming


import org.apache.spark.sql.SparkSession

object Kafka2Kafka extends App{

  val spark : SparkSession = SparkSession.builder()
    .appName("Kafka2Kafka")
    .master("local[*]")
    .getOrCreate()

  val df = spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "test")
   .load()

   val kafkaRawData = df.selectExpr("CAST(key AS STRING)", "CAST(value AS string)", "topic", "partition", "offset", "timestamp","timestampType")
   val kafkaWriteStream = kafkaRawData.writeStream.format("kafka").option("topic", "test2")
        .option("checkpointLocation","C:\\checkpoint")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .start().awaitTermination()
}
