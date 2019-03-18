package streaming

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{StreamingQueryListener, Trigger}


object Rate2KafkaSparkListener extends App {

  val spark: SparkSession = SparkSession.builder()
    .appName("Rate2KafkaSparkListener")
    .master("local[*]")
    .config("spark.sql.streaming.metricsEnabled", true)
    .getOrCreate()

  val logger = LogManager.getRootLogger
  logger.setLevel(Level.DEBUG)

  val df = spark.readStream
    .format("rate")
//    .option("rowsPerSecond", 1)
//    .option("numPartitions", 10)
//    .option("rampUpTime", 2)
    .load()

  val rateRawData = df.selectExpr("CAST(timestamp AS STRING)", "CAST(value AS string)")
  val kafkaWriteStream1 = rateRawData.writeStream
    .format("kafka")
    .queryName("First Kafka Stream")
    .option("topic", "test2")
    .option("checkpointLocation", "C:\\sparkCheckPoint\\Rate2KafkaSparkListener\\cp1").option("kafka.bootstrap.servers", "localhost:9092")
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .start()

//  val kafkaWriteStream2 = rateRawData.writeStream
//    .format("kafka")
//    .queryName("Second Kafka Stream")
//    .option("topic", "test2")
//    .option("checkpointLocation", "C:\\sparkCheckPoint\\Rate2KafkaSparkListener\\cp2").option("kafka.bootstrap.servers", "localhost:9092")
//    .trigger(Trigger.ProcessingTime("20 seconds"))
//    .start()

  val chartListener = new StreamingQueryListener() {
    val MaxDataPoints = 100
    // a mutable reference to an immutable container to buffer n data points
    override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = ()

    override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
      val queryProgress = event.progress

      if(queryProgress.numInputRows > 0) {
        val time = queryProgress.timestamp
        val inputRowsPerSecond = queryProgress.numInputRows
        val name = queryProgress.name
        val processedRowsPerSecond = queryProgress.processedRowsPerSecond

        println("Metrics name "+ name+" time "+ time + " inputRows "+ inputRowsPerSecond + " processedRowsPerSecond "+ processedRowsPerSecond)
      }
    }

    override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = ()
  }

  spark.streams.addListener(chartListener)
  spark.streams.awaitAnyTermination()

}