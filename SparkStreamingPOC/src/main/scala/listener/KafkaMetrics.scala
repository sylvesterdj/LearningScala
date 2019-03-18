package listener

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.sql.streaming.StreamingQueryListener

class KafkaMetrics(servers: String, metricsTopic: String, errorTopic: String) extends StreamingQueryListener {

  val kafkaProperties = new Properties()
  kafkaProperties.put("bootstrap.servers", servers)
  kafkaProperties.put("key.serializer", classOf[StringSerializer])
  kafkaProperties.put("value.serializer", classOf[StringSerializer])

  val producer = new KafkaProducer[String, String](kafkaProperties)

  def onQueryProgress(event: org.apache.spark.sql.streaming.StreamingQueryListener.QueryProgressEvent): Unit = {
    producer.send(new ProducerRecord(metricsTopic, event.progress.json))
  }
  def onQueryStarted(event: org.apache.spark.sql.streaming.StreamingQueryListener.QueryStartedEvent): Unit = {}
  def onQueryTerminated(event: org.apache.spark.sql.streaming.StreamingQueryListener.QueryTerminatedEvent): Unit = {
    producer.send(new ProducerRecord(errorTopic, event.exception.get))
  }
}