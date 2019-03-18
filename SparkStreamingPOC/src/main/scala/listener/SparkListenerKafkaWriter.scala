package listener

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.scheduler._

class SparkListenerKafkaWriter(servers: String, metricsTopic: String, errorTopic: String) extends SparkListener {

  val kafkaProperties = new Properties()
  kafkaProperties.put("bootstrap.servers", servers)
  kafkaProperties.put("key.serializer", classOf[StringSerializer])
  kafkaProperties.put("value.serializer", classOf[StringSerializer])

  val producer = new KafkaProducer[String, String](kafkaProperties)

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    super.onStageCompleted(stageCompleted)
//    stageCompleted.stageInfo.taskMetrics.
//    println("")
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = super.onStageSubmitted(stageSubmitted)

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = super.onTaskStart(taskStart)

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = super.onTaskGettingResult(taskGettingResult)

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = super.onTaskEnd(taskEnd)

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = super.onJobStart(jobStart)

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = super.onJobEnd(jobEnd)

  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = super.onEnvironmentUpdate(environmentUpdate)

  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = super.onBlockManagerAdded(blockManagerAdded)

  override def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = super.onBlockManagerRemoved(blockManagerRemoved)

  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit = super.onUnpersistRDD(unpersistRDD)

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = super.onApplicationStart(applicationStart)

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = super.onApplicationEnd(applicationEnd)

  override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = super.onExecutorMetricsUpdate(executorMetricsUpdate)

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = super.onExecutorAdded(executorAdded)

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = super.onExecutorRemoved(executorRemoved)

  override def onExecutorBlacklisted(executorBlacklisted: SparkListenerExecutorBlacklisted): Unit = super.onExecutorBlacklisted(executorBlacklisted)

  override def onExecutorUnblacklisted(executorUnblacklisted: SparkListenerExecutorUnblacklisted): Unit = super.onExecutorUnblacklisted(executorUnblacklisted)

  override def onNodeBlacklisted(nodeBlacklisted: SparkListenerNodeBlacklisted): Unit = super.onNodeBlacklisted(nodeBlacklisted)

  override def onNodeUnblacklisted(nodeUnblacklisted: SparkListenerNodeUnblacklisted): Unit = super.onNodeUnblacklisted(nodeUnblacklisted)

  override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit = super.onBlockUpdated(blockUpdated)

  override def onOtherEvent(event: SparkListenerEvent): Unit = super.onOtherEvent(event)
}