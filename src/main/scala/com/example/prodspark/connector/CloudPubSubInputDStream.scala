package com.example.prodspark.connector

import com.thumbtack.common.gcp.{CloudPubSubClient, CloudPubSubConfig, CloudPubSubMessage, CloudPubSubSubscription}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

private[connector] class CloudPubSubInputDStream(
  @transient ssc : StreamingContext,
  subscription: CloudPubSubSubscription,
  config: CloudPubSubConfig
) extends ReceiverInputDStream[CloudPubSubMessage](ssc) {
  override def getReceiver(): Receiver[CloudPubSubMessage] =
    new CloudPubSubReceiver(subscription, config)
}

/**
  * Receiver implementation for cloud pubsub consumer
  * @param subscription subscription to fetch messages for
  * @param storageLevel storage level in Spark Streaming - by default replication
  *                     is disabled since the write ahead log is supposed to be enabled
  * @param client for testability pass in a mock or stub pubsub client
  */
private[connector] class CloudPubSubReceiver(
  subscription: CloudPubSubSubscription,
  config: CloudPubSubConfig,
  storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER,
  client: Option[CloudPubSubClient] = None
) extends Receiver[CloudPubSubMessage](storageLevel) {

  private lazy val pubsub = client.getOrElse(new CloudPubSubClient(config))
  private val subName = subscription.subscriptionFQN

  override def onStart(): Unit = {
    log.info(s"[$subName] Starting subscription")
    try {
      pubsub.startSubscription(subscription)
      new Thread("pubsub-receiver") {
        override def run(): Unit = {
          receive()
        }
      }.start()
    } catch {
      case e: Exception => {
        // report error to driver - this will invoke onReceiverError
        // method of StreamingErrorHandler on the driver, which can then
        // signal the monitoring thread to halt the application
        reportError(s"Error starting receiver for subscription $subName ", e)
      }
    }
  }

  override def onStop(): Unit = {
  }

  def receive(): Unit = {
    while(!isStopped()) {
      try {
        val msgs = pubsub
          .getMessages(subscription)
          .getOrElse(List.empty[CloudPubSubMessage])

        if (msgs.nonEmpty) {
          // Store in Spark memory
          // For reliability, use the store(multiple-items) flavor instead of store(single-item)
          // https://spark.apache.org/docs/2.0.1/streaming-custom-receivers.html#receiver-reliability
          // This call blocks until the data is stored in the spark WAL
          store(ArrayBuffer[CloudPubSubMessage](msgs:_*))

          log.debug(s"[$subName] received ${msgs.size} messages from pubsub")
          pubsub.sendAcks(subscription, msgs.map(_.ackID))
        }
      } catch {
        case e: Exception =>
          // report error to driver (see lines 49-51)
          // application is aborted when there is any unexpected exception in receiving the
          // message, storing it in the WAL or while acking the message to pubsub
          reportError(s"Exception in receiver for $subName: ", e)
      }
    }
    log.info(s"[$subName] Message receiving stopped")
  }
  private val log = LoggerFactory.getLogger(this.getClass)
}
