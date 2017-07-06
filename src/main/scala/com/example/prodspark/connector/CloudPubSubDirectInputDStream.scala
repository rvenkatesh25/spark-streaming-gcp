package com.example.prodspark.connector

import com.thumbtack.common.gcp._
import java.io.IOException
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream

/**
  * Extend the InputDStream class of Spark in order to integrate Cloud
  * Pubsub with Spark Streaming; pull messages from a Cloud Pubsub topic
  *
  * @param ssc the Spark Streaming Context object
  * @param subscription Cloud PubSub subscription info
  *                     Subscription will be created if it doesn't exist
  * @param client for testability pass in a mock or stub pubsub client
  */
private[connector] class CloudPubSubDirectInputDStream (
  @transient ssc : StreamingContext,
  subscription: CloudPubSubSubscription,
  client: Option[CloudPubSubClient] = None
) extends InputDStream[CloudPubSubMessage](ssc) {

  private lazy val pubsub = client.getOrElse(new CloudPubSubClient(CloudPubSubConfig.default))
  private val subName = subscription.subscriptionFQN

  /**
    * This method is called when streaming starts
    * Create/start pubsub subscription here
    */
  override def start() {
    log.info(s"[$subName] Starting CloudPubSubInputDStream")
    pubsub.startSubscription(subscription)
  }

  override def stop() {
  }

  /**
    * This method generates an RDD of all pubsub messages pulled
    * in the current time window.
    *
    * NOTE: this mode of consumption does not automatically acknowledge the
    * messages to Google PubSub. The called has to ack after ensuring processing
    * of the consumed messages.
    *
    * @param validTime a given time
    * @return an RDD of [[CloudPubSubMessage]]
    */
  override def compute(validTime: Time): Option[RDD[CloudPubSubMessage]] = {
    def toRDD(messages: List[CloudPubSubMessage]): RDD[CloudPubSubMessage] = {
      ssc.sparkContext.parallelize(messages)
    }

    pubsub
      .getMessages(subscription)
      .map(toRDD)
  }

  /**
    * Acknowledge the consumed messages.
    * NOTE: This will delete the messages from the pubsub system. Ensure processing
    * completion before calling ack on the messages
    *
    * If the driver crashes and restarts, the inflight messages will be replayed
    *
    * @param ackIds list of ack ids extracted from the messages
    */
  @throws[IOException]
  def ackMessages(
    ackIds: List[String]
  ): Unit = {
    pubsub
      .sendAcks(subscription, ackIds)
  }
}
