package com.example.prodspark.connector

import com.example.prodspark.model.{CloudPubSubConfig, CloudPubSubMessage, CloudPubSubSubscription}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.StreamingContext

/**
  * Public API to create Direct or Receiver based input streams
  * from a Google PubSub subscription
  */
object CloudPubSubSource {

  /**
    * Create a Reliable Receiver Input stream: this will start a receiver thread on each of
    * the executors, each of which will poll the pubsub service for the given subscription
    *
    * Multiple receivers can be defined, which effectively causes the subscription to be shared
    * (a.k.a 'load-balancing' or 'queue semantics'), and is more suitable for very high volume use cases
    *
    * For fault tolerance:
    * a. spark.streaming.receiver.writeAheadLog.enable should be true
    * b. enable metadata checkpointing for driver continuity
    * c. (Recommended) enable data checkpointing if using state operations to minimize duplicates
    *
    * With WAL enabled, the store call will block until the data is persisted to disk, and
    * therefore it will be acknowledged to the pubsub thereafter.
    *
    * Streaming context start can throw [[java.io.IOException]] if the subscription create/start
    * fails with the Google pubsub system, and this will not fail the driver. Logs should be
    * monitored to alert on this scenario. (TODO: handle this case)
    *
    * @param ssc spark streaming context object
    * @param sub cloud pubsub subscription
    * @return ReceiverInputDStream of messages from the specific topic/subscription
    */
  def createStream(
    ssc: StreamingContext,
    sub: CloudPubSubSubscription,
    config: CloudPubSubConfig
  ): DStream[CloudPubSubMessage] = {
    new CloudPubSubInputDStream(ssc, sub, config)
  }

  def createStream(
    ssc: StreamingContext,
    sub: CloudPubSubSubscription,
    config: CloudPubSubConfig,
    numReceivers: Int
  ): DStream[CloudPubSubMessage] = {
    (1 to numReceivers map {i => createStream(ssc, sub, config)})
      .reduce(_ union _)
  }

  def createStream(
    ssc: StreamingContext,
    subs: Seq[CloudPubSubSubscription],
    config: CloudPubSubConfig,
    numReceivers: Int = 1
  ): DStream[CloudPubSubMessage] = {
    subs.map { sub => createStream(ssc, sub, config, numReceivers) }
      .reduce(_ union _)
  }


  /**
    * Create a Direct Input stream: this will fetch messages from gcloud pubsub
    * in the Spark Driver program itself.
    *
    * Useful in cases where the data volume is low - i.e. keeping an executor core
    * utilized with a receiver thread is an overkill
    *
    * For fault tolerance:
    * a. enable metadata checkpointing for driver conitnuity
    * b. acknowledge the messages after ensuring that processing is completed
    *
    * Streaming context start can throw [[java.io.IOException]] if the subscription create/start
    * fails with the Google pubsub system
    *
    * @param ssc spark streaming context object
    * @param sub cloud pubsub subscription
    * @return InputDStream of the messages from the specific topic/subscription
    */
  def createDirectStream(
    ssc: StreamingContext,
    sub: CloudPubSubSubscription,
    conf: CloudPubSubConfig = CloudPubSubConfig.default
  ): InputDStream[CloudPubSubMessage] = {
    new CloudPubSubDirectInputDStream(ssc, sub)
  }
}
