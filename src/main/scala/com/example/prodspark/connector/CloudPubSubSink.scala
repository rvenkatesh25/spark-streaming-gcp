package com.example.prodspark.connector

import java.nio.charset.StandardCharsets
import java.util.Base64

import com.example.prodspark.client.CloudPubSubClient
import com.example.prodspark.model.{CloudPubSubConfig, CloudPubSubTopic}
import com.example.prodspark.util.ListUtil._
import com.google.api.services.pubsub.model.PubsubMessage
import org.apache.spark.FutureAction
import org.apache.spark.rdd.{AsyncRDDActions, RDD}

import scala.collection.JavaConverters._

object CloudPubSubSink {
  lazy private val client = new CloudPubSubClient(CloudPubSubConfig.default)

  /**
    * @param s any string
    * @return size in bytes of the string's utf-8 representation
    */
  private[connector] def stringSize(s: String) = s.getBytes(StandardCharsets.UTF_8).length

  private[connector] def base64(s: String) =
    Base64.getEncoder.encodeToString(s.getBytes(StandardCharsets.UTF_8))

  /**
    * Convert a given json representation of a message into a pubsub
    * message. Input message is annotated with a type (e.g. event_type for
    * events) which is set as an attribute. Attributes enable downstream
    * jobs to act on data without necessarily deserializing them
    *
    * @param dataWithAttributes the tuple (data, Map(attr -> value))
    * @return the tuple (size, [[PubsubMessage]])
    */
  private[connector] def toPubSubRequestWithSize(
    dataWithAttributes: (String, Map[String, String])
  ): (Int, PubsubMessage) = {
    val encoded = base64(dataWithAttributes._1)
    (
      stringSize(encoded),
      new PubsubMessage()
        .setAttributes(dataWithAttributes._2.asJava)
        .setData(encoded)
    )
  }

  /**
    * Split the given list of messages into smaller quota-bound lists if
    * necessary, and call the publish api on the pubsub client
    * @param msgs list of tuple(size, pubsubMessage)
    * @param topic topic name
    * @param limits Pubsub publish limits, override-able for tests
    */
  def pubsubPublish(
    msgs: List[(Int, PubsubMessage)],
    topic: CloudPubSubTopic
  )(
    implicit limits: PubSubPublishLimits = PubSubPublishLimits.default
  ): Unit = {
    msgs
      .grouped(limits.MAX_MSGS_PER_REQUEST)
      .foreach { limitedSizeList =>
        limitedSizeList
          .groupedBySize(limits.REQUEST_LIMIT_BYTES)
          .foreach { c =>
            client.publish(c.map(_._2), topic)
          }
      }
  }

  /**
    * Publish to Cloud Pubsub with quota enforcement
    *
    * - Each request cannot have more than 1000 messages. Within each
    *   partition the messages are grouped into sub lists with a max
    *   limit of 1000 messages
    *
    *  - Each request cannot be more than 10MB in size. but since we cannot
    *    find out the exact size of the actual HTTP request object, we enforce
    *    that the size of the payload be less than 7MB, and use the remaining
    *    3MB as reserve space for rest of the metadata and encoding
    *
    *  - Other liimts - publish.s ack/s - are not supported
    *
    * @param dataWithAttributes (data, Map(attr -> value)) tuple, where attributes
    *                           and values are arbitrary strings
    * @param topic topic to publish to
    */
  def publish(
    dataWithAttributes: RDD[(String, Map[String, String])],
    topic: CloudPubSubTopic
  ): Unit = {
    dataWithAttributes
      .map(toPubSubRequestWithSize)
      .foreachPartition { p =>
        val msgs = p.toList
        pubsubPublish(msgs, topic)
      }
  }

  /**
    * Async version of the publish api
    *
    * Spark engine, by default, processes only one RDD at a time with massive
    * parallelism, but here we need multiple RDDs leading into their own output
    * operations to be executed in parallel. For such use cases, this async version
    * of streaming insert should be used (which uses [[AsyncRDDActions.foreachPartitionAsync]])
    *
    * NOTE: spark.scheduler.mode should be changed to FAIR (from the default FIFO)
    * in spark-defaults.conf at cluster creation
    *
    * @param dataWithAttributes (data, Map(attr -> value)) tuple, where attributes
    *                           and values are arbitrary strings
    * @param topic topic to publish to
    * @return a future object representing completion of publish
    */
  def publishAsync(
    dataWithAttributes: RDD[(String, Map[String, String])],
    topic: CloudPubSubTopic
  ): FutureAction[Unit] = {
    dataWithAttributes
      .map(toPubSubRequestWithSize)
      .foreachPartitionAsync { p =>
        val msgs = p.toList
        pubsubPublish(msgs, topic)
      }
  }
}

case class PubSubPublishLimits(
  MAX_MSGS_PER_REQUEST: Int,
  REQUEST_LIMIT_BYTES: Int
)

object PubSubPublishLimits {
  val default = PubSubPublishLimits(
    // 1000 msgs per request
    MAX_MSGS_PER_REQUEST = 1000,

    // 10MB max HTTP request size. Reserve 3MB for other fields in
    // the request object (note: this limit take into account that
    // the payload is already base64 encoded)
    // Use 7MB as payload size limit
    REQUEST_LIMIT_BYTES = 7 * 1024 * 1024
  )
}
