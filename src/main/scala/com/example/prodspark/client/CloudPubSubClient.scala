package com.example.prodspark.client

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.googleapis.util.Utils
import com.google.api.services.pubsub.model._
import com.google.api.services.pubsub.{Pubsub, PubsubScopes}
import com.google.common.util.concurrent.RateLimiter
import java.io.IOException
import java.net.HttpURLConnection

import com.example.prodspark.model.{CloudPubSubConfig, CloudPubSubMessage, CloudPubSubSubscription, CloudPubSubTopic}
import com.example.prodspark.util.RetryUtil
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
  * Cloud PubSub Client is designed to be instantiated per thread.
  * NOTE: use lazy instantiation for Receiver based Spark Streaming
  *
  * Singleton pattern is intentionally not used because to be used as a receiver in Spark
  * Streaming, the client object has to be serialized and sent to all executors. Google
  * PubSub and other classes used in to create this HTTP based client aren't serializable.
  *
  * @param config cloud pubsub config
  * @param client override pubsub client for unit tests
  */
class CloudPubSubClient(
  config: CloudPubSubConfig,
  client: Option[Pubsub] = None
) extends Serializable {
  private val pubsub = client.getOrElse {
    val httpTransport = Utils.getDefaultTransport
    val jsonFactory = Utils.getDefaultJsonFactory

    var credential = GoogleCredential.getApplicationDefault(httpTransport, jsonFactory)
    if (credential.createScopedRequired) {
      credential = credential.createScoped(PubsubScopes.all)
    }

    // Use custom HttpRequestInitializer for automatic retry upon failures.
    val initializer = new RetryHttpInitializerWrapper(credential)

    new Pubsub
    .Builder(httpTransport, jsonFactory, initializer)
      .setApplicationName("Spark Cloud Pubsub Connector")
      .build
  }

  // use the rate limit as the batch size if it is smaller
  private val effectiveBatchSize: Int = config.maxReceiveRate match {
    case Some(x) => if (x < config.batchSize) x else config.batchSize
    case _ => config.batchSize
  }

  private val pullRequest = new PullRequest()
    .setReturnImmediately(config.returnImmediately)
    .setMaxMessages(effectiveBatchSize)

  private lazy val receiveRateLimiter = config.maxReceiveRate.map(r => RateLimiter.create(r/1.0))

  /**
    * Create a Google PubSub Subscription. Silently continue if the
    * subscription already exists
    *
    * @param sub Subscription details
    * @throws IOException if subscription creation/start fails. Calling program
    *                     can choose to retry or bail out
    */
  @throws[IOException]
  def startSubscription(sub: CloudPubSubSubscription): Unit = {
    log.info(s"[${sub.subscriptionFQN}] Creating a new subscription on topic ${sub.topicFQN}")

    val subscriptionObject = new Subscription().setTopic(sub.topicFQN)
    try {
      pubsub
        .projects
        .subscriptions
        .create(sub.subscriptionFQN, subscriptionObject)
        .execute

    } catch {
      case jre: GoogleJsonResponseException =>
        if (jre.getDetails.getCode == HttpURLConnection.HTTP_CONFLICT) {
          log.info(s"[${sub.subscriptionFQN}] Subscription already exists")
        } else {
          log.error(jre.getDetails.getMessage)
          throw jre
        }

      case e: IOException =>
        log.error(s"[${sub.subscriptionFQN}] General exception: "+ e.getClass)
        throw e
    }
  }

  /**
    * Delete a subscription
    * NOTE: messages will be dropped unless another subscription
    * is created on the topic
    *
    * @param sub Subscription to delete
    * @throws IOException if the subscription delete fails
    */
  @throws[IOException]
  def delete(sub: CloudPubSubSubscription): Unit = {
    pubsub
      .projects
      .subscriptions
      .delete(sub.subscriptionFQN)
      .execute
    log.info(s"[${sub.subscriptionFQN}] Deleted subscription")
  }

  /**
    * Acknowledge messages so that they can be deleted from the
    * pubsub system
    *
    * NOTE: prefer batching of ackIds together rather than acking
    * for every message individually
    *
    * @param sub Subscription for which messages are acked
    * @param ackIds List of ackIds extracted from the messages
    * @throws IOException if the message ack fails
    */
  @throws[IOException]
  def sendAcks(sub: CloudPubSubSubscription, ackIds: List[String]): Unit = {
    assert(ackIds.nonEmpty)

    val ackRequest = new AcknowledgeRequest()
    ackRequest.setAckIds(ackIds.asJava)
    RetryUtil.retry(3) {
      pubsub
        .projects
        .subscriptions
        .acknowledge(sub.subscriptionFQN, ackRequest)
        .execute
    }
  }

  /**
    * Fetch messages as per default PullRequest configuration
    * Exceptions from the pubsub system will just be logged, it is recommended to
    * build some monitoring on it. The return value None does not differentiate between
    * an empty response (nothing to consume) or error, which should work well with long
    * running programs that will keep polling for new messages at fixed intervals.
    *
    * @param sub Subscription for which messages to fetch
    * @return List of messages or None
    */
  def getMessages(sub: CloudPubSubSubscription): Option[List[CloudPubSubMessage]] = {
    try {
      val msgs = pubsub
        .projects
        .subscriptions
        .pull(sub.subscriptionFQN, pullRequest)
        .execute
        .getReceivedMessages

      // msgs is of type java.util.List which can be null if
      // ReturnImmediately is set to true
      if (msgs == null || msgs.isEmpty) {
        None
      } else {
        receiveRateLimiter.map(_.acquire(msgs.size))
        Some(
          msgs
            .asScala.toList
            .filter(_.getMessage != null)
            .map(CloudPubSubMessage.fromMessage)
        )
      }
    } catch {
      case e: Exception =>
        log.error(s"[${sub.subscriptionFQN}] Error receiving messages:", e)
        None
    }
  }

  /**
    * Publish a batch of messages to a given topic
    * @param msgs batch of messages
    * @param topic topic
    * @throws IOException if all retries to publish fails
    */
  @throws[IOException]
  def publish(
    msgs: Seq[PubsubMessage],
    topic: CloudPubSubTopic
  ): Unit = {
    val req = new PublishRequest()
      .setMessages(msgs.asJava)

    log.debug(s"[${topic.topicFQN}] publishing ${msgs.length} messages")
    RetryUtil.retry(3) {
      try {
        val response = pubsub
          .projects
          .topics
          .publish(topic.topicFQN, req)
          .execute

        if (response.getMessageIds.size < msgs.length) {
          throw new IOException(s"[${topic.topicFQN}] Published ${msgs.length} " +
            s"messages, but got confirmation only for ${response.getMessageIds.size}")
        }
      } catch {
        case jre: GoogleJsonResponseException =>
          log.error(s"[${topic.topicFQN}] ${jre.getDetails.getMessage}")
          throw jre
        case e: IOException =>
          log.error(s"[${topic.topicFQN}] General exception: " + e.getClass)
          throw e
      }
    }
  }

  private val log = LoggerFactory.getLogger(this.getClass)
}
