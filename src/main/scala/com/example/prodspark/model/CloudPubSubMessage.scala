package com.example.prodspark.model

import java.nio.charset.StandardCharsets

import com.google.api.services.pubsub.model.ReceivedMessage
import collection.JavaConverters._

case class CloudPubSubMessage(
  ackID: String,
  messageID: String,
  messageContent: String,
  attributes: Map[String,String] = Map.empty[String, String]
) {
  def getAttr(key: String): Option[String] = attributes.get(key)
}

object CloudPubSubMessage {
  def fromMessage(msg: ReceivedMessage): CloudPubSubMessage = {
    CloudPubSubMessage(
      msg.getAckId,
      msg.getMessage.getMessageId,
      new String(msg.getMessage.decodeData, StandardCharsets.UTF_8),
      if (msg.getMessage.getAttributes != null) {
        msg.getMessage.getAttributes.asScala.toMap
      } else {
        Map.empty[String,String]
      }
    )
  }
}

case class CloudPubSubSubscription(
  projectFQN: String,
  topicFQN: String,
  subscriptionFQN: String
)

object CloudPubSubSubscription {
  def fromShortNames(
    project: String,
    topic: String,
    subscription: String
  ): CloudPubSubSubscription = {

    // Ensure subscription validity
    require(project.nonEmpty)
    require(topic.nonEmpty)
    require(subscription.nonEmpty)

    val projectFullName: String = s"projects/$project"
    val topicFullName: String = s"$projectFullName/topics/$topic"
    val subscriptionFullName: String = s"$projectFullName/subscriptions/$subscription"

    CloudPubSubSubscription(projectFullName, topicFullName, subscriptionFullName)
  }
}

case class CloudPubSubTopic(
  projectFQN: String,
  topicFQN: String
)

object CloudPubSubTopic {
  def fromShortNames(
    project: String,
    topic: String
  ): CloudPubSubTopic = {

    // Ensure subscription validity
    require(project.nonEmpty)
    require(topic.nonEmpty)

    val projectFullName: String = s"projects/$project"
    val topicFullName: String = s"$projectFullName/topics/$topic"

    CloudPubSubTopic(projectFullName, topicFullName)
  }
}

case class CloudPubSubConfig(
  // max batch to messages to fetch in a single pull
  batchSize: Int = 1000,
  // block or empty output on pull when no messages present
  returnImmediately: Boolean = false,
  // rate at which each client instances receives messages
  maxReceiveRate: Option[Int] = None
)

object CloudPubSubConfig {
  def default = new CloudPubSubConfig
}
