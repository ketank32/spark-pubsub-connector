package io.github.ketank32.spark.pubsub

import scala.beans.BeanProperty

case class PubSubMessage(@BeanProperty subscription: String, @BeanProperty offset: Long, @BeanProperty data: Array[Byte]
                         , @BeanProperty publish_timestamp: Long, @BeanProperty event_timestamp: Long, @BeanProperty attributes:String,
                         @BeanProperty message_id:String)


object PubSubMessage{

  def apply(subscription: String, offset: Long, data: Array[Byte], publish_timestamp: Long, event_timestamp: Long, attributes: String, message_id: String): PubSubMessage =
    new PubSubMessage(subscription, offset, data, publish_timestamp, event_timestamp, attributes,message_id)

}
