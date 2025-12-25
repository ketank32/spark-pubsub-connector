package io.github.ketank32.spark.pubsub

import org.apache.spark.sql.execution.streaming.Offset

case class PubSubSourceOffset(id:Long) extends Offset {

  override def json(): String = id.toString

}
