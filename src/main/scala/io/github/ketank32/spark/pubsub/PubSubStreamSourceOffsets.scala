package io.github.ketank32.spark.pubsub

import org.apache.spark.sql.execution.streaming.{Offset, SerializedOffset}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import scala.util.control.Exception.catching

case class PubSubStreamSourceOffsets(logOffset: Long) extends Offset {
  override def json: String = {
    Serialization.write(this)(PubSubStreamSourceOffsets.format)
  }
}

  object PubSubStreamSourceOffsets {
    implicit val format = Serialization.formats(NoTypeHints)

    def apply(offset: Offset): PubSubStreamSourceOffsets = {
      offset match {
        case f: PubSubStreamSourceOffsets => f
        case SerializedOffset(str) =>
          catching(classOf[NumberFormatException]).opt {
            PubSubStreamSourceOffsets(str.toLong)
          }.getOrElse {
            Serialization.read[PubSubStreamSourceOffsets](str)
          }
        case _ =>
          throw new IllegalArgumentException(
            s"Invalid conversion from offset of ${offset.getClass} to PubSubStreamSourceOffsets")
      }
    }
  }
