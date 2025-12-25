package test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object PubSubBatch {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    val result = spark.read
      .format("io/github/ketank32/spark/pubsub")
      .option("gcsAuthFile", "/Users/A200095816/Documents/GCloud/google.json")
      .option("projectId", "de1034-dev-halo-dil-0")
      .option("subscriptionId", "Topic1-sub")
      .load

    val query = result.write
      .format("io/github/ketank32/spark/pubsub")
      .option("gcsAuthFile", "/Users/A200095816/Documents/GCloud/google.json")
      .option("projectId", "de1034-dev-halo-dil-0")
      .option("topicId", "TopicDest")
      .save()

  }

}
