package test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object PubSubStreamingFile {

  def main(args: Array[String]): Unit = {


    val outputDirectory = "/Users/A200095816/Desktop/Ketan/Output/"

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    val result = spark.readStream
      .format("io/github/ketank32/spark/pubsub")
      .option("gcsAuthFile", "/Users/A200095816/Downloads/google.json")
      .option("projectId", "de1034-dev-halo-dil-0")
      .option("subscriptionId", "Topic1-sub")
      .load

    val query = result.writeStream
      .outputMode("append") // Use 'append' mode for streaming writes
      .format("csv")
      .option("header", "false")
      .option("checkpointLocation", "/Users/A200095816/Downloads/Checkpoint/CustomCode/PUBSUBB/") // Replace with a checkpoint directory
      .start(outputDirectory)

    query.awaitTermination
  }

}
