package test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StructType

object FileToPubSub {

  def main(args: Array[String]): Unit = {
    // Create a Spark session
    val spark = SparkSession.builder
      .appName("CsvStreamingExample")
      .master("local[*]")
      .getOrCreate()

//    spark.sparkContext.setLogLevel("WARN")

    // Define the schema for your CSV files
    val schema = new StructType()
      .add("orderId", "integer")
      .add("customerid", "integer")

    // Define the input and output directories
    val inputDirectory = "/Users/A200095816/Desktop/Ketan/Input/" // Replace with your actual input directory
    val outputDirectory = "/Users/A200095816/Desktop/Ketan/Output/" // Replace with your actual output directory

    // Read CSV files from the input directory in streaming mode
    val streamingDF = spark.readStream
      .option("header","false")
      .schema(schema)
      .csv(inputDirectory)

    // Perform any transformations if needed
    // For example, you can add a timestamp column to the streaming DataFrame
  //  val transformedDF = streamingDF.withColumn("timestamp", current_timestamp())

    // Write the streaming DataFrame to the output directory in streaming mode
    val query = streamingDF.writeStream
      .outputMode("append")
      .format("io/github/ketank32/spark/pubsub")
      .option("gcsAuthFile", "/Users/A200095816/Downloads/google.json")
      .option("projectId", "de1034-dev-halo-dil-0")
      .option("topicId", "Topic1")
      .option("checkpointLocation", "/Users/A200095816/Downloads/Checkpoint/CustomCode/PUBSUB/One8")
      .trigger(Trigger.ProcessingTime(5000))
      .start()

   /* val query = streamingDF.writeStream
      .outputMode("append") // Use 'append' mode for streaming writes
      .format("csv")
      .option("header","false")
      .option("checkpointLocation", "/Users/A200095816/Downloads/Checkpoint/CustomCode/File/") // Replace with a checkpoint directory
      .start(outputDirectory)*/

    // Wait for the streaming query to finish
    query.awaitTermination()
  }

}
