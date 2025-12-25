package test

import org.apache.avro.Schema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.avro.functions.from_avro
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.streaming.{StreamingQueryException, Trigger}
import org.apache.spark.sql.types.StructType

object PubSubStreaming {

  def main(args: Array[String]): Unit = {

    val schemaString = "{\n  \"type\": \"record\",\n  \"name\": \"topLevelRecord\",\n  \"fields\": [\n    {\n      " +
      "\"name\": \"id\",\n      \"type\": [\n        \"long\",\n        \"null\"\n      ]\n    },\n    {\n      " +
      "\"name\": \"firstname\",\n      \"type\": [\n        \"string\",\n        \"null\"\n      ]\n    },\n    {\n  " +
      "  " +
      "  \"name\": \"middlename\",\n      \"type\": [\n        \"string\",\n        \"null\"\n      ]\n    },\n    {\n      \"name\": \"lastname\",\n      \"type\": [\n        \"string\",\n        \"null\"\n      ]\n    },\n    {\n      \"name\": \"dob_year\",\n      \"type\": [\n        \"long\",\n        \"null\"\n      ]\n    },\n    {\n      \"name\": \"dob_month\",\n      \"type\": [\n        \"long\",\n        \"null\"\n      ]\n    },\n    {\n      \"name\": \"gender\",\n      \"type\": [\n        \"string\",\n        \"null\"\n      ]\n    },\n    {\n      \"name\": \"salary\",\n      \"type\": [\n        \"long\",\n        \"null\"\n      ]\n    }\n  ]\n}"
    val schema: StructType = SchemaConverters.toSqlType(new Schema.Parser().parse(schemaString)).dataType.asInstanceOf[StructType]
    val avroSchema = SchemaConverters.toAvroType(schema).toString
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    var finalDf = spark.readStream
      .format("io/github/ketank32/spark/pubsub")
      .option("gcsAuthFile", "/Users/A200095816/Documents/gcloud/google.json")
      .option("projectId", "de1034-dev-halo-dil-0")
      .option("subscriptionId", "PersonAvroTopic-sub")
      .load

    finalDf = finalDf.select(from_avro(col("data"), avroSchema).as("data")).select("data.*")

    try {
      val query = finalDf.writeStream
        .outputMode("append")
        .format("io/github/ketank32/spark/pubsub")
        .option("gcsAuthFile", "/Users/A200095816/Documents/gcloud/google.json")
        .option("projectId", "de1034-dev-halo-dil-0")
        .option("topicId", "JsonDest")
        .option("checkpointLocation", "/Users/A200095816/Downloads/cp5/")
        .trigger(Trigger.ProcessingTime(10000))
        .start()

    /*  val query = finalDf.writeStream
        .outputMode("append")
        .format("console")
        .option("checkpointLocation", "/Users/A200095816/Downloads/console/kk1")
        .trigger(Trigger.ProcessingTime(20000))
        .start()*/

      query.awaitTermination
    } catch {
      case sqe: StreamingQueryException =>
        if (sqe.cause.getMessage == "Positive number of partitions required") {
          throw new Exception("Please provide a new path for check point.")
        } else {
          throw sqe
        }
      case e: Exception =>
        throw e
    }

  }

}
