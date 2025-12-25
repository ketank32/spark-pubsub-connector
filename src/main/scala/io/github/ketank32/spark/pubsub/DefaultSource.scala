package io.github.ketank32.spark.pubsub

import com.google.api.gax.core.FixedCredentialsProvider
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.execution.streaming.{Sink, Source}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

private[pubsub] class DefaultSource extends RelationProvider with SchemaRelationProvider with CreatableRelationProvider with DataSourceRegister with StreamSourceProvider with StreamSinkProvider {

  private val logger: Logger = LogManager.getLogger(getClass.getName)

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, null)
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    parameters.get("subscriptionId") match {
      case Some(subscriptionId) => new PubSubDataSourceRelation(sqlContext, parameters)
      case None => throw new IllegalArgumentException("Option queue is needed")
    }
  }


  override def shortName(): String = "pubsub"

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    if (mode == SaveMode.Overwrite) {
      throw new UnsupportedOperationException("Data in a PubSub topic cannot be overridden! Delete the topic to implement this functionality")
    }
    val gcsAuthFile: String = parameters.getOrElse("gcsAuthFile", throw new IllegalArgumentException("Option gcsAuthFile is required"))
    val projectId: String = parameters.getOrElse("projectId", throw new IllegalArgumentException("Option projectId is required"))
    val topicId: String = parameters.getOrElse("topicId", throw new IllegalArgumentException("Option topicId is required"))
    val batchSize: Int = parameters.getOrElse("pubSubWriteBatchSize", "100").toInt
    val credentialsProvider: FixedCredentialsProvider = PubSubUtil.getCredentialProvider(gcsAuthFile)
    logger.info(s"Writing data to Pubsub Topic: $topicId with SaveMode: $mode and batchSize: $batchSize")
    data.collect().grouped(batchSize).foreach { batch =>
      PubSubUtil.publishBatch(projectId, topicId, batch, credentialsProvider)
    }
    new DataFrameRelation(sqlContext, data)
  }

  override def sourceSchema(sqlContext: SQLContext, schema: Option[StructType], providerName: String, parameters: Map[String, String]): (String, StructType) = {
    require(schema.isEmpty, "PubSub source has a fixed schema and cannot be set with a custom one")
    (shortName(),ScalaReflection.schemaFor[PubSubMessage].dataType.asInstanceOf[StructType])
  }

  override def createSource(sqlContext: SQLContext, metadataPath: String, schema: Option[StructType], providerName: String, parameters: Map[String, String]): Source = {
    new PubSubStreamingSource(sqlContext, parameters, metadataPath)
  }

  override def createSink(sqlContext: SQLContext, parameters: Map[String, String], partitionColumns: Seq[String], outputMode: OutputMode): Sink = {
    new PubSubStreamingSinkProvider().createSink(sqlContext,parameters,partitionColumns,outputMode)
  }

}
