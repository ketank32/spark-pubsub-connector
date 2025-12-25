package io.github.ketank32.spark.pubsub

import com.google.api.gax.core.FixedCredentialsProvider
import com.google.cloud.pubsub.v1.Publisher
import com.google.pubsub.v1.ProjectTopicName
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.{DataFrame, SQLContext}

class PubSubStreamingSink(sqlContext: SQLContext,
                          parameters: Map[String, String]
                         ) extends Sink {

  private val logger: Logger = LogManager.getLogger(getClass.getName)
  @volatile private var latestBatchId = -1L
  val gcsAuthFile: String = parameters.getOrElse("gcsAuthFile", throw new IllegalArgumentException("Option gcsAuthFile is required"))
  val projectId: String = parameters.getOrElse("projectId", throw new IllegalArgumentException("Option projectId is required"))
  val topicId: String = parameters.getOrElse("topicId", throw new IllegalArgumentException("Option topicId is required"))
  val credentialsProvider: FixedCredentialsProvider = PubSubUtil.getCredentialProvider(gcsAuthFile)
  val batchSize: Int = parameters.getOrElse("pubSubWriteBatchSize", "100").toInt

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    if (batchId <= latestBatchId) {
      logger.error("batchId is less than latestBatchId")
    } else {
      logger.info(s"Writing data to Pubsub Topic: $topicId and batchSize: $batchSize")
      data.collect().grouped(batchSize).foreach { batch =>
        PubSubUtil.publishBatch(projectId, topicId, batch, credentialsProvider)
      }
      latestBatchId = batchId
    }
  }

}
