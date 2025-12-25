package io.github.ketank32.spark.pubsub

import com.google.api.gax.core.FixedCredentialsProvider
import com.google.auth.oauth2.{GoogleCredentials, ServiceAccountCredentials}
import com.google.cloud.pubsub.v1.Publisher
import com.google.protobuf.ByteString
import com.google.pubsub.v1.{ProjectTopicName, PubsubMessage}
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.sql.Row
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

import java.io.FileInputStream

object PubSubUtil {

  private val logger: Logger = LogManager.getLogger(getClass.getName)

  def getCredentialProvider(gcsAuthFile: String): FixedCredentialsProvider = {
    val inputStream = new FileInputStream(gcsAuthFile)
    var credentialsProvider: FixedCredentialsProvider = null
    if (isServiceAccountJson(gcsAuthFile)) {
      logger.info("Provided Json is service account json")
      credentialsProvider = FixedCredentialsProvider.create(ServiceAccountCredentials.fromStream(inputStream))
    } else {
      logger.info("Provided Json is user json")
      credentialsProvider = FixedCredentialsProvider.create(GoogleCredentials.fromStream(inputStream))
    }
    credentialsProvider
  }

  private def isServiceAccountJson(gcsAuthFile: String): Boolean = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    scala.util.Try {
      val jsonString = scala.io.Source.fromFile(gcsAuthFile).mkString
      val accountType = (parse(jsonString) \ "type").extract[String]
      accountType.equalsIgnoreCase("service_account")
    } match {
      case scala.util.Success(result: Boolean) => result
      case scala.util.Failure(exception) =>
        logger.error(s"Error reading JSON file from path: $gcsAuthFile and exception: ${exception.getMessage}")
        false
    }
  }


  def publishBatch(projectId: String, topicId: String, batch: Seq[Row], credentialsProvider: FixedCredentialsProvider): Unit = {
    val topicName = ProjectTopicName.of(projectId, topicId)
    val publisherBuilder = Publisher.newBuilder(topicName).setCredentialsProvider(credentialsProvider)
    val publisher: Publisher = publisherBuilder.build()
    batch.map { record =>
      val pubsubMessage = PubsubMessage.newBuilder().setData(ByteString.copyFrom(record.toString().getBytes("UTF-8"))).build()
      publisher.publish(pubsubMessage)
    }
    publisher.shutdown()
  }
}
