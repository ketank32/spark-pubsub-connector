package io.github.ketank32.spark.pubsub

import com.google.api.gax.core.FixedCredentialsProvider
import com.google.cloud.pubsub.v1.stub.{GrpcSubscriberStub, SubscriberStubSettings}
import com.google.pubsub.v1.{AcknowledgeRequest, ProjectSubscriptionName, PullRequest, PullResponse}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}

import java.time.{Instant, LocalDateTime, ZoneOffset}
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class PubSubDataSourceRelation(override val sqlContext: SQLContext, parameters: Map[String, String]) extends BaseRelation with TableScan with Serializable {

  val gcsAuthFile: String = parameters.getOrElse("gcsAuthFile", throw new IllegalArgumentException("Option gcsAuthFile is required"))
  val projectId: String = parameters.getOrElse("projectId", throw new IllegalArgumentException("Option projectId is required"))
  val subscriptionId: String = parameters.getOrElse("subscriptionId", throw new IllegalArgumentException("Option subscriptionId is required"))
  private val messageCountPerBatch: String = parameters.getOrElse("messageCountPerBatch", "10")
  val subscriptionName: String = ProjectSubscriptionName.format(projectId, subscriptionId)
  val credentialsProvider: FixedCredentialsProvider = PubSubUtil.getCredentialProvider(gcsAuthFile)
  private var pullResponse: PullResponse = null
  private val subscriberStubSettings = SubscriberStubSettings.newBuilder().setTransportChannelProvider(
    SubscriberStubSettings.defaultGrpcTransportProviderBuilder().setMaxInboundMessageSize(20 * 1024 * 1024).build()).setCredentialsProvider(credentialsProvider).build()
  var subscriber: GrpcSubscriberStub = GrpcSubscriberStub.create(subscriberStubSettings)
  private val pullRequest = PullRequest.newBuilder().setMaxMessages(messageCountPerBatch.toInt).setSubscription(subscriptionName).build()


  override def schema: StructType = {
    ScalaReflection.schemaFor[PubSubMessage].dataType.asInstanceOf[StructType]
  }

  override def buildScan(): RDD[Row] = {
    val messageList: ListBuffer[PubSubMessage] = ListBuffer()
    try {
      pullResponse = subscriber.pullCallable().call(pullRequest)
      if (!pullResponse.getReceivedMessagesList.isEmpty) {
        val ackIds = pullResponse.getReceivedMessagesList.asScala.map(_.getAckId).toList
        val acknowledgeRequest = AcknowledgeRequest.newBuilder().setSubscription(subscriptionName).addAllAckIds(ackIds.asJava).build()
        subscriber.acknowledgeCallable().call(acknowledgeRequest)
        println(pullResponse.getReceivedMessagesList)
        pullResponse.getReceivedMessagesList.asScala.foreach(message => {
          val data = message.getMessage.getData.toByteArray
          val publishTime = message.getMessage.getPublishTime
          val publishTimestamp: LocalDateTime = LocalDateTime.ofEpochSecond(publishTime.getSeconds, publishTime.getNanos, ZoneOffset.UTC)
          val publishTimestampUtc: Long = publishTimestamp.toEpochSecond(ZoneOffset.UTC) * 1000 + publishTimestamp.getNano / 1000000
          val eventTimestampUtc = Instant.now().toEpochMilli
          val messageId = message.getMessage.getMessageId
          val attributes = message.getMessage.getAttributesMap.toString
          messageList += PubSubMessage(subscriptionId, -1, data, publishTimestampUtc, eventTimestampUtc, attributes, messageId)
        })
      }
      val messageDf = sqlContext.createDataFrame(messageList.toList.asJava, classOf[PubSubMessage])
      messageDf.rdd
    } finally {
      if (subscriber != null) {
        subscriber.shutdown()
      }
    }
  }
}
