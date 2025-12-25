package io.github.ketank32.spark.pubsub

import com.google.api.gax.core.FixedCredentialsProvider
import com.google.cloud.pubsub.v1.stub.{GrpcSubscriberStub, SubscriberStubSettings}
import com.google.pubsub.v1._
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import java.io._
import java.time.{Instant, LocalDateTime, ZoneOffset}
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._
import scala.util.Try

class PubSubStreamingSource(sqlContext: SQLContext,
                            parameters: Map[String, String],
                            metadataPath: String) extends Source {

  private val logger: Logger = LogManager.getLogger(getClass.getName)

  private val gcsAuthFile: String = parameters.getOrElse("gcsAuthFile",
    throw new IllegalArgumentException("Option gcsAuthFile is required"))

  private val projectId: String = parameters.getOrElse("projectId",
    throw new IllegalArgumentException("Option projectId is required"))

  private val subscriptionId: String = parameters.getOrElse("subscriptionId",
    throw new IllegalArgumentException("Option subscriptionId is required"))

  private val messageCountPerBatch: String =
    parameters.getOrElse("messageCountPerBatch", "100")

  private var acknowledgeTimeDuration: String =
    parameters.getOrElse("acknowledgeTimeDuration", "240")

  private val returnImmediatelyWhilePull: String =
    parameters.getOrElse("returnImmediatelyWhilePull", "false")

  private val maxInboundMessageSize: String =
    parameters.getOrElse("maxInboundMessageSize", "20971520")

  private val tempPath: String = parameters.getOrElse("tempPath", "")

  val subscriptionName: String = ProjectSubscriptionName.format(projectId, subscriptionId)
  val credentialsProvider: FixedCredentialsProvider = PubSubUtil.getCredentialProvider(gcsAuthFile)

  private var pullResponse: PullResponse = _
  private var subscriber: GrpcSubscriberStub = _

  private val subscriberStubSettings: SubscriberStubSettings =
    SubscriberStubSettings.newBuilder()
      .setTransportChannelProvider(
        SubscriberStubSettings
          .defaultGrpcTransportProviderBuilder()
          .setMaxInboundMessageSize(maxInboundMessageSize.toInt)
          .build()
      )
      .setCredentialsProvider(credentialsProvider)
      .build()

  private val pullRequest: PullRequest =
    PullRequest.newBuilder()
      .setMaxMessages(messageCountPerBatch.toInt)
      .setReturnImmediately(returnImmediatelyWhilePull.toBoolean)
      .setSubscription(subscriptionName)
      .build()

  private var ackIdPath: String = getAckIdPath

  override def schema: StructType =
    ScalaReflection.schemaFor[PubSubMessage].dataType.asInstanceOf[StructType]

  override def getOffset: Option[Offset] = {
    if (subscriber == null) {
      subscriber = GrpcSubscriberStub.create(subscriberStubSettings)
    }

    acknowledgePreviousAckIds(ackIdPath)

    subscriber.shutdown()
    subscriber = GrpcSubscriberStub.create(subscriberStubSettings)

    var storedOffset = readOffset()
    if (checkForNewData()) {
      storedOffset += 1
      Some(PubSubSourceOffset(storedOffset))
    } else {
      None
    }
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val spark = sqlContext.sparkSession
    import spark.implicits._

    val messageList = ListBuffer[PubSubMessage]()
    val ackIdsToAcknowledge = ListBuffer[String]()

    if (metadataPath.contains("privacy-checkpoint")) {
      logger.info("Privacy stream detected — reducing ack time")
      acknowledgeTimeDuration = "1"
    }

    if (pullResponse != null) {
      val receivedMessages = pullResponse.getReceivedMessagesList.asScala
      logger.info(s"Data pulled size: ${receivedMessages.size}")

      receivedMessages.foreach { rm =>
        val msg = rm.getMessage
        val publishTime = msg.getPublishTime

        val publishTimestamp = LocalDateTime.ofEpochSecond(
          publishTime.getSeconds,
          publishTime.getNanos,
          ZoneOffset.UTC
        )
        val publishTsMillis = publishTimestamp.toEpochSecond(ZoneOffset.UTC) * 1000 +
          publishTimestamp.getNano / 1000000

        val eventTimestamp = Instant.now.toEpochMilli

        messageList += PubSubMessage(
          subscriptionId,
          end.json().toLong,
          msg.getData.toByteArray,
          publishTsMillis,
          eventTimestamp,
          msg.getAttributesMap.toString,
          msg.getMessageId
        )

        val ackId = rm.getAckId
        ackIdsToAcknowledge += ackId

        subscriber.modifyAckDeadlineCallable.call(
          ModifyAckDeadlineRequest.newBuilder()
            .setSubscription(subscriptionName)
            .addAckIds(ackId)
            .setAckDeadlineSeconds(acknowledgeTimeDuration.toInt)
            .build()
        )
      }

      writeAckIdsToFile(ackIdsToAcknowledge, ackIdPath)
    }

    writeOffset(end.json().toLong)

    val df = messageList.toDF()

    df

  }

  private def getAckIdPath: String = {
    var resultPath: String = tempPath match {
      case p if p.nonEmpty => p
      case _ =>
        val idx = metadataPath.lastIndexOf("sources/0")
        if (idx != -1) metadataPath.substring(0, idx).stripSuffix("/") else metadataPath
    }

    if (resultPath.startsWith("file:"))
      resultPath = resultPath.substring("file:".length)

    val uniqueId = s"Unique_${Instant.now.toEpochMilli}"
    resultPath = s"$resultPath/ackIdPath/$uniqueId/ackId.txt"

    logger.info(s"AckIdPath generated: $resultPath")
    resultPath
  }

  private def writeAckIdsToFile(ackIds: Seq[String], path: String): Unit = {
    logger.info(s"Writing AckIds to path $path")
    createDirectoryIfNotExists(path)

    val writer = new PrintWriter(new BufferedWriter(new FileWriter(path, true)))
    try ackIds.foreach(writer.println) finally writer.close()
  }

  private def acknowledgePreviousAckIds(path: String): Unit = {
    val file = new File(path)
    if (file.exists()) {
      logger.info("Acknowledging previous AckIds")
      val reader = new BufferedReader(new FileReader(file))
      deleteAckFile()

      var line: String = null
      val ackList = new java.util.ArrayList[String]()
      while ({ line = reader.readLine(); line != null }) {
        ackList.add(line)
      }
      reader.close()

      subscriber.acknowledgeCallable().call(
        AcknowledgeRequest.newBuilder()
          .setSubscription(subscriptionName)
          .addAllAckIds(ackList)
          .build()
      )
    }
  }

  private def createDirectoryIfNotExists(path: String): Unit =
    Option(new File(path).getParentFile).foreach(_.mkdirs())

  override def stop(): Unit = {
    Option(subscriber).foreach(_.shutdown())
    deleteAckFile()
  }

  private def deleteAckFile(): Unit = {
    val f = new File(ackIdPath)
    if (f.exists()) {
      logger.info(s"Deleting AckIds at: $ackIdPath")
      f.delete()
    }
  }

  private def checkForNewData(): Boolean = {
    pullResponse = subscriber.pullCallable().call(pullRequest)
    !pullResponse.getReceivedMessagesList.isEmpty
  }

  private def readOffset(): Long = {
    Try {
      var path = metadataPath
      val idx = path.lastIndexOf("sources/0")
      if (idx != -1) path = path.substring(0, idx).stripSuffix("/")

      if (path.startsWith("file:")) path = path.substring("file:".length)

      val dir = new File(s"$path/offsetDetails")
      if (!dir.exists() || !dir.isDirectory) return 0

      dir.listFiles()
        .flatMap(f => Try(f.getName.toLong).toOption)
        .sorted
        .lastOption
        .getOrElse(0L)
    }.getOrElse(0L)
  }

  private def writeOffset(offset: Long): Unit = Try {
    var path = metadataPath
    val idx = path.lastIndexOf("sources/0")
    if (idx != -1) path = path.substring(0, idx).stripSuffix("/")

    if (path.startsWith("file:")) path = path.substring("file:".length)

    val dir = new File(s"$path/offsetDetails")
    if (!dir.exists()) dir.mkdirs()

    val file = new File(dir, s"$offset")
    val writer = new PrintWriter(file)
    try writer.println(offset) finally writer.close()
  }
}
