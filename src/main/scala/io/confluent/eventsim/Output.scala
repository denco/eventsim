package io.confluent.eventsim

import java.io.{File, FileOutputStream}
import java.time.ZoneOffset
import java.util.Properties

import com.fasterxml.jackson.core.JsonEncoding
import io.confluent.eventsim.config.ConfigFromFile
import io.confluent.eventsim.events.Auth.Constructor
import io.confluent.eventsim.events.StatusChange.{AvroConstructor, JSONConstructor}
import org.apache.avro.file.DataFileWriter
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.slf4j.{Logger, LoggerFactory}
import software.amazon.awssdk.auth.credentials.{DefaultCredentialsProvider, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region.EU_CENTRAL_1
import software.amazon.awssdk.services.sns.SnsClient
import software.amazon.awssdk.services.sns.model.{CreateTopicRequest, PublishRequest}

/**
  * @author jadler @ 1/13/16.
  * @author denco
  * @version 0.3
  * @since 2019-01-22
  */
object Output {

  // place to put all the output related code

  val fileNameExtension: String =
    if (Main.useAvro) ".avro"
    else ".json"
  val authConstructor: Constructor =
    if (Main.useAvro) new io.confluent.eventsim.events.Auth.AvroConstructor()
    else new io.confluent.eventsim.events.Auth.JSONConstructor()
  val listenConstructor: io.confluent.eventsim.events.Listen.Constructor =
    if (Main.useAvro) new io.confluent.eventsim.events.Listen.AvroConstructor()
    else new io.confluent.eventsim.events.Listen.JSONConstructor()
  val pageViewConstructor: io.confluent.eventsim.events.PageView.Constructor =
    if (Main.useAvro) new io.confluent.eventsim.events.PageView.AvroConstructor()
    else new io.confluent.eventsim.events.PageView.JSONConstructor()
  val statusChangeConstructor: io.confluent.eventsim.events.StatusChange.Constructor =
    if (Main.useAvro) new AvroConstructor()
    else new JSONConstructor()
  val kbl = Main.ConfFromOptions.kafkaBrokerList
  val pushToSNS = Main.ConfFromOptions.pushToSNS
  val dirName = new File(if (Main.ConfFromOptions.outputDir.isSupplied) Main.ConfFromOptions.outputDir.get.get else "output")
  val authOutputName = "auth_events"
  val listenOutputName = "listen_events"
  val pageViewOutputName = "page_view_events"
  val statusChangeOutputName = "status_change_events"

  if (!dirName.exists())
    dirName.mkdir()
  val authEventWriter =
    if (kbl.isSupplied) new KafkaEventWriter(authConstructor, authOutputName, kbl.toOption.get)
    else if (pushToSNS.toOption.get) new SNSEventWriter(authConstructor, authOutputName)
    else new FileEventWriter(authConstructor, new File(dirName, authOutputName + fileNameExtension))
  val listenEventWriter =
    if (kbl.isSupplied) new KafkaEventWriter(listenConstructor, listenOutputName, kbl.toOption.get)
    else if (pushToSNS.toOption.get) new SNSEventWriter(listenConstructor, listenOutputName)
    else new FileEventWriter(listenConstructor, new File(dirName, listenOutputName + fileNameExtension))
  val pageViewEventWriter =
    if (kbl.isSupplied) new KafkaEventWriter(pageViewConstructor, pageViewOutputName, kbl.toOption.get)
    else if (pushToSNS.toOption.get) new SNSEventWriter(pageViewConstructor, pageViewOutputName)
    else new FileEventWriter(pageViewConstructor, new File(dirName, pageViewOutputName + fileNameExtension))
  val statusChangeEventWriter =
    if (kbl.isSupplied) new KafkaEventWriter(statusChangeConstructor, statusChangeOutputName, kbl.toOption.get)
    else if (pushToSNS.toOption.get) new SNSEventWriter(statusChangeConstructor, statusChangeOutputName)
    else new FileEventWriter(statusChangeConstructor, new File(dirName, statusChangeOutputName + fileNameExtension))

  def flushAndClose(): Unit = {
    authEventWriter.flushAndClose()
    listenEventWriter.flushAndClose()
    pageViewEventWriter.flushAndClose()
    statusChangeEventWriter.flushAndClose()
  }

  def writeEvents(session: Session, device: Map[String, Any], userId: Int, props: Map[String, Any]) = {

    val showUserDetails = ConfigFromFile.showUserWithState(session.currentState.auth)
    pageViewConstructor.start
    pageViewConstructor.setTs(session.nextEventTimeStamp.get.toInstant(ZoneOffset.UTC) toEpochMilli())
    pageViewConstructor.setSessionId(session.sessionId)
    pageViewConstructor.setPage(session.currentState.page)
    pageViewConstructor.setAuth(session.currentState.auth)
    pageViewConstructor.setMethod(session.currentState.method)
    pageViewConstructor.setStatus(session.currentState.status)
    pageViewConstructor.setLevel(session.currentState.level)
    pageViewConstructor.setItemInSession(session.itemInSession)
    pageViewConstructor.setDeviceDetails(device)
    if (showUserDetails) {
      pageViewConstructor.setUserId(userId)
      pageViewConstructor.setUserDetails(props)
      if (Main.tag.isDefined)
        pageViewConstructor.setTag(Main.tag.get)
    }

    if (session.currentState.page == "NextSong") {
      pageViewConstructor.setArtist(session.currentSong.get._2)
      pageViewConstructor.setTitle(session.currentSong.get._3)
      pageViewConstructor.setDuration(session.currentSong.get._4)
      listenConstructor.start()
      listenConstructor.setArtist(session.currentSong.get._2)
      listenConstructor.setTitle(session.currentSong.get._3)
      listenConstructor.setDuration(session.currentSong.get._4)
      listenConstructor.setTs(session.nextEventTimeStamp.get.toInstant(ZoneOffset.UTC) toEpochMilli())
      listenConstructor.setSessionId(session.sessionId)
      listenConstructor.setAuth(session.currentState.auth)
      listenConstructor.setLevel(session.currentState.level)
      listenConstructor.setItemInSession(session.itemInSession)
      listenConstructor.setDeviceDetails(device)
      if (showUserDetails) {
        listenConstructor.setUserId(userId)
        listenConstructor.setUserDetails(props)
        if (Main.tag.isDefined)
          listenConstructor.setTag(Main.tag.get)
      }
      listenEventWriter.write
    }

    if (session.currentState.page == "Submit Downgrade" || session.currentState.page == "Submit Upgrade") {
      statusChangeConstructor.start()
      statusChangeConstructor.setTs(session.nextEventTimeStamp.get.toInstant(ZoneOffset.UTC) toEpochMilli())
      statusChangeConstructor.setSessionId(session.sessionId)
      statusChangeConstructor.setAuth(session.currentState.auth)
      statusChangeConstructor.setLevel(session.currentState.level)
      statusChangeConstructor.setItemInSession(session.itemInSession)
      statusChangeConstructor.setDeviceDetails(device)
      if (showUserDetails) {
        statusChangeConstructor.setUserId(userId)
        statusChangeConstructor.setUserDetails(props)
        if (Main.tag.isDefined)
          statusChangeConstructor.setTag(Main.tag.get)
      }
      statusChangeEventWriter.write
    }

    if (session.previousState.isDefined && session.previousState.get.page == "Login") {
      authConstructor.start()
      authConstructor.setTs(session.nextEventTimeStamp.get.toInstant(ZoneOffset.UTC) toEpochMilli())
      authConstructor.setSessionId(session.sessionId)
      authConstructor.setLevel(session.currentState.level)
      authConstructor.setItemInSession(session.itemInSession)
      authConstructor.setDeviceDetails(device)
      if (showUserDetails) {
        authConstructor.setUserId(userId)
        authConstructor.setUserDetails(props)
        if (Main.tag.isDefined)
          authConstructor.setTag(Main.tag.get)
      }
      authConstructor.setSuccess(session.currentState.auth == "Logged In")
      authEventWriter.write
    }

    pageViewEventWriter.write
  }

  trait canwrite {

    val log: Logger = LoggerFactory.getLogger(this.getClass.getSimpleName)

    def write()

    def flushAndClose()
  }

  private class FileEventWriter(val constructor: events.Constructor, val file: File) extends Object with canwrite {

    val out = if (Main.useAvro) {
      val avro = constructor.asInstanceOf[events.AvroConstructor[Object]]
      val avro_out = new DataFileWriter[Object](avro.datumWriter)
      avro_out.create(avro.schema, file)
    } else {
      new FileOutputStream(file)
    }

    override def write(): Unit = {
      val value: Object = if (Main.useAvro) {
        constructor.asInstanceOf[events.AvroConstructor[Object]].end()
      } else {
        constructor.end()
      }

      if (Main.useAvro) {
        log.debug("Write AVRO, event: {}", value)
        out.asInstanceOf[DataFileWriter[Object]].append(value)
      } else {
        log.debug("Write JSON, event: {}", new String(value.asInstanceOf[Array[Byte]], JsonEncoding.UTF8.getJavaName))
        out.asInstanceOf[FileOutputStream].write(value.asInstanceOf[Array[Byte]])
      }
    }

    override def flushAndClose(): Unit = {
      out.flush()
      out.close()
    }
  }

  private class SNSEventWriter(val constructor: events.Constructor, val topic: String) extends Object with canwrite {

    val cred = DefaultCredentialsProvider.builder().build().resolveCredentials()
    val sns = SnsClient.builder().credentialsProvider(StaticCredentialsProvider.create(cred)).region(EU_CENTRAL_1).build()

    override def write(): Unit = {

      val topicResponse = sns.createTopic(CreateTopicRequest.builder().name(topic).build())
      val msg = new String(constructor.end().asInstanceOf[Array[Byte]], JsonEncoding.UTF8.getJavaName)
      log.debug(s"Will publish to Topic: %s, ARN: %s; MSG: %s".format(topic, topicResponse.topicArn, msg))
      val publishResponce = sns.publish(PublishRequest.builder.topicArn(topicResponse.topicArn).message(msg).build)
      log.debug(s"MessageId: %s".format(publishResponce.messageId))
    }

    override def flushAndClose(): Unit = {
      sns.close()
    }

  }

  private class KafkaEventWriter(val constructor: events.Constructor, val topic: String, val brokers: String) extends Object with canwrite {

    val props = new Properties()
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    //if (constructor.isInstanceOf[JSONConstructor]) "org.apache.kafka.common.serialization.ByteArraySerializer"
    //else "io.confluent.kafka.serializers.KafkaAvroSerializer")
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)

    val producer = new KafkaProducer[Object, Object](props)

    def write() = {
      val value: Object =
        if (Main.useAvro) {
          constructor.asInstanceOf[events.AvroConstructor[Object]].serialize(constructor.end())
        } else {
          constructor.end()
        }
      val pr = new ProducerRecord[Object, Object](topic, value)
      producer.send(pr)
    }

    override def flushAndClose(): Unit = {
      producer.flush();
      producer.close();
    }
  }

}
