package com.rob

import com.rabbitmq.client._

import scalaz._
import Scalaz._
import argonaut._
import Argonaut._

import scala.collection.JavaConverters._
import scala.util.Try
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory

import scalaz.concurrent.Task
import scalaz.stream._



case class Cxn(connection: Connection, channel: com.rabbitmq.client.Channel, queueName: String)

object Rabbit {

  val jsonPreamble = "{\n    \"all\": ["
  val jsonPostamble = "]}"

  val logger = LoggerFactory.getLogger(Rabbit.getClass)


  def init(cxn: Config): Cxn = {
    val exchange     = cxn.getString("exchangeName")
    val queueName    = cxn.getString("queue")
    val routingKey   = cxn.getString("routingKey")

    val factory = {
      val cxnFactory = new ConnectionFactory()

      cxnFactory.setHost(cxn.getString("ip"))
      cxnFactory.setPort(cxn.getInt("port"))
      cxnFactory.setUsername(cxn.getString("user"))
      cxnFactory.setPassword(cxn.getString("password"))

      if (cxn.getBoolean("useSSL")) cxnFactory.useSslProtocol()

      cxnFactory
    }

    val connection = factory.newConnection()
    val channel    = connection.createChannel()

    channel.exchangeDeclarePassive(exchange)
    channel.queueDeclare(queueName, true, false, false, Map.empty[String, AnyRef].asJava).getQueue
    channel.queueBind(queueName, exchange, routingKey)

    Cxn(connection, channel, queueName)
  }

  private def close(cxn: Cxn) = {
    for {
      _ <- Try(cxn.channel.close())
      _ <- Try(cxn.connection.close())
    } yield ()
  }
  def local() = all("local.conf")

  def uat() = all("uat.conf")

  def oat() = all("oat.conf")

  def all(configName: String): Unit = {
    val connections = ConfigFactory.load(configName).getConfigList("amqp.connections").asScala

    val cxns = connections.map(init)

    connections zip cxns foreach {
      case (config, cxn) =>
        val filename = config.getString("fileName").replaceFirst("^~", System.getProperty("user.home"))

        getMessages(filename)(cxn).run.run

        close(cxn)
    }
  }


  private def getMessages(filename: String)(cxn: Cxn): Process[Task, Unit] = {
    Process(jsonPreamble) ++ (receiveAll(cxn) map (_.spaces2) intersperse ",") ++ Process(jsonPostamble) pipe text.utf8Encode to io.fileChunkW(filename)
  }


  private def receiveAll(cxn: Cxn): Process0[Json] = {
    val response = Option(cxn.channel.basicGet(cxn.queueName, false))

    val json = response.flatMap { res => {
      val msgBody = new String(res.getBody, "UTF-8")

      cxn.channel.basicAck(res.getEnvelope.getDeliveryTag, false)

      msgBody.parseOption
    }
    }

    json match {
      case Some(txt) => Process.emit(txt) ++ receiveAll(cxn)
      case None      => Process.halt
    }
  }
}
