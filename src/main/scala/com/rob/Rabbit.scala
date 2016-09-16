package com.rob

import java.io.{File, FileOutputStream, PrintWriter}

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

  val conf = ConfigFactory.load("local.conf")

  val connections = conf.getConfigList("amqp.connections").asScala

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

  def all() = {
    val cxns = connections.map(init)

    connections zip cxns foreach {
      case (config, cxn) =>
        val filename = config.getString("fileName").replaceFirst("^~", System.getProperty("user.home"))

        ackAll(filename)(cxn).run.run

        close(cxn)
    }
  }


  private def ackAll(filename: String)(cxn: Cxn): Process[Task, Unit] = {
    ackOne(cxn) map (_.spaces2) pipe text.utf8Encode to io.fileChunkW(filename)
  }


  private def ackOne(cxn: Cxn): Process0[Json] = {
    val response = Option(cxn.channel.basicGet(cxn.queueName, false))

    val json = response.flatMap { res => {
      val msgBody = new String(res.getBody, "UTF-8")

      cxn.channel.basicAck(res.getEnvelope.getDeliveryTag, false)

      msgBody.parseOption
    }
    }

    json match {
      case Some(txt) => Process.emit(txt) ++ ackOne(cxn)
      case None      => Process.halt
    }
  }
}
