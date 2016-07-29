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



case class Cxn(connection: Connection, channel: Channel, queueName: String)

object Rabbit {

  val logger = LoggerFactory.getLogger(Rabbit.getClass)

  val conf = ConfigFactory.load("uat.conf")

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
        val fileName = config.getString("fileName")

        val file = new File(fileName.replaceFirst("^~", System.getProperty("user.home")))
        file.createNewFile()

        val pw = new PrintWriter(new FileOutputStream(file, true))

        ackAll(pw)(cxn)

        pw.close()
        close(cxn)
    }
  }


  private def ackAll(pw: PrintWriter)(cxn: Cxn): Unit = {
    def loop(count: Int = 0): Unit = {
      ackOne(cxn)(count) match {
        case Some(json) =>
          pw.append(json.spaces2)
          pw.append(",\n")
          loop(count + 1)

        case None => println ("No new messages")
      }
    }

    loop(count = 0)
  }


  private def ackOne(cxn: Cxn)(count: Int = 0): Option[Json] = {
    val response = Option(cxn.channel.basicGet(cxn.queueName, false))

    val json = response.flatMap{ res => {
      val msgBody = new String(res.getBody, "UTF-8")

      cxn.channel.basicAck(res.getEnvelope.getDeliveryTag, false)

      println(s"""Acking message #$count """)
      msgBody.parseOption
    }
    }

    json
  }
//
//
//  def x() = {
//    val dt = new DateTime()
//    val fmt = DateTimeFormat.forPattern("MMMM, yyyy")
//    fmt.print(dt)
//  }
}
