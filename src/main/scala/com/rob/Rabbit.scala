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

import scalaz.stream.channel

object Rabbit {

  val logger = LoggerFactory.getLogger(Rabbit.getClass)

  val conf = ConfigFactory.load("local.conf")

  val connections = conf.getConfigList("amqp.connections").asScala




  def init(cxn: Config) = {
    val exchange     = cxn.getString("exchangeName")
    val exchangeType = cxn.getString("exchangeType")
    val queueName    = cxn.getString("queue")
    val routingKey   = cxn.getString("routingKey")

    val factory = {
      val cxnFactory = new ConnectionFactory()

      cxnFactory.setHost(conf.getString("amqp.ip"))
      cxnFactory.setPort(conf.getInt("amqp.port"))
      cxnFactory.setUsername(conf.getString("amqp.user"))
      cxnFactory.setPassword(conf.getString("amqp.password"))

      if (conf.getBoolean("amqp.useSSL")) cxnFactory.useSslProtocol()

      cxnFactory
    }

    val connection = factory.newConnection()
    val channel = connection.createChannel()

    channel.exchangeDeclare(exchange, exchangeType, true)
    channel.queueDeclare(queueName, true, false, false, Map.empty[String, AnyRef].asJava).getQueue
    channel.queueBind(queueName, exchange, routingKey)

    ackOne(channel)(queueName)
  }

  def all() = {
    val ackOnes = connections.map(init)

    connections zip ackOnes foreach {
      case (config, f) =>
        val fileName = config.getString("fileName")

        val file = new File(fileName.replaceFirst("^~", System.getProperty("user.home")))
        file.createNewFile()

        val pw = new PrintWriter(new FileOutputStream(file, true))

        ackAll(pw)(f)

        pw.close()
    }
  }


  private def ackAll(pw: PrintWriter)(f: Int => Option[Json]): Unit = {
    def loop(count: Int = 0, f: Int => Option[Json]): Unit = {
      f(count) match {
        case Some(json) =>
          pw.append(json.spaces2)
          pw.append(",\n")
          loop(count + 1, f)

        case None => println ("No new messages")
      }
    }

    loop(count = 0, f)
  }

  private def ackOne(channel: Channel)(queueName: String)(count: Int = 0): Option[Json] = {
    val response = Option(channel.basicGet(queueName, false))

    val json = response.flatMap{ res => {
        val msgBody = new String(res.getBody, "UTF-8")

        channel.basicAck(res.getEnvelope.getDeliveryTag, false)

        println(s"""Acking message #$count """)
        msgBody.parseOption
      }
    }

//    for {
//      _ <- Try(channel.close())
//      _ <- Try(connection.close())
//    } yield ()


    json
  }
}
