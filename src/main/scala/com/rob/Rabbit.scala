package com.rob

import java.io.{File, FileOutputStream, PrintWriter}

import com.rabbitmq.client._

import scalaz._
import Scalaz._
import argonaut._
import Argonaut._

import scala.collection.JavaConverters._
import scala.util.Try
import com.typesafe.config.ConfigFactory

import org.slf4j.LoggerFactory

object Rabbit {

  val logger = LoggerFactory.getLogger(Rabbit.getClass)

  val conf = ConfigFactory.load("local.conf")

  val connections = conf.getConfigList("amqp.connections").asScala

  val factory = {
    val factory = new ConnectionFactory()

    factory.setHost(conf.getString("amqp.ip"))
    factory.setPort(conf.getInt("amqp.port"))
    factory.setUsername(conf.getString("amqp.user"))
    factory.setPassword(conf.getString("amqp.password"))

    if (conf.getBoolean("amqp.useSSL")) factory.useSslProtocol()

    factory
  }


  def init(): Unit = {
    connections.foreach( cxn => {
      val exchange     = cxn.getString("exchangeName")
      val exchangeType = cxn.getString("exchangeType")
      val queueName    = cxn.getString("queue")
      val routingKey   = cxn.getString("routingKey")

      val connection = factory.newConnection()
      val channel = connection.createChannel()

      channel.exchangeDeclare(exchange, exchangeType, true)
      channel.queueDeclare(queueName, true, false, false, Map.empty[String, AnyRef].asJava).getQueue
      channel.queueBind(queueName, exchange, routingKey)
    })
  }

  def all() = connections.foreach(cxn => {
      val queueName = cxn.getString("queue")
      val fileName  = cxn.getString("fileName")

      val f = new File(fileName.replaceFirst("^~",System.getProperty("user.home")))
      f.createNewFile()

      val pw = new PrintWriter(new FileOutputStream(f, true))

      ackAll(queueName, pw)
      pw.close()
    }
  )

  private def ackAll(queueName: String, pw: PrintWriter): Unit = {

    def loop(count: Int = 0, f: Int => Option[Json]): Unit = {
      f(count) match {
        case Some(json) =>
          pw.append(json.spaces2)
          pw.append(",\n")
          loop(count + 1, f)

        case None => println ("No new messages")
      }
    }

    loop(f = ackOne(queueName))
  }

  private def ackOne(queueName: String)(count: Int = 0): Option[Json] = {
    val connection = factory.newConnection()
    val channel = connection.createChannel()

    val response = Option(channel.basicGet(queueName, false))

    val json = response.flatMap{ res => {
        val msgBody = new String(res.getBody, "UTF-8")

        channel.basicAck(res.getEnvelope.getDeliveryTag, false)

        println(s"""Acking message #$count """)
        msgBody.parseOption
      }
    }

    for {
      _ <- Try(channel.close())
      _ <- Try(connection.close())
    } yield ()

    json
  }
}
