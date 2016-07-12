package com.rob

import java.io.{File, FileOutputStream, PrintWriter}
import java.util

import com.rabbitmq.client.AMQP.BasicProperties.Builder
import com.rabbitmq.client._

import scalaz._
import Scalaz._
import argonaut._
import Argonaut._

import scala.collection.JavaConverters._
import scala.util.Try
import com.typesafe.config.{Config, ConfigFactory, ConfigList}

import scala.collection.mutable


object Rabbit {

  val conf = ConfigFactory.load("application.conf")

  val connections = conf.getConfigList("amqp.connections").asScala

  val DELAY_EXCHANGE_NAME = ""
  val DELAY_QUEUE_NAME    = ""

  val factory = {
    val factory = new ConnectionFactory()

    factory.setHost(conf.getString("amqp.ip"))
    factory.setPort(conf.getInt("amqp.port"))
    factory.setUsername(conf.getString("amqp.user"))
    factory.setPassword(conf.getString("amqp.password"))

    if (conf.getBoolean("amqp.useSSL")) factory.useSslProtocol()

    factory
  }

//  def publish(): Unit = publish("""{"id":123146, "__SluiceChangeType__":"UPDATE", "status":"V","issuer_card_id":"1446430854974957","card_id":"1000","acct_id":"27329531"}""")


//  def publish(msg: String): Unit = {
//    val connection = factory.newConnection()
//    val channel = connection.createChannel()
//
//    channel.exchangeDeclare(UI_EXCHANGE_NAME, "fanout", true)
//    channel.exchangeDeclare(DM_EXCHANGE_NAME, "direct", true)
//
//    val builder = new Builder()
//    builder.headers(Map[String, AnyRef]("seqNo" -> Long.box(System.currentTimeMillis), "committedAt" -> Long.box(System.currentTimeMillis)).asJava)
//
////    channel.basicPublish(EXCHANGE_NAME, "cardStateChange", builder.build(), msg.getBytes())
//    println( s"""[x] Sent $msg""")
//
//    channel.close()
//    connection.close()
//  }

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
//    val connection = factory.newConnection()
//    val channel = connection.createChannel()
//
//    channel.exchangeDeclare(DM_EXCHANGE_NAME, "direct", true)
//    channel.exchangeDeclare(UI_EXCHANGE_NAME, "fanout", true)

//    val args = Map[String, AnyRef]("x-dead-letter-exchange" -> DELAY_EXCHANGE_NAME, "x-dead-letter-routing-key" -> ROUTING_KEY).asJava
//    channel.queueDeclare(UI_QUEUE_NAME, true, false, false, Map.empty[String, AnyRef].asJava).getQueue
//    channel.queueDeclare(DM_QUEUE_NAME, true, false, false, Map.empty[String, AnyRef].asJava).getQueue
//
//    val delayArgs = Map[String, AnyRef]("x-dead-letter-exchange" -> EXCHANGE_NAME, "x-dead-letter-routing-key" -> ROUTING_KEY, "x-message-ttl" -> Long.box(10000L)).asJava
//    channel.queueDeclare(DELAY_QUEUE_NAME, true, false, false, delayArgs).getQueue

//    channel.queueBind(UI_QUEUE_NAME, UI_EXCHANGE_NAME, UI_ROUTING_KEY)
//    channel.queueBind(DM_QUEUE_NAME, DM_EXCHANGE_NAME, DM_ROUTING_KEY)
  }

  def nackNext(): Unit = {
    val connection = factory.newConnection()
    val channel = connection.createChannel()

//    channel.exchangeDeclare(EXCHANGE_NAME, "fanout")
//    channel.exchangeDeclare(DELAY_EXCHANGE_NAME, "direct")
//
//    val args = Map[String, AnyRef]("x-dead-letter-exchange" -> DELAY_EXCHANGE_NAME, "x-dead-letter-routing-key" -> ROUTING_KEY).asJava
//    channel.queueDeclare(QUEUE_NAME, true, false, false, args).getQueue
//
//    val delayArgs = Map[String, AnyRef]("x-dead-letter-exchange" -> EXCHANGE_NAME, "x-dead-letter-routing-key" -> ROUTING_KEY).asJava
//    channel.queueDeclare(DELAY_QUEUE_NAME, true, false, false, delayArgs).getQueue
//
//    channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY)
//    channel.queueBind(DELAY_QUEUE_NAME, DELAY_EXCHANGE_NAME, ROUTING_KEY)
//
//    val consumer = new DefaultConsumer(channel) {
//      override def handleDelivery(consumerTag: String, envelope: Envelope, properties: AMQP.BasicProperties, body: Array[Byte]) {
//        val message = new String(body, "UTF-8")
//        println( s""" [x] Received $message""")
//        channel.basicNack(envelope.getDeliveryTag, false, false)
//      }
//    }

//    val response = channel.basicGet(QUEUE_NAME, false)
//
//    println( s""" [x] Nacking ${new String(response.getBody, "UTF-8")}""")
//    channel.basicNack(response.getEnvelope.getDeliveryTag, false, false)
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
