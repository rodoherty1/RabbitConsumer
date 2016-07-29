package com.rob

import scalaz._
import Scalaz._
import scalaz.concurrent.Task
import scalaz.stream.{Process, async}
import scala.concurrent.duration._
import scalaz.stream.async.mutable.Queue


object test extends App{




  def test1(): Vector[String] = {
    val q = async.boundedQueue[Int](10)

    val left: Process[Task, Int] = q.dequeue
    val right: Process[Task, Int] = q.dequeue

    val x1: Process[Task, Unit] = Process(1, 2, 3, 4, 5) to q.enqueue
    val x2: Task[Unit] = x1.run
    val x3: Unit = x2.run

    val both: Process[Task, String] = (left map { i => s"left: $i" }) merge (right map { i => s"right: $i" })

    val y1: Process[Task, String] = both take 5
    val y2: Task[Vector[String]] = y1.runLog

    y2.run
  }

  println(test1())


  def test2(): Unit = {
    val q: Queue[String] = async.boundedQueue[String](10)

    val p: Process[Task, String] = q.dequeue

    val t = q.enqueueOne("Hello").run

    val x: Unit = q.close.run

    println(p.runLog.run)
  }

  test2()

}
