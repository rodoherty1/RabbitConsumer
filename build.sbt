name := "RabbitMQ Client"

version := "1.0"

scalaVersion := "2.11.7"

addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.7.1")

val scalazV = "7.1.3"
val scalazStreamV = "0.8"
val argonautV = "6.1"
val typesafeConfigV = "1.3.0"

val typesafeConfig = Seq(
  "com.typesafe" % "config" % typesafeConfigV
)

val scalaz = Seq(
  "org.scalaz" %% "scalaz-core" % scalazV,
  "org.scalaz.stream" %% "scalaz-stream" % scalazStreamV
)

val argonaut = Seq(
  "io.argonaut" %% "argonaut" % argonautV
)

val scalacheck = Seq(
    "org.scalacheck" %% "scalacheck" % "1.12.2"
)

val scalatest = Seq(
    "org.scalatest" %% "scalatest" % "2.2.4"
)

val logging = Seq (
  "org.slf4j" % "slf4j-api" % "1.7.12",
  "ch.qos.logback" % "logback-classic" % "1.1.3"
//  "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0"
)

val amqpClient = Seq(
    "com.rabbitmq" % "amqp-client" % "3.5.3"
)

libraryDependencies ++= logging ++ scalacheck ++ scalatest ++ amqpClient ++ scalaz ++ argonaut ++ typesafeConfig

initialCommands in console :=
  """
    |import com.rob.{Rabbit => R}
  """.stripMargin

