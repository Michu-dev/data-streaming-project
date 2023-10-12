ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(

    name := "KafkaProducer",
    libraryDependencies += "org.apache.kafka" % "kafka-clients" % "3.3.1",
  )
