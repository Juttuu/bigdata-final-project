ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "com.nyctaxi"
ThisBuild / version := "0.1.0"

lazy val root = (project in file("."))
  .settings(
    name := "nyc-taxi-analytics-ml",
    Compile / run / fork := true,
    Compile / run / outputStrategy := Some(StdoutOutput),
    Compile / run / javaOptions ++= Seq(
      "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
      "-Dlog4j.configurationFile=log4j2.properties"
    ),
    Test / fork := true,
    Test / javaOptions ++= Seq(
      "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
      "-Dlog4j.configurationFile=log4j2.properties"
    ),
    Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat,
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.1",
      "org.apache.spark" %% "spark-sql" % "3.5.1",
      "org.apache.spark" %% "spark-mllib" % "3.5.1",
      "com.typesafe.akka" %% "akka-http" % "10.5.3",
      "com.typesafe.akka" %% "akka-http-spray-json" % "10.5.3",
      "com.typesafe.akka" %% "akka-stream" % "2.8.5",
      "org.scalatest" %% "scalatest" % "3.2.18" % Test
    )
  )
