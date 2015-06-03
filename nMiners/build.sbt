name := """nMiners"""

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-core" % "1.2.1",
  "org.apache.mahout" % "mahout-core" % "0.9",

  "org.scalatest" % "scalatest_2.10" % "2.2.4" % "test",
  "com.typesafe" % "config" % "1.2.1",

  "org.apache.camel" % "camel-jetty" % "2.15.1",
  "org.apache.camel" % "camel-core" % "2.15.1",
  "org.slf4j" % "slf4j-log4j12" % "1.7.12",
  "log4j" % "log4j" % "1.2.17",
  "com.google.guava" % "guava" % "18.0"
)

//Add a new dependency repository - Typesafe Repo
resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"