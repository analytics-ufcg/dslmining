//name := """nMiners"""
//
//version := "1.0"
//
//scalaVersion := "2.10.4"
//
//libraryDependencies ++= Seq(
//  "org.apache.hadoop" % "hadoop-core" % "1.2.1",
//  "org.apache.mahout" % "mahout-core" % "0.9",
//
//  "org.scalatest" % "scalatest_2.10" % "2.2.4" % "test",
//  "com.typesafe" % "config" % "1.2.1",
//
//  "org.apache.camel" % "camel-jetty" % "2.15.1",
//  "org.apache.camel" % "camel-core" % "2.15.1",
//  "org.slf4j" % "slf4j-log4j12" % "1.7.12",
//  "log4j" % "log4j" % "1.2.17"
//)
//
//libraryDependencies += "org.apache.commons" % "commons-math3" % "3.5"
//libraryDependencies += "org.apache.mahout" % "mahout-math-scala_2.10" % "0.10.1"
//libraryDependencies += "org.apache.mahout" % "mahout-spark_2.10" % "0.10.1"
//libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.2.2"
//libraryDependencies += "org.apache.mahout" % "mahout-core" % "0.9"
////Add a new dependency repository - Typesafe Repo
//resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"
//
//name := """item-similarity-1.0"""
name := """nMiners"""

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.commons" % "commons-math3" % "3.5"
libraryDependencies += "org.apache.mahout" % "mahout-math-scala_2.10" % "0.10.1"
libraryDependencies += "org.apache.mahout" % "mahout-spark_2.10" % "0.10.1"
libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.2.2"
libraryDependencies += "org.apache.mahout" % "mahout-core" % "0.9"
libraryDependencies += "org.apache.camel" % "camel-jetty" % "2.15.1"
libraryDependencies += "org.apache.camel" % "camel-core" % "2.15.1"
libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.2.4" % "test"
libraryDependencies += "com.typesafe" % "config" % "1.2.1"
