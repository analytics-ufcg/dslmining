name := """nMiners"""

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.commons" % "commons-math3" % "3.5"
libraryDependencies += "org.apache.mahout" % "mahout-math" % "0.10.1"
libraryDependencies += "org.apache.mahout" % "mahout-math-scala_2.10" % "0.10.1"
libraryDependencies += "org.apache.mahout" % "mahout-spark_2.10" % "0.10.1"
libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.2.2"
libraryDependencies += "org.apache.mahout" % "mahout-core" % "0.9"
libraryDependencies += "org.apache.camel" % "camel-jetty" % "2.15.1"
libraryDependencies += "org.apache.camel" % "camel-core" % "2.15.1"
libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.2.4" % "test"
