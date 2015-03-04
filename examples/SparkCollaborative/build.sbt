name := """SparkCollaborative"""

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  // Uncomment to use Akka
  //"com.typesafe.akka" % "akka-actor_2.11" % "2.3.9",
  "junit"             % "junit"           % "4.12"  % "test",
  "com.novocode"      % "junit-interface" % "0.11"  % "test",
    "org.apache.spark" % "spark-mllib_2.10" % "1.1.0",
    "org.apache.spark" % "spark-core_2.10" % "1.1.0"
)
