name := """wordcount"""

version := "1.0"

scalaVersion := "2.11.5"

libraryDependencies ++= Seq(
  // Uncomment to use Akka
  //"com.typesafe.akka" % "akka-actor_2.11" % "2.3.9",
  "junit"             % "junit"           % "4.12"  % "test",
  "com.novocode"      % "junit-interface" % "0.11"  % "test",
  "org.apache.mahout" % "mahout-core" % "0.9",
  "org.slf4j" % "slf4j-simple" % "1.7.5",
  "org.apache.hadoop" % "hadoop-core" % "1.2.1"
)

