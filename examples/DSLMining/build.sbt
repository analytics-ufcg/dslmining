name := """dslinaction-scala"""

version := "1.0"

scalaVersion := "2.11.5"

// Change this to another test framework if you prefer

libraryDependencies ++= Seq(
  // Uncomment to use Akka
  //"com.typesafe.akka" % "akka-actor_2.11" % "2.3.6",
  "org.apache.mahout" % "mahout-core" % "0.9",
  "org.slf4j" % "slf4j-simple" % "1.7.5"
)