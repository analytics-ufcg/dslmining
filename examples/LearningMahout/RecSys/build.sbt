name := """FCMahout"""

version := "1.0"

scalaVersion := "2.11.1"

libraryDependencies ++= Seq(
  // Uncomment to use Akka
  //"com.typesafe.akka" % "akka-actor_2.11" % "2.3.6",
  "junit"             % "junit"           % "4.11"  % "test",
  "org.apache.mahout" % "mahout-core" % "0.9",
  "org.slf4j" % "slf4j-simple" % "1.7.5"
)
