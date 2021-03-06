name := """MahoutCollaborative"""

version := "1.0"

scalaVersion := "2.11.5"

//mainClass in (Compile, run) := Some("com.example.CollaborativeFiltering")

libraryDependencies ++= Seq(
  // Uncomment to use Akka
  //"com.typesafe.akka" % "akka-actor_2.11" % "2.3.9",
  "junit"             % "junit"           % "4.11"  % "test",
  "org.apache.mahout" % "mahout-core" % "0.5",
  "org.slf4j" % "slf4j-simple" % "1.7.5"
)
