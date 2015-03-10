name := """ScalaCollaborative"""

version := "1.0"

scalaVersion := "2.10.4"

// Change this to another test framework if you prefer
libraryDependencies ++= Seq(

  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "org.apache.spark"  % "spark-core_2.10"              % "1.1.0" % "provided",
  "org.apache.spark"  % "spark-mllib_2.10"             % "1.1.0"

)


// Uncomment to use Akka
//libraryDependencies += "com.typesafe.akka" % "akka-actor_2.11" % "2.3.9"


