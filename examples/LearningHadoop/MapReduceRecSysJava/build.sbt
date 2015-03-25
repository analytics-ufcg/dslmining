name := """hadoop_cap6"""

version := "1.0"

scalaVersion := "2.11.5"

mainClass in (Compile, run) := Some("com.example.Main")

libraryDependencies ++= Seq(
  // Uncomment to use Akka
  //"com.typesafe.akka" % "akka-actor_2.11" % "2.3.9",
  "junit"             % "junit"           % "4.12"  % "test",
  "com.novocode"      % "junit-interface" % "0.11"  % "test",
  "org.apache.mahout" % "mahout-core" % "0.7",
  "org.apache.hadoop" % "hadoop-core" % "1.2.1"
)
