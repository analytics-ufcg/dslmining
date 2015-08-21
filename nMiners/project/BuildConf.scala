import sbt._

object BuildConf extends Build {

  lazy val nMiners = Project(id = "nMiners", base = file(".")) //dependsOn(hadoop, spark)

  lazy val hadoop = Project(id = "hadoop", base = file("hadoop")) dependsOn nMiners

  lazy val spark = Project(id = "spark", base = file("spark")) dependsOn nMiners
}