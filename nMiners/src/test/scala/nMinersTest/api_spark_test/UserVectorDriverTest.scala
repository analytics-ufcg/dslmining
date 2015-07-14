//package nMinersTest.api_spark_test
//
//import api_spark.UserVectorDriver
//import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
//
//
///**
// * Created by leonardo on 08/07/15.
// */
//class ApiSparkTest extends FlatSpec with Matchers with BeforeAndAfterAll {
//
//  it should "UservectorDrm returns a DRMLike" in {
//    val inputFile = "data/test.csv" //Input Data
//    val outPath = Some("data/similarity-matrices/")
//    val masterNode = "local"
//    val userVectorDRM = UserVectorDriver.run(Array(
//      "--input", inputFile,
//      "--output", outPath.getOrElse(""),
//      "--master", masterNode
//    ))
//
//
//  }
//
//
//
//}
