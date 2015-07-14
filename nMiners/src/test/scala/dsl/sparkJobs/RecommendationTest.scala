package dsl.sparkJobs

import org.apache.mahout.sparkbindings._
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}

/**
 * Created by igleson on 14/07/15.
 */
class RecommendationTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  implicit val context = mahoutSparkContext(masterUrl = "local",
    appName = "MahoutLocalContext")
}
