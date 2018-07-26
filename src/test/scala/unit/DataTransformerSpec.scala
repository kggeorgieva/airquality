package unit

import common.SparkSessionWrapperTest
import org.apache.spark.sql.test.SharedSparkSession
import org.scalatest.WordSpec

class DataTransformerSpec extends WordSpec with SparkSessionWrapperTest {
    import spark.implicits._
    "a shared spark session" can {
      "work properly in a WordSpec" in {
        assert((1 to 100).toDS.reduce(_ + _) === 5050)
      }
    }
}
