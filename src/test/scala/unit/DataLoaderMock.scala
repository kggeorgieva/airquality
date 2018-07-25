package unit

import edu.airquality.data.processing.api.DataLoader
import org.apache.spark.sql.DataFrame

class DataLoaderMock extends DataLoader with SparkSessionWrapperTest {

  override def loadDataFile(fileName: String): DataFrame = {
    import spark.implicits._
    val someDF = Seq(
      (8, "bat"),
      (64, "mouse"),
      (-27, "horse")
    ).toDF("number", "word")
    someDF
  }
}
