package common

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapperTest {
  lazy val spark = SparkSession
    .builder()
    .appName("Air quality data analysis unit test")
    .master("local")
    .getOrCreate()

}
