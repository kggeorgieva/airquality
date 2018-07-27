package common

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapperTest {
  lazy val spark: SparkSession = SparkSession
    .builder()
    .appName("Air quality data analysis unit test")
    .master("local")
    .config("spark.sql.shuffle.partitions", 2)
    .config("spark.executor.memory", "2g")
    .getOrCreate()
}
