package edu.airquality.common.spark

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper  {
  lazy val spark: SparkSession = initializeSparkSession

   def initializeSparkSession = {
    SparkSession
      .builder()
      .appName("Air quality data analysis")
      .master("local")
      .config("spark.sql.shuffle.partitions", 2)//Because: Small data file + 8 logical cores on the test machine
      .config("spark.executor.memory", "2g")
      .getOrCreate()
  }
}
