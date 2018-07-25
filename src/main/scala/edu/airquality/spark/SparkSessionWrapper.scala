package edu.airquality.spark

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {
  lazy val spark = SparkSession
    .builder()
    .appName("Air quality data analysis")
    .master("local")
    .config("spark.sql.shuffle.partitions", 6)
    .config("spark.executor.memory", "2g")
    .getOrCreate()
}
