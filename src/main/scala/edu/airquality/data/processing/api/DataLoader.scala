package edu.airquality.data.processing.api

import org.apache.spark.sql.DataFrame

trait DataLoader {
  def loadDataFile(fileName: String): DataFrame
}
