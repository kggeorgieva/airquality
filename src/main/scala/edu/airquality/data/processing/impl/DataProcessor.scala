package edu.airquality.data.processing.impl

import com.typesafe.scalalogging.LazyLogging
import edu.airquality.common.AppConfig
import edu.airquality.data.processing.api.{DataLoader, DataTransformer}
import org.apache.spark.sql.DataFrame

import scala.compat.Platform.currentTime

class DataProcessor(loader: DataLoader, transformer: DataTransformer) extends LazyLogging {
  def process(config: AppConfig): Unit = {
    val start = currentTime
    printResultData(transformer.transform(loader.loadDataFile(config.dataFileName), config))
    logger.info(s"Air quality data processing finished in ${currentTime - start} ms.")
  }

  def printResultData(data :DataFrame): Unit = {
    //TODO
    data.printSchema()
    data.show()
  }
}
