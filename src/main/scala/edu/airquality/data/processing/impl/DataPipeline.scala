package edu.airquality.data.processing.impl

import com.typesafe.scalalogging.LazyLogging
import edu.airquality.common.AppConfig
import edu.airquality.data.processing.api.{DataLoader, DataTransformer}

import scala.compat.Platform.currentTime

class DataPipeline(loader: DataLoader, transformer: DataTransformer)
    extends LazyLogging {

  def process(config: AppConfig): Unit = {
    val start = currentTime
    transformer.transform(loader.loadDataFile(config.dataFileName), config);
    logger.info(s"Air quality data processing finished in ${currentTime - start} ms.")
  }
}
