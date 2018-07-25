import com.typesafe.scalalogging.LazyLogging
import edu.airquality.common.AppConfig
import edu.airquality.data.processing.api.DataPipeline
import edu.airquality.data.processing.impl.{
  AirQualityDataLoader,
  AirQualityDataTransformer
}
import edu.airquality.spark.SparkSessionWrapper

import pureconfig.error.ConfigReaderFailures
import pureconfig.loadConfig

object AppEntryPoint extends App with SparkSessionWrapper with LazyLogging {

  def runAirQualityPipeline(): Unit = {

    val appConfig: Either[ConfigReaderFailures, AppConfig] =
      loadConfig[AppConfig]
    appConfig match {
      case Left(ex) => logger.error("Can not load configuration from file\n {ex}")
      case Right(config) =>
        println(s"File Data Name ${config.dataFileName}")
        println(s"Corrupted records at ${config.corruptedRecordsDir}")
        val pipeline =
          new DataPipeline(AirQualityDataLoader, AirQualityDataTransformer)
        pipeline.process(config)
    }
  }

  runAirQualityPipeline()
}
