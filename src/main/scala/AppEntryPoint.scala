import com.typesafe.scalalogging.LazyLogging
import edu.airquality.common.AppConfig
import edu.airquality.data.processing.impl.{AirQualityDataLoader, AirQualityDataTransformer, DataPipeline}
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
        println(s" Reading data from ${config.dataFileName}")
        println(s"Corrupted records will be saved in ${config.corruptedRecordsDir}")
        val pipeline =
          new DataPipeline(new AirQualityDataLoader, new AirQualityDataTransformer)
        pipeline.process(config)
    }
  }

  runAirQualityPipeline()
}
