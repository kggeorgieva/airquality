import com.typesafe.scalalogging.LazyLogging
import edu.airquality.common.AppConfig
import edu.airquality.data.processing.impl.{AirQualityDataLoader, AirQualityDataTransformer, DataProcessor}
import edu.airquality.common.spark. SparkSessionWrapper
import pureconfig.error.ConfigReaderFailures
import pureconfig.loadConfig

object AppEntryPoint extends App with SparkSessionWrapper with LazyLogging {

  def runAirQualityPipeline(): Unit = {
    val appConfig: Either[ConfigReaderFailures, AppConfig] = loadConfig[AppConfig]
    appConfig match {
      case Left(ex) => logger.error(s"Can not load configuration from file\n ${ex}")
      case Right(config) =>
        logger.info(s"Loading application configurations...")
        logger.info(s"Reading data from ${config.dataFileName}")
        logger.info(s"Corrupted records will be saved in ${config.corruptedRecordsDir}")

        val processor = new DataProcessor(new AirQualityDataLoader(spark), new AirQualityDataTransformer(spark))
        processor.process(config)
    }
  }

  runAirQualityPipeline()
}
