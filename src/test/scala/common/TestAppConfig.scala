package common
import com.typesafe.scalalogging.LazyLogging
import edu.airquality.common.AppConfig
import pureconfig.error.ConfigReaderFailures
import pureconfig.loadConfig

trait TestAppConfig extends LazyLogging {
  lazy val testConfig = readConfig()

  def readConfig(): AppConfig = {
    val appConfig: Either[ConfigReaderFailures, AppConfig] = loadConfig[AppConfig]
    appConfig match {
      case Left(ex) => new AppConfig()
      case Right(config) => config
    }
  }
}
