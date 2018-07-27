package unit.data.processing

import edu.airquality.common.AppConfig
import edu.airquality.data.processing.impl.AirQualityDataTransformer
import org.apache.spark.sql.{DataFrame, SparkSession}

class TestAirQualityDataTransformer(spark: SparkSession) extends AirQualityDataTransformer(spark) {
  def preProcessTested(df: DataFrame, config: AppConfig) = preProcessTempData(df, config)
  def generateBasicSatasTested(df: DataFrame) = generateBasisStats(df)
  def raisePerMonthTested(df: DataFrame) = highestRisePerMonth(df)
}
