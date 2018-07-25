package edu.airquality.data.processing.api

import edu.airquality.common.AppConfig
import org.apache.spark.sql.DataFrame

trait DataTransformer {
  def transform(df: DataFrame, config: AppConfig): DataFrame
}
