package edu.airquality.data.processing.impl

import com.typesafe.scalalogging.LazyLogging
import edu.airquality.data.processing.api.DataLoader
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Failure, Success, Try}

class AirQualityDataLoader(spark: SparkSession)
    extends DataLoader
    with LazyLogging {

  private def readCSV(fileName: String, options: Map[String, String]): DataFrame = {
    spark.read.options(options).csv(fileName)
  }

  override def loadDataFile(fileName: String): DataFrame = {
    logger.info("Loading air quality data from file...")

    val options = Map("header" -> "true",
                      "inferSchema" -> "true",
                      "mode" -> "DROPMALFORMED",
                      "delimiter" -> ";")

    Try(readCSV(fileName, options)) match {
      case Success(df) => df
      case Failure(ex) =>
        logger.error(
          s"Loading data from file has failed.\n ${ex.getStackTrace.toList
            .foreach(_.toString)}")
        throw ex
    }
  }
}
