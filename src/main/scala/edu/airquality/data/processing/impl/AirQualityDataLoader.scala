package edu.airquality.data.processing.impl

import com.typesafe.scalalogging.LazyLogging
import edu.airquality.data.processing.api.DataLoader
import edu.airquality.spark.SparkSessionWrapper
import org.apache.spark.sql.DataFrame

import scala.util.{Failure, Success, Try}

object AirQualityDataLoader extends DataLoader with SparkSessionWrapper with LazyLogging{

  def readCSV(fileName: String, options: Map[String, String]): DataFrame = {
    spark.read.format("csv").options(options).load(fileName)
  }

  override def loadDataFile(fileName: String): DataFrame = {
    logger.info("Loading air quality data from file...")

    val options = Map("header" -> "true",
      "inferSchema" -> "true",
      "mode" -> "DROPMALFORMED",
      "delimiter" -> ";")

    Try(readCSV(fileName, options)) match {
      case Success(df) => df
      case Failure(ex) => logger.error(s"Loading data fro file has failed.\n ${ex.getStackTrace.toList.foreach(_.toString)}")
        throw ex
    }
  }
}
