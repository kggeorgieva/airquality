package edu.airquality.data.processing.impl

import com.typesafe.scalalogging.LazyLogging
import edu.airquality.common.AppConfig
import edu.airquality.data.processing.api.DataTransformer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

import scala.compat.Platform.currentTime
import scala.util.{Failure, Success, Try}

class AirQualityDataTransformer(spark: SparkSession)
    extends DataTransformer
    with LazyLogging {

  private val dateColName = "Date"
  private val timeColName = "Time"
  private val tColName = "T"
  private val avg_T_Per_Day = "Avg_T_Per_Day"

  override def transform(df: DataFrame, config: AppConfig): DataFrame = {
    val preProcessed = preProcessTempData(df, config)
    val aggregated = generateBasisStats(preProcessed)
    highestRisePerMonth(aggregated)
  }

  //Move to separate Transformer if you need that exposed to UI
  def generateBasisStats(data: DataFrame): DataFrame = {
    val byDate = Window.partitionBy(dateColName)
    data
      .withColumn("Min_T_Per_Day", min(tColName) over byDate)
      .withColumn("Max_T_Per_Day", max(tColName) over byDate)
      .withColumn("Avg_T_Per_Day", avg(tColName) over byDate)
  }

  private def selectTemperatureData(data: DataFrame,
                                    colNames: Array[String]): DataFrame = {
    Try(data.select(colNames.map(col): _*)) match {
      case Success(tempData) => tempData
      case Failure(ex) =>
        logger.info(
          s"Data format has been changed. Expected columns might be changed or removed\n  ${ex.getStackTrace.toString} ")
        throw ex
    }
  }

  private def editTempDataScheme(data: DataFrame): DataFrame = {
    data
      .withColumn(tColName,
                  regexp_replace(col(tColName), ",", ".").cast(DoubleType))
      .withColumn(dateColName, to_date(col(dateColName), "dd/MM/yyyy"))
  }

  private def filterTempSensorRecordingMalfunctions(
      dirName: String,
      data: DataFrame): DataFrame = {
    Try(data.filter(col(tColName) === -200)) match {
      case Success(corruptData) =>
        Try(
          corruptData.write.csv(
            "file:///" + dirName + "airquality_temp_sensor_err_" + currentTime)) match {
          case Success(_) =>
            logger.info(s"Writing corrupted data records on server...")
          case Failure(ex) =>
            logger.error(
              s"Can not write corrupted data records on server. Check space availability or write permissions. ${ex.getStackTrace.toString}")
          //TODO log info for the dates of the corrupted records
        }
        data.withColumn(
          tColName,
          when(col(tColName) === -200, lit(null)).otherwise(col(tColName)))
      case Failure(ex) =>
        logger.error(
          s"The corrupted temperature sensor data can not be cleaned ${ex.getStackTrace.toString}")
        throw ex
    }
  }

  protected def preProcessTempData(df: DataFrame, config: AppConfig): DataFrame = {
    val tempData =
      selectTemperatureData(df, Array(dateColName, timeColName, tColName))
    val numericTempData = editTempDataScheme(tempData)
    filterTempSensorRecordingMalfunctions(config.corruptedRecordsDir,
                                          numericTempData)
  }

  protected def highestRisePerMonth(data: DataFrame): DataFrame = {
    val prevDay = "prev_day"
    val monthYear = "Month_year"

    val filteredData = data
      .select(dateColName, avg_T_Per_Day)
      .distinct()
      .withColumn(
        monthYear,
        concat(month(col(dateColName)), lit("/"), year(col(dateColName))))

    val byDate = Window.orderBy(dateColName)
    val shiftDF = filteredData
      .withColumn(prevDay, lag(avg_T_Per_Day, 1, "") over byDate)
      .withColumn("diff", col(avg_T_Per_Day) - col(prevDay))

    val byMonthYear = Window.partitionBy(monthYear)
    val highestRise =
      shiftDF
        .withColumn("highest_rise_per_month", max("diff") over byMonthYear)
        .select(monthYear, "highest_rise_per_month")
        .distinct()
    highestRise
  }
}
