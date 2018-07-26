package edu.airquality.data.processing.impl

import com.typesafe.scalalogging.LazyLogging
import edu.airquality.common.AppConfig
import edu.airquality.data.processing.api.DataTransformer
import edu.airquality.spark.SparkSessionWrapper
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

import scala.compat.Platform.currentTime
import scala.util.{Failure, Success, Try}

class AirQualityDataTransformer
    extends DataTransformer
    with SparkSessionWrapper
    with LazyLogging {

  private val dateColName = "Date"
  private val timeColName = "Time"
  private val tColName = "T"
  private val tNum = "T_Numeric"

  protected def selectTemperatureData(data: DataFrame):DataFrame = {
    Try(data.select(dateColName, timeColName, tColName)) match {
      case Success(data) => data
      case Failure(ex) =>
        throw ex
    }
  }

  protected def editTempDataScheme(data: DataFrame): Try[DataFrame] = {
    Try(data.withColumn(tColName, data(tColName).cast(DoubleType)))
  }

  protected def filterTempSensorRecordingMalfunctions(
      dirName: String,
      data: DataFrame): DataFrame = {
    Try(data.filter(col(tNum) === -200)) match {
      case Success(corruptData) =>
        Try(corruptData.write.csv(
          "file:///" + dirName + "airquality_temp_sensor_err_" + currentTime)) match {
          case Success(_) => logger.info("Writing corrupted data records on server...")
          case Failure(_) => logger.error("Can not write corrupted data records on server. Check space availability or write permissions. ${ex.getStackTrace}")
            //TODO log info for the dates of the corrupted records
        }
        data.withColumn(tNum, when(col(tNum) === -200, lit(null)).otherwise(col(tNum)))
      case Failure(ex) => logger.error(s"The corrupted temperature sensor data can not be cleaned ${ex.getStackTrace.toString}")
        throw ex
    }
  }

  protected def transformData(df: DataFrame, config: AppConfig): DataFrame = {
    val numeric = df
      .withColumn(tNum, regexp_replace(df(tColName), ",", ".").cast(DoubleType))
      .withColumn(dateColName, to_date(col(dateColName), "dd/MM/yyyy"))
    val filtered =
      filterTempSensorRecordingMalfunctions(config.corruptedRecordsDir, numeric)

    numeric.show(10)

    val aggregated = filtered
      .groupBy(dateColName)
      .agg(min(tNum) as "Min_T_Per_Day",
           max(tNum) as "Max_T_Per_Day",
           avg(tNum) as "Avg_T_Per_Day")
      .withColumn("month", month(col(dateColName)))
      .withColumn("year", year(col(dateColName)))
      .withColumn("day", dayofmonth(col(dateColName)))
      .withColumn("sum", concat(col("month"), col("year")))

    val statsData = filtered.join(aggregated, dateColName)
    statsData.show(10)

    val byDate = Window.orderBy(dateColName)
    val shiftDF = aggregated
      .withColumn("Avg_T_Per_Day_Shift",
                  lag("Avg_T_Per_Day", 1, "").over(byDate))
      .withColumn("diff", col("Avg_T_Per_Day_Shift") - col("Avg_T_Per_Day"))
      .groupBy("sum")
      .agg(min(col("diff")))
    shiftDF.show(10)
    //shiftDF.sort(col(dateColName)).show(400)

    aggregated.printSchema()
    aggregated.sort(aggregated(dateColName)).show(10000)
    aggregated
  }

  override def transform(df: DataFrame, config: AppConfig): DataFrame = {
    transformData(df, config)
  }
}
