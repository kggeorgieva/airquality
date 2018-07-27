package unit.data.processing

import common.{TestAppConfig, TestData}
import org.scalatest.WordSpec

class DataTransformerSpec extends WordSpec with TestData with TestAppConfig {

  val airQualityTransformer = new TestAirQualityDataTransformer(spark)

    "AirQualityDataTransformer" can {
      "prepare and clean data" in {
       val testPreProcessed = airQualityTransformer.preProcessTested(hourlyDf, testConfig)

        testPreProcessed.printSchema()
        testPreProcessed.show(10)

        val structFiled = testPreProcessed.schema.fields
        assert(structFiled.length === 3)
        assert( testPreProcessed.select("T").filter("T is null").count() === 7)
      }

      "aggregate basic stats min, man, avg" in {
        val testPreProcessed = airQualityTransformer.preProcessTested(hourlyDf, testConfig)
        val stats = airQualityTransformer.generateBasicSatasTested(testPreProcessed)

        stats.printSchema()
        stats.show(10)
        assert(stats.count() === 62)
      }

      "aggregate highest temp raise on monthly basis" in {
        val testPreProcessed = airQualityTransformer.preProcessTested(hourlyDf, testConfig)
        val stats = airQualityTransformer.generateBasicSatasTested(testPreProcessed)
        val highestRaiseMonth = airQualityTransformer.raisePerMonthTested(stats)

        highestRaiseMonth.printSchema()
        highestRaiseMonth.show(10)
        assert(highestRaiseMonth.count() === 2)

        val values = highestRaiseMonth.select("highest_rise_per_month").collect().map(_(0)).toList
        assert(values.head ===  90.7)
        assert(values.tail.head ===  81.4)

      }
    }
}
