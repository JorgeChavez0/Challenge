import kueskiChallenge.extract.FileInput
import kueskiChallenge.transform.HistoryDataTransform
import org.apache.spark.sql.Row
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.functions._

class HistoryDataTransformTest extends AnyFunSuite {

  test("Canary Test") {
    assert(true)
  }

  test("Moving average for AHH") {
    val fileInput = new FileInput
    val dataTransform = new HistoryDataTransform

    val data = fileInput.extractFromCSV("resources/historical_stock_prices_lite.csv")
    val transformedData = dataTransform.movingAverageForTicker(data, "AHH", 7)
    val dataArray = transformedData.select(col("close")).take(7)
    val averageArray = transformedData.select(col("average")).take(7)

    var dataSum = 0.0
    dataArray.foreach(a => dataSum = dataSum + a.getString(0).toDouble)

    val average = dataSum / 7

    println("Calculated avg = " + average)
    println("Process Moving average = " + averageArray(6).getDouble(0))
    assert(average === averageArray(6).getDouble(0))
  }

  test("Moving average for APO") {
    val fileInput = new FileInput
    val dataTransform = new HistoryDataTransform

    val data = fileInput.extractFromCSV("resources/historical_stock_prices_lite.csv")
    val transformedData = dataTransform.movingAverageForTicker(data, "APO", 7)
    val dataArray = transformedData.select(col("close")).take(7)
    val averageArray = transformedData.select(col("average")).take(7)

    var dataSum = 0.0
    dataArray.foreach(a => dataSum = dataSum + a.getString(0).toDouble)

    val average = dataSum / 7

    println("Calculated avg = " + average)
    println("Process Moving average = " + averageArray(6).getDouble(0))
    assert(average === averageArray(6).getDouble(0))
  }
}
