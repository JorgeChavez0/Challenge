package kueskiChallenge.runners

import kueskiChallenge.extract.FileInput
import kueskiChallenge.load.DataWriter
import kueskiChallenge.transform.HistoryDataTransform

object MovingAverageForAllTickers extends App {

  val fileInput = new FileInput
  val dataTransform = new HistoryDataTransform
  val dataWriter = new DataWriter
  val WINDOW_SIZE_IN_DAYS = 7

  def run() = {
    println("Reading data from CSV")
    val data = fileInput.extractFromCSV("resources/historical_stock_prices.csv")
    println("Calculating moving average")
    val transformedData = dataTransform.movingAverageForAllData(data, WINDOW_SIZE_IN_DAYS)
    println("Writing data")
    dataWriter.writeInConsole(transformedData)
  }

  run()
}
