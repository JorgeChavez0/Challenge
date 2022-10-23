package kueskiChallenge.runners

import kueskiChallenge.extract.FileInput
import kueskiChallenge.load.DataWriter
import kueskiChallenge.transform.HistoryDataTransform

object MovingAverageForTicker extends App {

  val fileInput = new FileInput
  val dataTransform = new HistoryDataTransform
  val dataWriter = new DataWriter
  val WINDOW_SIZE_IN_DAYS = 7

  def run(ticker: String) = {
    println("Reading data from CSV")
    val data = fileInput.extractFromCSV("resources/historical_stock_prices.csv")
    println("Calculating moving average")
    val transformedData = dataTransform.movingAverageForTicker(data, ticker, WINDOW_SIZE_IN_DAYS)
    println("Writing data")
    dataWriter.writeInConsole(transformedData)
  }

  run("AHH")
}
