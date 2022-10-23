package kueskiChallenge.runners

import kueskiChallenge.extract.FileInput
import kueskiChallenge.load.DataWriter
import kueskiChallenge.transform.HistoryDataTransform

object MovingAverageForTicker extends App {

  val fileInput = new FileInput
  val dataTransform = new HistoryDataTransform
  val dataWriter = new DataWriter

  def run(ticker: String) = {
    println("Reading data from CSV")
    val data = fileInput.extractFromCSV("resources/historical_stock_prices.csv")
    println("Calculating moving average")
    val transformedData = dataTransform.movingAverageForTicker(data, ticker, 7)
    println("Writing data")
    dataWriter.writeInConsole(transformedData)
  }

  run("AHH")
}
