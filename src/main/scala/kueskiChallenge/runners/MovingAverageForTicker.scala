package kueskiChallenge.runners

import kueskiChallenge.extract.FileInput
import kueskiChallenge.transform.HistoryDataTransform

object MovingAverageForTicker extends App {

  val fileInput = new FileInput
  val dataTransform = new HistoryDataTransform

  def run(ticker: String) = {
    println("Reading data from CSV")
    val data = fileInput.extractFromCSV("resources/historical_stock_prices.csv")
    println("Calculating moving average")
    val transformedData = dataTransform.movingAverageForTicker(data, ticker, 7)
    transformedData.show(50, false)
  }

  run("AHH")
}
