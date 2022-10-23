package kueskiChallenge.load

import org.apache.spark.sql.{DataFrame, SaveMode}

class DataWriter {

  val outputFolder: String = "output/"

  def writeInConsole(data: DataFrame): Unit = {
    data.show(50, false)
  }

  def writeInCSV(data: DataFrame, ticker: String = "All"): Unit = {
    data.write.mode(SaveMode.Overwrite).csv(outputFolder + "csv/" + ticker + "-MovingAverage")
  }

  def writeInParquet(data: DataFrame, ticker: String = "All"): Unit = {
    data.write.mode(SaveMode.Overwrite).parquet(outputFolder + "parquet/" + ticker + "-MovingAverage")
  }

}
