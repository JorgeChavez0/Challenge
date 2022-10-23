package kueskiChallenge.transform

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

class HistoryDataTransform {

  private val MILLIS_IN_A_DAY: Long = 86400000L

  def movingAverageForTicker(historyDF: DataFrame, ticker: String, windowSize: Long): DataFrame = {
    val window = Window
      .partitionBy(col("ticker"))
      .orderBy(col("date").cast("timestamp").cast("long"))
      .rangeBetween(-( (windowSize-1) * MILLIS_IN_A_DAY ),0)

    val movingAverageDF = historyDF
      .select(col("ticker"),col("close"),col("date"),mean("close").over(window).as("average"))
      .where(col("ticker") === ticker)

    movingAverageDF
  }

  def movingAverageForAllData(historyDF: DataFrame, windowSize: Long): DataFrame = {
    val window = Window
      .partitionBy(col("ticker"))
      .orderBy(col("date").cast("timestamp").cast("long"))
      .rangeBetween(-( (windowSize - 1) * MILLIS_IN_A_DAY ), 0)

    val movingAverageDF = historyDF
      .select(col("ticker"), col("close"), col("date"), mean("close").over(window).as("average"))

    movingAverageDF
  }
}
