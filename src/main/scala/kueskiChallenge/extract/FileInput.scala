package kueskiChallenge.extract
import kueskiChallenge.sparkSession
import org.apache.spark.sql.DataFrame

class FileInput {

  def extractFromCSV(filePath: String): DataFrame = {
    sparkSession.read
      .option("header", "true")
      .csv(filePath)
  }

}
