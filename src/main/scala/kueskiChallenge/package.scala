import org.apache.spark.sql.SparkSession

package object kueskiChallenge {

  val sparkSession: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("Kueski Challenge")
    .getOrCreate()

  sparkSession.sparkContext.setLogLevel("ERROR")
}
