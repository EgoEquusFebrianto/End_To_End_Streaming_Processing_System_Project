package utility

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object SparkSessionUtils {
  def createSparkSession(config: SparkConf): SparkSession = {
    val spark = SparkSession.builder()
      .config(conf = config)
      .getOrCreate()

    spark
  }
}
