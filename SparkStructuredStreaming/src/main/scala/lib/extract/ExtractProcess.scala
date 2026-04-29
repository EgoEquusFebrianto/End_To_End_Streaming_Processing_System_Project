package lib.extract

import org.apache.spark.sql.{DataFrame, SparkSession}
import utility.ConfigUtils

import scala.collection.mutable

object ExtractProcess {
  def readStreamKafka(spark: SparkSession, settings: mutable.HashMap[String, String]): DataFrame = {
    val df_raw = spark.readStream
      .format(settings("format"))
      .option("kafka.bootstrap.servers", settings("servers"))
      .option("startingOffsets", settings("startValue"))
      .load()

    df_raw
  }
}
