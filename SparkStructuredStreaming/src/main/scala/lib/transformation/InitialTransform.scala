package lib.transformation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.avro.functions.from_avro
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object InitialTransform {
  def process(data: DataFrame, schema: String, features: Array[String]): DataFrame = {
    val df_extract = data
      .select(
        from_avro(col("value"), schema).alias("data"),
        col("timestamp").alias("kafkaIngestTimeStr")
      )
      .select("data.*", "kafkaIngestTimeStr")

    val df_preprocessing = df_extract
      .na.drop(features)
      .filter(
        features.map{ c =>
          !isnan(col(c)) &&
            col(c) =!= Double.PositiveInfinity &&
            col(c) =!= Double.NegativeInfinity
        }.reduce(_ && _)
      )

    val df = df_preprocessing
      .withColumn(
        "kafkaIngestTime",
        (col("kafkaIngestTimeStr").cast(DoubleType) * 1000).cast(LongType)
      )
      .withColumn(
        "processingStartTime",
        ((current_timestamp().cast(DoubleType) * 1000).cast(LongType))
      )
      .withColumn(
        "processingStartTimeStr",
        current_timestamp().cast(TimestampType)
      )

    df
  }
}
