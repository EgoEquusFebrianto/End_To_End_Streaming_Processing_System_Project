package lib.transformation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object FinalTransform {
  def process(data: DataFrame): DataFrame = {
    val df = data
      .withColumn(
        "processingEndTime",
        ((current_timestamp().cast(DoubleType) * 1000).cast(LongType))
      )
      .withColumn(
        "processingEndTimeStr",
        current_timestamp().cast(TimestampType)
      )
      .withColumn(
        "end_to_end_latency",
        col("processingEndTime") - col("kafkaIngestTime")
      )

    df
  }
}
