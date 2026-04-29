package lib.load

import org.apache.spark.sql.DataFrame
import scala.collection.mutable

object ParquetSink {
  def process(data_final: DataFrame, config: mutable.HashMap[String, String]) = {

    val query = data_final.writeStream
      .format("console")
      .option("truncate", "false")
      .outputMode("append")
      .start()

//    val query = data_final.writeStream
//      .format(config("format"))
//      .option("path", config("target"))
//      .option("checkpointLocation", config("checkpoint"))
//      .outputMode(config("mode"))
//      .start()

    query.awaitTermination()
  }
}
