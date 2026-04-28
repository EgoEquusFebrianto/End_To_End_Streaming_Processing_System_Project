package kudadiri.dataengineer.sparkApp

import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.avro.functions.from_avro
import org.apache.spark.sql.streaming.Trigger.ProcessingTime

import scala.io.{Source, StdIn}

object SparkStructuredStreamingApp {
  def main(args: Array[String]): Unit = {
    println("Spark Dimulai...")
    //    val ssl_location = "D:/Data_Engineer/Portofollio/End_To_End_Streaming_Processing_System_Project_V1/System_Application/secret/ssl/kafka.truststore.jks"
    val target_checkpoint = "D:/Data_Engineer/Portofollio/End_To_End_Streaming_Processing_System_Project_V1/System_Application/analytics/checkpoint"
    //    val target_storage = "D:/Data_Engineer/Portofollio/End_To_End_Streaming_Processing_System_Project_V1/System_Application/analytics/parquet"
    val target_storage = "D:/Data_Engineer/csv"

    val stream = getClass.getResourceAsStream("/avro/DataRecord.avsc")
    require(stream != null, "Schema file not found!")
    val source = Source.fromInputStream(stream)
    val avroSchema  = source.mkString
    source.close()

    val spark = SparkSession.builder()
      .appName("SparkApp")
      .master("local[3]")
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.shuffle.partitions", "3")
      .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
      .getOrCreate()

    println("Spark selesai dimuat..")

    // load model
    //    println("Muat model..")
    //    val rf_model = RandomForestClassificationModel.load("rf_model_ddos_detection")
    //    println("Model berhasil dimuat..")

    val featureCols = Array(
      "Flow_Duration", "Total_Fwd_Packets", "Total_Backward_Packets",
      "Flow_Bytes_s", "Flow_Packets_s",
      "Min_Packet_Length", "Max_Packet_Length", "Packet_Length_Mean",
      "Packet_Length_Std", "Packet_Length_Variance",
      "Flow_IAT_Mean", "Flow_IAT_Std", "Fwd_IAT_Mean", "Fwd_IAT_Std",
      "Bwd_IAT_Mean", "Bwd_IAT_Std",
      "SYN_Flag_Count", "RST_Flag_Count", "ACK_Flag_Count",
      "Fwd_Header_Length", "Bwd_Header_Length",
      "Subflow_Fwd_Packets", "Subflow_Bwd_Packets",
      "Subflow_Fwd_Bytes", "Subflow_Bwd_Bytes",
      "Init_Win_bytes_forward", "Init_Win_bytes_backward"
    )

    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")

    print("Inisialisasi Proses Transformasi Dimulai..")

    println("Proses inisialisasi proses selesai..")
    //    print("Klik tombol untuk melanjut.. ")
    //    StdIn.readLine()
    println("Transformasi data dimulai..")

    val df_raw = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "172.25.5.7:9092")
      .option("subscribe", "topic-network")
      .option("startingOffsets", "earliest")
      .load()
    //      .option("kafka.security.protocol", "SSL")
    //      .option("kafka.ssl.truststore.location", ssl_location)
    //      .option("kafka.ssl.truststore.password", "password")
    //      .load()

    val df_take = df_raw
      .select(
        from_avro(col("value"), avroSchema).alias("data"),
        col("timestamp").alias("kafkaIngestTimeStr")
      )
      .select("data.*", "kafkaIngestTimeStr")

    val df_preprocessing = df_take
      .na.drop(featureCols) // hanya drop NULL di featureCols
      .filter(
        featureCols.map { c =>
          !isnan(col(c)) &&
            col(c) =!= Double.PositiveInfinity &&
            col(c) =!= Double.NegativeInfinity
        }.reduce(_ && _)
      )

    val df_take1 = df_preprocessing.withColumn(
      "kafkaIngestTime",
      (col("kafkaIngestTimeStr").cast(DoubleType) * 1000).cast(LongType)
    )

    val df_with_start = df_take1
      .withColumn(
        "processingStartTime",
        ((current_timestamp().cast(DoubleType) * 1000).cast(LongType))
      )
      .withColumn(
        "processingStartTimeStr",
        current_timestamp().cast(TimestampType)
      )

    val cols = df_with_start.columns
    println(cols.map(c => s"'$c'").mkString("[", ", ", "]"))

    //    // vector assembly
    //    val df_vec = assembler.transform(df_with_start)
    //
    ////    // predict data
    //    val df_predicted = rf_model.transform(df_vec)
    //
    ////    val df_clean = df_predicted.drop("features", "rawPrediction", "probability")
    //    val selectedColumns = Array(
    //      "Flow_ID",
    //      "Source_IP",
    //      "Source_Port",
    //      "Destination_IP",
    //      "Destination_Port",
    //      "Protocol",
    //      "Timestamp",
    //      "Flow_Duration",
    //      "SYN_Flag_Count",
    //      "RST_Flag_Count",
    //      "ACK_Flag_Count",
    //      "Total_Fwd_Packets",
    //      "Total_Backward_Packets",
    //      "Flow_Bytes_s",
    //      "Flow_Packets_s",
    //      "Inbound",
    //      "kafkaIngestTime",
    //      "processingStartTime",
    //      "kafkaIngestTimeStr",
    //      "processingStartTimeStr",
    //      "Label",
    //      "prediction"
    //    )
    //
    //    // Seleksi kolom
    //    val df_selected = df_predicted.select(selectedColumns.map(col): _*)
    //    val df_final = df_selected
    //      .withColumn(
    //        "processingEndTime",
    //        ((current_timestamp().cast(DoubleType) * 1000).cast(LongType))
    //      )
    //      .withColumn(
    //        "processingEndTimeStr",
    //        current_timestamp().cast(TimestampType)
    //      )
    //      .withColumn(
    //        "end_to_end_latency",
    //        col("processingEndTime") - col("kafkaIngestTime")
    //      )




    //    val query = df_take.writeStream
    //      .format("console")
    //      .option("truncate", "false")
    //      .outputMode("append")
    //      .start()

    //    val query = df_take.coalesce.writeStream
    //      .format("parquet")
    //      .option("path", target_storage)
    //      .option("checkpointLocation", target_checkpoint)
    //      .outputMode("append")
    //      .start()

    //    query.awaitTermination()
    spark.stop()
  }
}