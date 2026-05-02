import lib.extract.ExtractProcess
import lib.load.ParquetSink
import lib.transformation.{FeatureEngineeringTransform, FinalTransform, InitialTransform, RandomForestPredictor}
import utility.ConfigUtils._
import utility.ModelLoader
import utility.SparkSessionUtils._

object SparkStructuredStreamingApp {
  def main(args: Array[String]): Unit = {
    println("Spark Application Dimulai...")

    val config = getSparkConfigs("local")
    val spark = createSparkSession(config)

    println("Spark selesai dimuat..")

    println("Memuat model..")
//    val rf_model = ModelLoader.getModelRandomForest("rf_model_ddos_detection")
    println("Model berhasil dimuat..")

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

    val schema = getAvroSchema
    val settings = getStreamConfigs("readStream")
    println("Proses inisialisasi proses selesai..")
    println("Process Pipeline Data Streaming dimulai..")

    // Extract Data
    val df_raw = ExtractProcess.readStreamKafka(spark, settings)

    // Awal Transformasi Data
    val df_initial = InitialTransform.process(df_raw, schema, featureCols)

    // Feature Engineering
    val df_vec = FeatureEngineeringTransform.process(df_initial, featureCols)

//    // Random Forest Prediction
//    val df_pred = RandomForestPredictor.process(df_vec, rf_model)
//
//    // Final Process
//    val df_final = FinalTransform.process(df_pred)

    // Store (Simpan) Data
    val sinkConfig = getStreamConfigs("sink")
    ParquetSink.process(df_vec, sinkConfig)

    println("Pipeline Data Streaming Berhenti...")
    spark.stop()
    println("Spark Application Terminate...")
  }
}