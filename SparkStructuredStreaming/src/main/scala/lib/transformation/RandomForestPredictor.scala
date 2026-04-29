package lib.transformation

import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object RandomForestPredictor {
  def process(data: DataFrame, rf_model: RandomForestClassificationModel): DataFrame = {
    val selectedColumns = Array(
      "Flow_ID",
      "Source_IP",
      "Source_Port",
      "Destination_IP",
      "Destination_Port",
      "Protocol",
      "Timestamp",
      "Flow_Duration",
      "SYN_Flag_Count",
      "RST_Flag_Count",
      "ACK_Flag_Count",
      "Total_Fwd_Packets",
      "Total_Backward_Packets",
      "Flow_Bytes_s",
      "Flow_Packets_s",
      "Inbound",
      "kafkaIngestTime",
      "processingStartTime",
      "kafkaIngestTimeStr",
      "processingStartTimeStr",
      "Label",
      "prediction"
    )

    val df_predict = rf_model.transform(data)
    val df = df_predict.select(selectedColumns.map(col): _*)

    df
  }
}
