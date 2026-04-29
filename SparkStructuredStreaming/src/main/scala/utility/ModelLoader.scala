package utility

import org.apache.spark.ml.classification.RandomForestClassificationModel

object ModelLoader {
  def getModelRandomForest(path: String): RandomForestClassificationModel = {
    println("Memanggil model..")
    val rf_model = RandomForestClassificationModel.load(path)
    println("Model berhasil dipanggil..")

    rf_model
  }
}
