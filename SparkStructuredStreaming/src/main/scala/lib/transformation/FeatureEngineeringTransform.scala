package lib.transformation

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame

object FeatureEngineeringTransform {
  def process(data: DataFrame, features: Array[String]): DataFrame = {
    val assambler = new VectorAssembler()
      .setInputCols(features)
      .setOutputCol("features")

    val df = assambler.transform(data)

    df
  }
}
