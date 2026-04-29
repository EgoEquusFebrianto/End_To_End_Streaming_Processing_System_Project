package utility

import org.apache.spark.SparkConf
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.mutable
import scala.io.Source

object ConfigUtils {
  def getSparkConfigs(cluster: String): SparkConf = {
    val config: Config = ConfigFactory.load()
    val sparkConf = new SparkConf()

    config.getConfig(s"spark.$cluster")
      .entrySet()
      .forEach{ entry =>
        sparkConf.set(entry.getKey, entry.getValue.unwrapped().toString)
      }

    sparkConf
  }

  def getStreamConfigs(action: String): mutable.HashMap[String, String] = {
    val settings: mutable.HashMap[String, String] = mutable.HashMap().empty
    val config: Config = ConfigFactory.load()
    config.getConfig(s"variables.$action")
      .entrySet()
      .forEach { entry =>
        settings(entry.getKey) = entry.getValue.unwrapped().toString
      }

    settings
  }

  def getAvroSchema: String = {
    val stream = getClass.getResourceAsStream("/avro/DataRecord.avsc")
    require(stream != null, "schema file not found!")
    val source = Source.fromInputStream(stream)
    val avroSchema = source.mkString
    source.close()

    avroSchema
  }
}
