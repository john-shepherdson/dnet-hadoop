package eu.dnetlib.dhp.sx.graph

import com.fasterxml.jackson.databind.ObjectMapper
import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.schema.oaf.Result
import org.apache.commons.io.IOUtils
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

object SparkConvertDatasetToJsonRDD {

  def main(args: Array[String]): Unit = {
    val log: Logger = LoggerFactory.getLogger(getClass)
    val conf: SparkConf = new SparkConf()
    val parser = new ArgumentApplicationParser(
      IOUtils.toString(
        getClass.getResourceAsStream("/eu/dnetlib/dhp/sx/graph/convert_dataset_json_params.json")
      )
    )
    parser.parseArgument(args)
    val spark: SparkSession =
      SparkSession
        .builder()
        .config(conf)
        .appName(getClass.getSimpleName)
        .master(parser.get("master"))
        .getOrCreate()

    val sourcePath = parser.get("sourcePath")
    log.info(s"sourcePath  -> $sourcePath")
    val targetPath = parser.get("targetPath")
    log.info(s"targetPath  -> $targetPath")

    val resultObject = List("publication", "dataset", "software", "otherResearchProduct")
    val mapper = new ObjectMapper()
    implicit val oafEncoder: Encoder[Result] = Encoders.kryo(classOf[Result])

    resultObject.foreach { item =>
      spark.read
        .load(s"$sourcePath/$item")
        .as[Result]
        .map(r => mapper.writeValueAsString(r))(Encoders.STRING)
        .rdd
        .saveAsTextFile(s"$targetPath/${item.toLowerCase}", classOf[GzipCodec])
    }
  }

}
