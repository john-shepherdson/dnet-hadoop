package eu.dnetlib.dhp.sx.graph

import com.fasterxml.jackson.databind.ObjectMapper
import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.schema.sx.scholix.Scholix
import eu.dnetlib.dhp.schema.sx.summary.ScholixSummary
import org.apache.commons.io.IOUtils
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

object SparkConvertObjectToJson {

  def main(args: Array[String]): Unit = {
    val log: Logger = LoggerFactory.getLogger(getClass)
    val conf: SparkConf = new SparkConf()
    val parser = new ArgumentApplicationParser(IOUtils.toString(getClass.getResourceAsStream("/eu/dnetlib/dhp/sx/graph/convert_object_json_params.json")))
    parser.parseArgument(args)
    val spark: SparkSession =
      SparkSession
        .builder()
        .config(conf)
        .appName(getClass.getSimpleName)
        .master(parser.get("master")).getOrCreate()

    val sourcePath = parser.get("sourcePath")
    log.info(s"sourcePath  -> $sourcePath")
    val targetPath = parser.get("targetPath")
    log.info(s"targetPath  -> $targetPath")
    val objectType = parser.get("objectType")
    log.info(s"objectType  -> $objectType")


    implicit val scholixEncoder: Encoder[Scholix] = Encoders.kryo[Scholix]
    implicit val summaryEncoder: Encoder[ScholixSummary] = Encoders.kryo[ScholixSummary]


    val mapper = new ObjectMapper

    objectType.toLowerCase match {
      case "scholix" =>
        log.info("Serialize Scholix")
        val d: Dataset[Scholix] = spark.read.load(sourcePath).as[Scholix]
        d.map(s => mapper.writeValueAsString(s))(Encoders.STRING).rdd.repartition(6000).saveAsTextFile(targetPath, classOf[GzipCodec])
      case "summary" =>
        log.info("Serialize Summary")
        val d: Dataset[ScholixSummary] = spark.read.load(sourcePath).as[ScholixSummary]
        d.map(s => mapper.writeValueAsString(s))(Encoders.STRING).rdd.repartition(1000).saveAsTextFile(targetPath, classOf[GzipCodec])
    }
  }

}
