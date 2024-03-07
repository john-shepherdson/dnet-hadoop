package eu.dnetlib.dhp.sx.graph

import com.fasterxml.jackson.databind.ObjectMapper
import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.schema.sx.scholix.Scholix
import eu.dnetlib.dhp.schema.sx.summary.ScholixSummary
import eu.dnetlib.dhp.sx.graph.SparkConvertObjectToJson.toInt
import org.apache.commons.io.IOUtils
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

object SparkConvertObjectToJson {

  def toInt(s: String): Option[Int] = {
    try {
      Some(s.toInt)
    } catch {
      case e: Exception => None
    }
  }

  def main(args: Array[String]): Unit = {
    val log: Logger = LoggerFactory.getLogger(getClass)
    val conf: SparkConf = new SparkConf()
    val parser = new ArgumentApplicationParser(
      IOUtils.toString(
        getClass.getResourceAsStream("/eu/dnetlib/dhp/sx/graph/convert_object_json_params.json")
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
    val objectType = parser.get("objectType")
    log.info(s"objectType  -> $objectType")
    val scholixUpdatePath = parser.get("scholixUpdatePath")
    log.info(s"scholixUpdatePath  -> $scholixUpdatePath")
    val maxPidNumberFilter = parser.get("maxPidNumberFilter")
    log.info(s"maxPidNumberFilter  -> $maxPidNumberFilter")

    implicit val scholixEncoder: Encoder[Scholix] = Encoders.kryo[Scholix]
    implicit val summaryEncoder: Encoder[ScholixSummary] = Encoders.kryo[ScholixSummary]

    val mapper = new ObjectMapper

    objectType.toLowerCase match {
      case "scholix" =>
        log.info("Serialize Scholix")
        val d: Dataset[Scholix] = spark.read.load(sourcePath).as[Scholix]
//        val u: Dataset[Scholix] = spark.read.load(s"$scholixUpdatePath/scholix").as[Scholix]
        if (maxPidNumberFilter != null && toInt(maxPidNumberFilter).isDefined) {
          val mp = toInt(maxPidNumberFilter).get
          d
            .filter(s => (s.getSource.getIdentifier.size() <= mp) && (s.getTarget.getIdentifier.size() <= mp))
            .map(s => mapper.writeValueAsString(s))(Encoders.STRING)
            .rdd
            .saveAsTextFile(targetPath, classOf[GzipCodec])
        } else {
          d
            .repartition(8000)
            .map(s => mapper.writeValueAsString(s))(Encoders.STRING)
            .rdd
            .saveAsTextFile(targetPath, classOf[GzipCodec])
        }

      case "summary" =>
        log.info("Serialize Summary")
        val d: Dataset[ScholixSummary] = spark.read.load(sourcePath).as[ScholixSummary]
        d.map(s => mapper.writeValueAsString(s))(Encoders.STRING)
          .rdd
          .repartition(1000)
          .saveAsTextFile(targetPath, classOf[GzipCodec])
    }
  }

}
