package eu.dnetlib.dhp.oa.graph.raw

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.common.HdfsSupport
import eu.dnetlib.dhp.schema.common.ModelSupport
import eu.dnetlib.dhp.utils.DHPUtils
import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.io.Source

object CopyHdfsOafSparkApplication {

  def main(args: Array[String]): Unit = {
    val log = LoggerFactory.getLogger(getClass)
    val conf = new SparkConf()
    val parser = new ArgumentApplicationParser(
      Source
        .fromInputStream(
          getClass.getResourceAsStream("/eu/dnetlib/dhp/oa/graph/copy_hdfs_oaf_parameters.json")
        )
        .mkString
    )
    parser.parseArgument(args)

    val spark =
      SparkSession
        .builder()
        .config(conf)
        .appName(getClass.getSimpleName)
        .master(parser.get("master"))
        .getOrCreate()

    val sc: SparkContext = spark.sparkContext

    val mdstoreManagerUrl = parser.get("mdstoreManagerUrl")
    log.info("mdstoreManagerUrl: {}", mdstoreManagerUrl)

    val mdFormat = parser.get("mdFormat")
    log.info("mdFormat: {}", mdFormat)

    val mdLayout = parser.get("mdLayout")
    log.info("mdLayout: {}", mdLayout)

    val mdInterpretation = parser.get("mdInterpretation")
    log.info("mdInterpretation: {}", mdInterpretation)

    val hdfsPath = parser.get("hdfsPath")
    log.info("hdfsPath: {}", hdfsPath)

    val paths =
      DHPUtils.mdstorePaths(mdstoreManagerUrl, mdFormat, mdLayout, mdInterpretation, true).asScala

    val validPaths: List[String] =
      paths.filter(p => HdfsSupport.exists(p, sc.hadoopConfiguration)).toList

    if (validPaths.nonEmpty) {
      val oaf = spark.read
        .textFile(validPaths: _*)
        .map(v => (getOafType(v), v))(Encoders.tuple(Encoders.STRING, Encoders.STRING))
        .cache()

      try {
        ModelSupport.oafTypes
          .keySet()
          .asScala
          .foreach(entity =>
            oaf
              .filter(s"_1 = '${entity}'")
              .selectExpr("_2")
              .write
              .option("compression", "gzip")
              .mode(SaveMode.Append)
              .text(s"$hdfsPath/${entity}")
          )
      } finally {
        oaf.unpersist()
      }
    }
  }

  def getOafType(input: String): String = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json: org.json4s.JValue = parse(input)

    val hasId = (json \ "id").extractOrElse[String](null)
    val hasSource = (json \ "source").extractOrElse[String](null)
    val hasTarget = (json \ "target").extractOrElse[String](null)

    if (hasId == null && hasSource != null && hasTarget != null) {
      "relation"
    } else if (hasId != null) {
      val oafType: String = ModelSupport.idPrefixEntity.get(hasId.substring(0, 2))

      oafType match {
        case "result" =>
          (json \ "resulttype" \ "classid").extractOrElse[String](null) match {
            case "other" => "otherresearchproduct"
            case any     => any
          }
        case _ => oafType
      }
    } else {
      null
    }
  }
}
