package eu.dnetlib.dhp.oa.graph.raw

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.common.HdfsSupport
import eu.dnetlib.dhp.schema.common.ModelSupport
import eu.dnetlib.dhp.schema.oaf.Oaf
import eu.dnetlib.dhp.utils.DHPUtils
import org.apache.spark.sql.{Encoder, Encoders, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.io.Source

object CopyIncrementalOafSparkApplication {

  def main(args: Array[String]): Unit = {
    val log = LoggerFactory.getLogger(getClass)
    val conf = new SparkConf()
    val parser = new ArgumentApplicationParser(
      Source
        .fromInputStream(
          getClass.getResourceAsStream("/eu/dnetlib/dhp/oa/graph/copy_incremental_oaf_parameters.json")
        )
        .mkString
    )
    parser.parseArgument(args)

    val spark =
      SparkSession
        .builder()
        .config(conf)
        .appName(getClass.getSimpleName)
        .getOrCreate()

    val sc: SparkContext = spark.sparkContext

    val inputPath = parser.get("inputPath")
    log.info("inputPath: {}", inputPath)

    val graphOutputPath = parser.get("graphOutputPath")
    log.info("graphOutputPath: {}", graphOutputPath)

    implicit val oafEncoder: Encoder[Oaf] = Encoders.kryo[Oaf]

    val types = ModelSupport.oafTypes.entrySet.asScala
      .map(e => Tuple2(e.getKey, e.getValue))

    val oaf = spark.read.textFile(inputPath)
    val mapper =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

    types.foreach(t =>
      oaf
        .filter(o => isOafType(o, t._1))
        .map(j => mapper.readValue(j, t._2).asInstanceOf[Oaf])
        .map(s => mapper.writeValueAsString(s))(Encoders.STRING)
        .write
        .option("compression", "gzip")
        .mode(SaveMode.Append)
        .text(s"$graphOutputPath/${t._1}")
    )
  }

  def isOafType(input: String, oafType: String): Boolean = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json: org.json4s.JValue = parse(input)
    if (oafType == "relation") {
      val hasSource = (json \ "source").extractOrElse[String](null)
      val hasTarget = (json \ "target").extractOrElse[String](null)

      hasSource != null && hasTarget != null
    } else {
      val hasId = (json \ "id").extractOrElse[String](null)
      val resultType = (json \ "resulttype" \ "classid").extractOrElse[String]("")
      hasId != null && oafType.startsWith(resultType)
    }

  }
}
