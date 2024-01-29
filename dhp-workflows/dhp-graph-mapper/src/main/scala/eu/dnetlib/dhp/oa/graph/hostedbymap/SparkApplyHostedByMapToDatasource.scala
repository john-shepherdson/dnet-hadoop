package eu.dnetlib.dhp.oa.graph.hostedbymap

import com.fasterxml.jackson.databind.ObjectMapper
import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.oa.graph.hostedbymap.model.EntityInfo
import eu.dnetlib.dhp.schema.common.ModelConstants
import eu.dnetlib.dhp.schema.oaf.Datasource
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.json4s.DefaultFormats
import org.slf4j.{Logger, LoggerFactory}

object SparkApplyHostedByMapToDatasource {

  def applyHBtoDats(join: Dataset[EntityInfo], dats: Dataset[Datasource]): Dataset[Datasource] = {
    dats
      .joinWith(join, dats.col("id").equalTo(join.col("hostedById")), "left")
      .map(t2 => {
        val d: Datasource = t2._1
        if (t2._2 != null) {
          if (d.getOpenairecompatibility.getClassid.equals(ModelConstants.UNKNOWN)) {
            d.getOpenairecompatibility.setClassid("hostedBy")
            d.getOpenairecompatibility.setClassname("collected from a compatible aggregator")
          }
        }
        d
      })(Encoders.bean((classOf[Datasource])))
  }

  def main(args: Array[String]): Unit = {

    val logger: Logger = LoggerFactory.getLogger(getClass)
    val conf: SparkConf = new SparkConf()
    val parser = new ArgumentApplicationParser(
      IOUtils.toString(
        getClass.getResourceAsStream(
          "/eu/dnetlib/dhp/oa/graph/hostedbymap/hostedby_apply_params.json"
        )
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

    val graphPath = parser.get("graphPath")
    val outputPath = parser.get("outputPath")
    val preparedInfoPath = parser.get("preparedInfoPath")

    implicit val formats = DefaultFormats

    implicit val mapEncoderPubs: Encoder[Datasource] = Encoders.bean(classOf[Datasource])
    implicit val mapEncoderEinfo: Encoder[EntityInfo] = Encoders.bean(classOf[EntityInfo])

    val mapper = new ObjectMapper()

    val dats: Dataset[Datasource] = spark.read
      .textFile(graphPath + "/datasource")
      .map(r => mapper.readValue(r, classOf[Datasource]))

    val pinfo: Dataset[EntityInfo] = Aggregators.datasourceToSingleId(
      spark.read
        .textFile(preparedInfoPath)
        .map(ei => mapper.readValue(ei, classOf[EntityInfo]))
    )

    applyHBtoDats(pinfo, dats).write
      .mode(SaveMode.Overwrite)
      .option("compression", "gzip")
      .json(outputPath)

    spark.read
      .textFile(outputPath)
      .write
      .mode(SaveMode.Overwrite)
      .option("compression", "gzip")
      .text(graphPath + "/datasource")
  }

}
