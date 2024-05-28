package eu.dnetlib.dhp.oa.graph.hostedbymap

import com.fasterxml.jackson.databind.ObjectMapper
import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.oa.graph.hostedbymap.model.EntityInfo
import eu.dnetlib.dhp.schema.common.ModelConstants
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils
import eu.dnetlib.dhp.schema.oaf.{Instance, OpenAccessRoute, Publication}
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.json4s.DefaultFormats
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.JavaConverters._

object SparkApplyHostedByMapToResult {

  def applyHBtoPubs(join: Dataset[EntityInfo], pubs: Dataset[Publication]) = {
    pubs
      .joinWith(join, pubs.col("id").equalTo(join.col("id")), "left")
      .map(t2 => {
        val p: Publication = t2._1
        if (t2._2 != null) {
          val ei: EntityInfo = t2._2
          val i = p.getInstance().asScala
          if (i.size == 1) {
            val inst: Instance = i.head
            patchInstance(p, ei, inst)

          } else if (i.size == 2) {
            if (i.map(ii => ii.getCollectedfrom.getValue).contains("UnpayWall")) {
              val inst: Instance = i.filter(ii => "Crossref".equals(ii.getCollectedfrom.getValue)).head
              patchInstance(p, ei, inst)
            }
          }
        }
        p
      })(Encoders.bean(classOf[Publication]))
  }

  private def patchInstance(p: Publication, ei: EntityInfo, inst: Instance): Unit = {
    inst.getHostedby.setKey(ei.getHostedById)
    inst.getHostedby.setValue(ei.getName)
    if (ei.getOpenAccess) {
      inst.setAccessright(
        OafMapperUtils.accessRight(
          ModelConstants.ACCESS_RIGHT_OPEN,
          "Open Access",
          ModelConstants.DNET_ACCESS_MODES,
          ModelConstants.DNET_ACCESS_MODES
        )
      )
      inst.getAccessright.setOpenAccessRoute(OpenAccessRoute.gold)
      p.setBestaccessright(OafMapperUtils.createBestAccessRights(p.getInstance()));
    }
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

    implicit val mapEncoderPubs: Encoder[Publication] = Encoders.bean(classOf[Publication])
    implicit val mapEncoderEinfo: Encoder[EntityInfo] = Encoders.bean(classOf[EntityInfo])
    val mapper = new ObjectMapper()

    val pubs: Dataset[Publication] = spark.read
      .textFile(graphPath + "/publication")
      .map(r => mapper.readValue(r, classOf[Publication]))

    val pinfo: Dataset[EntityInfo] = spark.read
      .textFile(preparedInfoPath)
      .map(ei => mapper.readValue(ei, classOf[EntityInfo]))

    applyHBtoPubs(pinfo, pubs).write
      .mode(SaveMode.Overwrite)
      .option("compression", "gzip")
      .json(outputPath)

    spark.read
      .textFile(outputPath)
      .write
      .mode(SaveMode.Overwrite)
      .option("compression", "gzip")
      .text(graphPath + "/publication")
  }

}
