package eu.dnetlib.dhp.oa.graph.hostedbymap

import com.fasterxml.jackson.databind.ObjectMapper
import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.oa.graph.hostedbymap.model.EntityInfo
import eu.dnetlib.dhp.schema.common.ModelConstants
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils
import eu.dnetlib.dhp.schema.oaf.{Datasource, Instance, OpenAccessRoute, Publication}
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SaveMode, SparkSession}
import org.json4s.DefaultFormats
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

//a. publication join risultato del passo precedente su result id (left) setto la istanza (se piu' di una instance
// nel result => salto)con l'hosted by anche access right della instance se openaccess e' true


object SparkApplyHostedByMapToResult {

  def applyHBtoPubs(join: Dataset[EntityInfo], pubs: Dataset[Publication]) = {
    pubs.joinWith(join, pubs.col("id").equalTo(join.col("id")), "left")
      .map(t2 => {
        val p: Publication = t2._1
        if (t2._2 != null) {
          val ei: EntityInfo = t2._2
          val i = p.getInstance().asScala
          if (i.size == 1) {
            val inst: Instance = i(0)
            inst.getHostedby.setKey(ei.getHb_id)
            inst.getHostedby.setValue(ei.getName)
            if (ei.getOpenaccess) {
              inst.setAccessright(OafMapperUtils.accessRight(ModelConstants.ACCESS_RIGHT_OPEN, "Open Access", ModelConstants.DNET_ACCESS_MODES, ModelConstants.DNET_ACCESS_MODES))
              inst.getAccessright.setOpenAccessRoute(OpenAccessRoute.hybrid)
              p.setBestaccessright(OafMapperUtils.createBestAccessRights(p.getInstance()));
            }

          }
        }
        p
      })(Encoders.bean(classOf[Publication]))
  }
  def main(args: Array[String]): Unit = {


    val logger: Logger = LoggerFactory.getLogger(getClass)
    val conf: SparkConf = new SparkConf()
    val parser = new ArgumentApplicationParser(IOUtils.toString(getClass.getResourceAsStream("/eu/dnetlib/dhp/oa/graph/hostedbymap/hostedby_apply_params.json")))
    parser.parseArgument(args)
    val spark: SparkSession =
      SparkSession
        .builder()
        .config(conf)
        .appName(getClass.getSimpleName)
        .master(parser.get("master")).getOrCreate()


    val graphPath = parser.get("graphPath")

    val outputPath = parser.get("outputPath")
    val preparedInfoPath = parser.get("preparedInfoPath")


    implicit val formats = DefaultFormats


    implicit val mapEncoderPubs: Encoder[Publication] = Encoders.bean(classOf[Publication])
    implicit val mapEncoderEinfo: Encoder[EntityInfo] = Encoders.bean(classOf[EntityInfo])
    val mapper = new ObjectMapper()

    val pubs : Dataset[Publication] = spark.read.textFile(graphPath + "/publication")
      .map(r => mapper.readValue(r, classOf[Publication]))

    val pinfo : Dataset[EntityInfo] = spark.read.textFile(preparedInfoPath)
        .map(ei => mapper.readValue(ei, classOf[EntityInfo]))

    //a. publication join risultato del passo precedente su result id (left) setto la istanza (se piu' di una instance
    // nel result => salto)con l'hosted by anche access right della instance se openaccess e' true
    applyHBtoPubs(pinfo, pubs).write.mode(SaveMode.Overwrite).option("compression","gzip").json(outputPath)



  }


}
