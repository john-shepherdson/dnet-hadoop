package eu.dnetlib.dhp.oa.graph.hostedbymap

import com.fasterxml.jackson.databind.ObjectMapper
import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.oa.graph.hostedbymap.model.EntityInfo
import eu.dnetlib.dhp.schema.oaf.{Journal, Publication}
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.json4s
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import org.slf4j.{Logger, LoggerFactory}

object SparkPrepareHostedByInfoToApply {

  implicit val mapEncoderPInfo: Encoder[EntityInfo] = Encoders.bean(classOf[EntityInfo])

  def getList(id: String, j: Journal, name: String): List[EntityInfo] = {
    var lst: List[EntityInfo] = List()

    if (j.getIssnLinking != null && !j.getIssnLinking.equals("")) {
      lst = EntityInfo.newInstance(id, j.getIssnLinking, name) :: lst
    }
    if (j.getIssnOnline != null && !j.getIssnOnline.equals("")) {
      lst = EntityInfo.newInstance(id, j.getIssnOnline, name) :: lst
    }
    if (j.getIssnPrinted != null && !j.getIssnPrinted.equals("")) {
      lst = EntityInfo.newInstance(id, j.getIssnPrinted, name) :: lst
    }
    lst
  }

  def prepareResultInfo(spark: SparkSession, publicationPath: String): Dataset[EntityInfo] = {
    implicit val mapEncoderPubs: Encoder[Publication] = Encoders.bean(classOf[Publication])

    val mapper = new ObjectMapper()

    val dd: Dataset[Publication] = spark.read
      .textFile(publicationPath)
      .map(r => mapper.readValue(r, classOf[Publication]))

    dd.filter(p => p.getJournal != null).flatMap(p => getList(p.getId, p.getJournal, ""))

  }

  def toEntityInfo(input: String): EntityInfo = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats

    lazy val json: json4s.JValue = parse(input)
    val c: Map[String, HostedByItemType] = json.extract[Map[String, HostedByItemType]]
    toEntityItem(c.keys.head, c.values.head)
  }

  def toEntityItem(journal_id: String, hbi: HostedByItemType): EntityInfo = {

    EntityInfo.newInstance(hbi.id, journal_id, hbi.officialname, hbi.openAccess)

  }

  def joinResHBM(res: Dataset[EntityInfo], hbm: Dataset[EntityInfo]): Dataset[EntityInfo] = {
    Aggregators.resultToSingleId(
      res
        .joinWith(hbm, res.col("journalId").equalTo(hbm.col("journalId")), "left")
        .map(t2 => {
          val res: EntityInfo = t2._1
          if (t2._2 != null) {
            val ds = t2._2
            res.setHostedById(ds.getId)
            res.setOpenAccess(ds.getOpenAccess)
            res.setName(ds.getName)
          }
          res
        })
    )
  }

  def main(args: Array[String]): Unit = {

    val logger: Logger = LoggerFactory.getLogger(getClass)
    val conf: SparkConf = new SparkConf()
    val parser = new ArgumentApplicationParser(
      IOUtils.toString(
        getClass.getResourceAsStream(
          "/eu/dnetlib/dhp/oa/graph/hostedbymap/hostedby_prepare_params.json"
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

    val outputPath = parser.get("preparedInfoPath")
    val hostedByMapPath = parser.get("hostedByMapPath")

    implicit val formats = DefaultFormats

    logger.info("Getting the Datasources")

    import spark.implicits._

    //STEP1: read the hostedbymap and transform it in EntityInfo
    val hostedByInfo: Dataset[EntityInfo] =
      spark.createDataset(spark.sparkContext.textFile(hostedByMapPath)).map(toEntityInfo)

    //STEP2: create association (publication, issn), (publication, eissn), (publication, lissn)
    val resultInfoDataset: Dataset[EntityInfo] =
      prepareResultInfo(spark, graphPath + "/publication")

    //STEP3: left join resultInfo with hostedByInfo on journal_id. Reduction of all the results with the same id in just
    //one entry (one result could be associated to issn and eissn and so possivly matching more than once against the map)
    //to this entry we add the id of the datasource for the next step
    joinResHBM(resultInfoDataset, hostedByInfo).write
      .mode(SaveMode.Overwrite)
      .option("compression", "gzip")
      .json(outputPath)

  }

}
