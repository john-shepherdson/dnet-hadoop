package eu.dnetlib.dhp.oa.graph.hostebymap

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.oa.graph.hostebymap.model.{DOAJModel, UnibiGoldModel}
import eu.dnetlib.dhp.oa.merge.AuthorMerger
import eu.dnetlib.dhp.schema.common.ModelConstants
import eu.dnetlib.dhp.schema.oaf.{Datasource, Organization, Publication, Relation}
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SaveMode, SparkSession}
import org.json4s.DefaultFormats
import org.slf4j.{Logger, LoggerFactory}
import org.json4s.jackson.Serialization.write

import scala.collection.mutable.ListBuffer

object SparkPrepareHostedByMapData {

  case class HostedByInfo(id: Option[String], officialname: String, journal_id: String, provenance : String, id_type: String) {}

  implicit val tupleForJoinEncoder: Encoder[(String, HostedByItemType)] = Encoders.tuple(Encoders.STRING, Encoders.product[HostedByItemType])
  implicit val mapEncoderDats: Encoder[Datasource] = Encoders.bean(classOf[Datasource])
  implicit val mapEncoderDOAJ: Encoder[DOAJModel] = Encoders.kryo[DOAJModel]
  implicit val mapEncoderUnibi: Encoder[UnibiGoldModel] = Encoders.kryo[UnibiGoldModel]
  implicit val mapEncoderHBI: Encoder[HostedByInfo] = Encoders.product[HostedByInfo]


  def toHostedByItemType(input: ((HostedByInfo, HostedByInfo), HostedByInfo)) : HostedByItemType = {
    val openaire: HostedByInfo = input._1._1
    val doaj: HostedByInfo = input._1._2
    val gold: HostedByInfo = input._2
    val isOpenAccess: Boolean = doaj == null && gold == null

    openaire.journal_id match {
      case Constants.ISSN => return HostedByItemType(openaire.id.get, openaire.officialname, openaire.journal_id, "", "", isOpenAccess)
      case Constants.EISSN => return HostedByItemType(openaire.id.get, openaire.officialname, "", openaire.journal_id, "", isOpenAccess)
      case Constants.ISSNL => return HostedByItemType(openaire.id.get, openaire.officialname, "", "", openaire.journal_id, isOpenAccess)

      // catch the default with a variable so you can print it
      case whoa => return null
    }
  }

  def toHostedByMap(input: HostedByItemType): ListBuffer[String] = {
    implicit val formats = DefaultFormats
    val serializedJSON:String = write(input)

    var hostedBy  = new ListBuffer[String]()
    if(!input.issn.equals("")){
      hostedBy += "{\"" + input.issn + "\":" + serializedJSON + "}"
    }
    if(!input.eissn.equals("")){
      hostedBy += "{\"" + input.eissn + "\":" + serializedJSON + "}"
    }
    if(!input.lissn.equals("")){
      hostedBy += "{\"" + input.lissn + "\":" + serializedJSON + "}"
    }

    hostedBy

  }


  def readOADataset(input:String, spark: SparkSession): Dataset[HostedByInfo] = {
    spark.read.textFile(input).as[Datasource].flatMap(ds => {
      val lst = new ListBuffer[HostedByInfo]()
      if (ds.getJournal == null) {
        return null
      }
      val issn: String = ds.getJournal.getIssnPrinted
      val issnl: String = ds.getJournal.getIssnOnline
      val eissn: String = ds.getJournal.getIssnOnline
      val id: String = ds.getId
      val officialname: String = ds.getOfficialname.getValue
      if (issn != null) {
        lst += HostedByInfo(Some(id), officialname, issn, Constants.OPENAIRE, Constants.ISSN)
      }
      if (issnl != null) {
        lst += HostedByInfo(Some(id), officialname, issnl, Constants.OPENAIRE, Constants.ISSNL)
      }
      if (eissn != null) {
        lst += HostedByInfo(Some(id), officialname, eissn, Constants.OPENAIRE, Constants.EISSN)
      }
      lst
    }).filter(i => i != null)
  }

  def main(args: Array[String]): Unit = {

    val logger: Logger = LoggerFactory.getLogger(getClass)
    val conf: SparkConf = new SparkConf()
    val parser = new ArgumentApplicationParser(IOUtils.toString(getClass.getResourceAsStream("/eu/dnetlib/dhp/oa/graph/hostedby/prepare_hostedby_params.json")))
    parser.parseArgument(args)
    val spark: SparkSession =
      SparkSession
        .builder()
        .config(conf)
        .appName(getClass.getSimpleName)
        .master(parser.get("master")).getOrCreate()

    import spark.implicits._

    val datasourcePath = parser.get("datasourcePath")
    val workingDirPath = parser.get("workingPath")




    logger.info("Getting the Datasources")

    val doajDataset: Dataset[DOAJModel] = spark.read.load(workingDirPath + "/doaj").as[DOAJModel]
    val unibiDataset: Dataset[UnibiGoldModel] = spark.read.load(datasourcePath).as[UnibiGoldModel]

    val oa: Dataset[HostedByInfo] = readOADataset(datasourcePath, spark)

    val doaj: Dataset[HostedByInfo] = doajDataset.flatMap(doaj => {
      val lst = new ListBuffer[HostedByInfo]()
      val issn: String = doaj.getIssn
      val eissn: String = doaj.getEissn
      val officialname: String = doaj.getJournalTitle
      if (issn != null) {
        lst += HostedByInfo(null, officialname, issn, Constants.DOAJ, Constants.ISSN)
      }
      if (eissn != null) {
        lst += HostedByInfo(null, officialname, eissn, Constants.DOAJ, Constants.EISSN)
      }
      lst
    })

    val gold: Dataset[HostedByInfo] = unibiDataset.flatMap(gold => {
      val lst = new ListBuffer[HostedByInfo]()
      val issn: String = gold.getIssn
      val issnl: String = gold.getIssn_l
      val officialname: String = gold.getTitle
      if (issn != null) {
        lst += HostedByInfo(null, officialname, issn, Constants.UNIBI, Constants.ISSN)
      }
      if (issnl != null) {
        lst += HostedByInfo(null, officialname, issnl, Constants.UNIBI, Constants.ISSNL)
      }
      lst
    })

    Aggregators.createHostedByItemTypes(oa.joinWith(doaj, oa.col("journal_id").equalTo(doaj.col("journal_id")), "left")
      .joinWith(gold, $"_1.col('journal_id')".equalTo(gold.col("journal_id")), "left").map(toHostedByItemType)
      .filter(i => i != null))
      .flatMap(toHostedByMap)
      //      .map(i => (i.id,i))
      //      .groupByKey(_._1)
      //      .agg(hostedByAggregator.toColumn)
      //      .map(p => p._2)
      .write.mode(SaveMode.Overwrite).save(s"$workingDirPath/HostedByMap")


  }


}
