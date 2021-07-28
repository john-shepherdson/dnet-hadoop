package eu.dnetlib.dhp.oa.graph.hostebymap

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.oa.graph.hostebymap.model.{DOAJModel, UnibiGoldModel}
import eu.dnetlib.dhp.schema.oaf.{Datasource}
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SaveMode, SparkSession}
import org.json4s.DefaultFormats
import org.slf4j.{Logger, LoggerFactory}

import com.fasterxml.jackson.databind.ObjectMapper

object SparkPrepareHostedByMapData {


  implicit val tupleForJoinEncoder: Encoder[(String, HostedByItemType)] = Encoders.tuple(Encoders.STRING, Encoders.product[HostedByItemType])






  def toHostedByItemType(input: ((HostedByInfo, HostedByInfo), HostedByInfo)) : HostedByItemType = {
    val openaire: HostedByInfo = input._1._1
    val doaj: HostedByInfo = input._1._2
    val gold: HostedByInfo = input._2
    val isOpenAccess: Boolean = doaj == null && gold == null

    openaire.journal_id match {
      case Constants.ISSN =>  HostedByItemType(openaire.id, openaire.officialname, openaire.journal_id, "", "", isOpenAccess)
      case Constants.EISSN =>  HostedByItemType(openaire.id, openaire.officialname, "", openaire.journal_id, "", isOpenAccess)
      case Constants.ISSNL =>  HostedByItemType(openaire.id, openaire.officialname, "", "", openaire.journal_id, isOpenAccess)

      // catch the default with a variable so you can print it
      case whoa =>  null
    }
  }

//  def toHostedByMap(input: HostedByItemType): ListBuffer[String] = {
//    implicit val formats = DefaultFormats
//    val serializedJSON:String = write(input)
//
//    var hostedBy  = new ListBuffer[String]()
//    if(!input.issn.equals("")){
//      hostedBy += "{\"" + input.issn + "\":" + serializedJSON + "}"
//    }
//    if(!input.eissn.equals("")){
//      hostedBy += "{\"" + input.eissn + "\":" + serializedJSON + "}"
//    }
//    if(!input.lissn.equals("")){
//      hostedBy += "{\"" + input.lissn + "\":" + serializedJSON + "}"
//    }
//
//    hostedBy
//
//  }

  def getHostedByItemType(id:String, officialname: String, issn:String, eissn:String, issnl:String, oa:Boolean): HostedByItemType = {
    if(issn != null){
      if(eissn != null){
        if(issnl != null){
          HostedByItemType(id, officialname, issn, eissn, issnl  , oa)
        }else{
          HostedByItemType(id, officialname, issn, eissn, ""  , oa)
        }
      }else{
        if(issnl != null){
          HostedByItemType(id, officialname, issn, "", issnl  , oa)
        }else{
          HostedByItemType(id, officialname, issn, "", ""  , oa)
        }
      }
    }else{
      if(eissn != null){
        if(issnl != null){
          HostedByItemType(id, officialname, "", eissn, issnl  , oa)
        }else{
          HostedByItemType(id, officialname, "", eissn, ""  , oa)
        }
      }else{
        if(issnl != null){
          HostedByItemType(id, officialname, "", "", issnl  , oa)
        }else{
          HostedByItemType("", "", "", "", ""  , oa)
        }
      }
    }
  }

  def oaToHostedbyItemType(dats: Datasource): HostedByItemType = {
    if (dats.getJournal != null) {

      return getHostedByItemType(dats.getId, dats.getOfficialname.getValue, dats.getJournal.getIssnPrinted, dats.getJournal.getIssnOnline, dats.getJournal.getIssnLinking, false)
    }
    HostedByItemType("","","","","",false)
  }

  def oaHostedByDataset(spark:SparkSession, datasourcePath : String) : Dataset[HostedByItemType] = {

    import spark.implicits._


    val mapper = new ObjectMapper()

    implicit var encoderD = Encoders.kryo[Datasource]

    val dd : Dataset[Datasource] = spark.read.textFile(datasourcePath)
      .map(r => mapper.readValue(r, classOf[Datasource]))

    dd.map{ddt => oaToHostedbyItemType(ddt)}.filter(hb => !(hb.id.equals("")))

  }


  def goldToHostedbyItemType(gold: UnibiGoldModel): HostedByItemType = {
    return getHostedByItemType(Constants.UNIBI, gold.getTitle, gold.getIssn, "", gold.getIssn_l, true)
  }


  def goldHostedByDataset(spark:SparkSession, datasourcePath:String) : Dataset[HostedByItemType] = {
    import spark.implicits._

    implicit val mapEncoderUnibi: Encoder[UnibiGoldModel] = Encoders.kryo[UnibiGoldModel]

    val mapper = new ObjectMapper()

    val dd : Dataset[UnibiGoldModel] = spark.read.textFile(datasourcePath)
      .map(r => mapper.readValue(r, classOf[UnibiGoldModel]))

    dd.map{ddt => goldToHostedbyItemType(ddt)}.filter(hb => !(hb.id.equals("")))

  }

  def doajToHostedbyItemType(doaj: DOAJModel): HostedByItemType = {

    return getHostedByItemType(Constants.DOAJ, doaj.getJournalTitle, doaj.getIssn, doaj.getEissn, "", true)
  }

  def doajHostedByDataset(spark:SparkSession, datasourcePath:String) : Dataset[HostedByItemType] = {
    import spark.implicits._

    implicit val mapEncoderDOAJ: Encoder[DOAJModel] = Encoders.kryo[DOAJModel]

    val mapper = new ObjectMapper()

    val dd : Dataset[DOAJModel] = spark.read.textFile(datasourcePath)
      .map(r => mapper.readValue(r, classOf[DOAJModel]))

    dd.map{ddt => doajToHostedbyItemType(ddt)}.filter(hb => !(hb.id.equals("")))

  }

  def toList(input: HostedByItemType): List[(String, HostedByItemType)] = {
    var lst : List[(String, HostedByItemType)] = List()
    if(!input.issn.equals("")){
      lst = (input.issn, input) :: lst
    }
    if(!input.eissn.equals("")){
      lst = (input.eissn, input) :: lst
    }
    if(!input.lissn.equals("")){
      lst = (input.lissn, input) :: lst
    }
    lst
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



    implicit val formats = DefaultFormats


    logger.info("Getting the Datasources")

   // val doajDataset: Dataset[DOAJModel] = spark.read.textFile(workingDirPath + "/doaj").as[DOAJModel]

    val dats : Dataset[HostedByItemType] =
      oaHostedByDataset(spark, datasourcePath)
      .union(goldHostedByDataset(spark, workingDirPath + "/unibi_gold"))
      .union(doajHostedByDataset(spark, workingDirPath + "/doaj"))
    dats.flatMap(hbi => toList(hbi))
      .groupByKey(_._1)


//
//

//

//
//    Aggregators.createHostedByItemTypes(oa.joinWith(doaj, oa.col("journal_id").equalTo(doaj.col("journal_id")), "left")
//      .joinWith(gold, $"_1.col('journal_id')".equalTo(gold.col("journal_id")), "left").map(toHostedByItemType)
//      .filter(i => i != null))
//      .flatMap(toHostedByMap)
//      .write.mode(SaveMode.Overwrite).save(s"$workingDirPath/HostedByMap")
//
//
  }


}
