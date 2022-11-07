package eu.dnetlib.dhp.oa.graph.hostedbymap

import com.fasterxml.jackson.databind.ObjectMapper
import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.common.HdfsSupport
import eu.dnetlib.dhp.oa.graph.hostedbymap.model.{DOAJModel, UnibiGoldModel}
import eu.dnetlib.dhp.schema.oaf.Datasource
import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import org.json4s.DefaultFormats
import org.slf4j.{Logger, LoggerFactory}

import java.io.{File, PrintWriter}
import scala.collection.JavaConverters._

object SparkProduceHostedByMap {

  implicit val tupleForJoinEncoder: Encoder[(String, HostedByItemType)] =
    Encoders.tuple(Encoders.STRING, Encoders.product[HostedByItemType])

  def toHostedByItemType(input: ((HostedByInfo, HostedByInfo), HostedByInfo)): HostedByItemType = {
    val openaire: HostedByInfo = input._1._1
    val doaj: HostedByInfo = input._1._2
    val gold: HostedByInfo = input._2
    val isOpenAccess: Boolean = doaj == null && gold == null

    openaire.journal_id match {
      case Constants.ISSN =>
        HostedByItemType(
          openaire.id,
          openaire.officialname,
          openaire.journal_id,
          "",
          "",
          isOpenAccess
        )
      case Constants.EISSN =>
        HostedByItemType(
          openaire.id,
          openaire.officialname,
          "",
          openaire.journal_id,
          "",
          isOpenAccess
        )
      case Constants.ISSNL =>
        HostedByItemType(
          openaire.id,
          openaire.officialname,
          "",
          "",
          openaire.journal_id,
          isOpenAccess
        )

      // catch the default with a variable so you can print it
      case whoa => null
    }
  }

  def toHostedByMap(input: (String, HostedByItemType)): String = {
    import org.json4s.jackson.Serialization

    implicit val formats = org.json4s.DefaultFormats

    val map: Map[String, HostedByItemType] = Map(input._1 -> input._2)

    Serialization.write(map)

  }

  def getHostedByItemType(
    id: String,
    officialname: String,
    issn: String,
    eissn: String,
    issnl: String,
    oa: Boolean
  ): HostedByItemType = {
    if (issn != null) {
      if (eissn != null) {
        if (issnl != null) {
          HostedByItemType(id, officialname, issn, eissn, issnl, oa)
        } else {
          HostedByItemType(id, officialname, issn, eissn, "", oa)
        }
      } else {
        if (issnl != null) {
          HostedByItemType(id, officialname, issn, "", issnl, oa)
        } else {
          HostedByItemType(id, officialname, issn, "", "", oa)
        }
      }
    } else {
      if (eissn != null) {
        if (issnl != null) {
          HostedByItemType(id, officialname, "", eissn, issnl, oa)
        } else {
          HostedByItemType(id, officialname, "", eissn, "", oa)
        }
      } else {
        if (issnl != null) {
          HostedByItemType(id, officialname, "", "", issnl, oa)
        } else {
          HostedByItemType("", "", "", "", "", oa)
        }
      }
    }
  }

  def oaToHostedbyItemType(dats: Datasource): HostedByItemType = {
    if (dats.getJournal != null) {

      return getHostedByItemType(
        dats.getId,
        dats.getOfficialname.getValue,
        dats.getJournal.getIssnPrinted,
        dats.getJournal.getIssnOnline,
        dats.getJournal.getIssnLinking,
        false
      )
    }
    HostedByItemType("", "", "", "", "", false)
  }

  def oaHostedByDataset(spark: SparkSession, datasourcePath: String): Dataset[HostedByItemType] = {

    import spark.implicits._

    val mapper = new ObjectMapper()

    implicit var encoderD = Encoders.kryo[Datasource]

    val dd: Dataset[Datasource] = spark.read
      .textFile(datasourcePath)
      .map(r => mapper.readValue(r, classOf[Datasource]))

    dd.map { ddt => oaToHostedbyItemType(ddt) }.filter(hb => !(hb.id.equals("")))

  }

  def goldToHostedbyItemType(gold: UnibiGoldModel): HostedByItemType = {
    return getHostedByItemType(
      Constants.UNIBI,
      gold.getTitle,
      gold.getIssn,
      "",
      gold.getIssnL,
      true
    )
  }

  def goldHostedByDataset(
    spark: SparkSession,
    datasourcePath: String
  ): Dataset[HostedByItemType] = {
    import spark.implicits._

    implicit val mapEncoderUnibi: Encoder[UnibiGoldModel] = Encoders.kryo[UnibiGoldModel]

    val mapper = new ObjectMapper()

    val dd: Dataset[UnibiGoldModel] = spark.read
      .textFile(datasourcePath)
      .map(r => mapper.readValue(r, classOf[UnibiGoldModel]))

    dd.map { ddt => goldToHostedbyItemType(ddt) }.filter(hb => !(hb.id.equals("")))

  }

  def doajToHostedbyItemType(doaj: DOAJModel): HostedByItemType = {
    if (doaj.getOaStart == null) {
      return getHostedByItemType(
        Constants.DOAJ,
        doaj.getJournalTitle,
        doaj.getIssn,
        doaj.getEissn,
        "",
        true
      )
    }
    return getHostedByItemType(
      Constants.DOAJ,
      doaj.getJournalTitle,
      doaj.getIssn,
      doaj.getEissn,
      "",
      true
    )
  }

  def doajHostedByDataset(
    spark: SparkSession,
    datasourcePath: String
  ): Dataset[HostedByItemType] = {
    import spark.implicits._

    implicit val mapEncoderDOAJ: Encoder[DOAJModel] = Encoders.kryo[DOAJModel]

    val mapper = new ObjectMapper()

    val dd: Dataset[DOAJModel] = spark.read
      .textFile(datasourcePath)
      .map(r => mapper.readValue(r, classOf[DOAJModel]))

    dd.map { ddt => doajToHostedbyItemType(ddt) }.filter(hb => !(hb.id.equals("")))

  }

  def toList(input: HostedByItemType): List[(String, HostedByItemType)] = {
    var lst: List[(String, HostedByItemType)] = List()
    if (!input.issn.equals("")) {
      lst = (input.issn, input) :: lst
    }
    if (!input.eissn.equals("")) {
      lst = (input.eissn, input) :: lst
    }
    if (!input.lissn.equals("")) {
      lst = (input.lissn, input) :: lst
    }
    lst
  }

  def writeToHDFS(input: Array[String], outputPath: String, hdfsNameNode: String): Unit = {
    val conf = new Configuration()

    conf.set("fs.defaultFS", hdfsNameNode)
    val fs = FileSystem.get(conf)
    val output = fs.create(new Path(outputPath))
    val writer = new PrintWriter(output)
    try {
      input.foreach(hbi => writer.println(hbi))
    } finally {
      writer.close()

    }

  }

  def main(args: Array[String]): Unit = {

    val logger: Logger = LoggerFactory.getLogger(getClass)
    val conf: SparkConf = new SparkConf()
    val parser = new ArgumentApplicationParser(
      IOUtils.toString(
        getClass.getResourceAsStream("/eu/dnetlib/dhp/oa/graph/hostedbymap/hostedby_params.json")
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

    val datasourcePath = parser.get("datasourcePath")
    val workingDirPath = parser.get("workingPath")
    val outputPath = parser.get("outputPath")

    implicit val formats = DefaultFormats

    logger.info("Getting the Datasources")

    HdfsSupport.remove(outputPath, spark.sparkContext.hadoopConfiguration)

    Aggregators
      .explodeHostedByItemType(
        oaHostedByDataset(spark, datasourcePath)
          .union(goldHostedByDataset(spark, workingDirPath + "/unibi_gold.json"))
          .union(doajHostedByDataset(spark, workingDirPath + "/doaj.json"))
          .flatMap(hbi => toList(hbi))
      )
      .filter(hbi => hbi._2.id.startsWith("10|"))
      .map(hbi => toHostedByMap(hbi))(Encoders.STRING)
      .rdd
      .saveAsTextFile(outputPath, classOf[GzipCodec])

  }

}
