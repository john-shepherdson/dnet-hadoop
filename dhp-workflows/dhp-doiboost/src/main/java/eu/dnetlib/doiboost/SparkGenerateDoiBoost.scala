package eu.dnetlib.doiboost

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.schema.oaf.{Publication, Relation, Dataset => OafDataset}
import eu.dnetlib.doiboost.mag.ConversionUtil
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.JavaConverters._

object SparkGenerateDoiBoost {

  def main(args: Array[String]): Unit = {

    val logger: Logger = LoggerFactory.getLogger(getClass)
    val conf: SparkConf = new SparkConf()
    val parser = new ArgumentApplicationParser(IOUtils.toString(getClass.getResourceAsStream("/eu/dnetlib/dhp/doiboost/generate_doiboost_params.json")))
    parser.parseArgument(args)
    val spark: SparkSession =
      SparkSession
        .builder()
        .config(conf)
        .appName(getClass.getSimpleName)
        .master(parser.get("master")).getOrCreate()

    import spark.implicits._
    val crossrefPublicationPath = parser.get("crossrefPublicationPath")
    val crossrefDatasetPath = parser.get("crossrefDatasetPath")
    val uwPublicationPath = parser.get("uwPublicationPath")
    val magPublicationPath = parser.get("magPublicationPath")
    val orcidPublicationPath = parser.get("orcidPublicationPath")
    val workingDirPath = parser.get("workingDirPath")


    logger.info("Phase 1) repartition and move all the dataset in a same working folder")
    spark.read.load(crossrefPublicationPath).as(Encoders.bean(classOf[Publication])).map(s => s)(Encoders.kryo[Publication]).write.mode(SaveMode.Overwrite).save(s"$workingDirPath/crossrefPublication")
    spark.read.load(crossrefDatasetPath).as(Encoders.bean(classOf[OafDataset])).map(s => s)(Encoders.kryo[OafDataset]).write.mode(SaveMode.Overwrite).save(s"$workingDirPath/crossrefDataset")
    spark.read.load(uwPublicationPath).as(Encoders.bean(classOf[Publication])).map(s => s)(Encoders.kryo[Publication]).write.mode(SaveMode.Overwrite).save(s"$workingDirPath/uwPublication")
    spark.read.load(orcidPublicationPath).as(Encoders.bean(classOf[Publication])).map(s => s)(Encoders.kryo[Publication]).write.mode(SaveMode.Overwrite).save(s"$workingDirPath/orcidPublication")
    spark.read.load(magPublicationPath).as(Encoders.bean(classOf[Publication])).map(s => s)(Encoders.kryo[Publication]).write.mode(SaveMode.Overwrite).save(s"$workingDirPath/magPublication")

    implicit val mapEncoderPub: Encoder[Publication] = Encoders.kryo[Publication]
    implicit val mapEncoderDataset: Encoder[OafDataset] = Encoders.kryo[OafDataset]
    implicit val tupleForJoinEncoder: Encoder[(String, Publication)] = Encoders.tuple(Encoders.STRING, mapEncoderPub)
    implicit val mapEncoderRel: Encoder[Relation] = Encoders.kryo[Relation]

    logger.info("Phase 2) Join Crossref with UnpayWall")

    val crossrefPublication: Dataset[(String, Publication)] = spark.read.load(s"$workingDirPath/crossrefPublication").as[Publication].map(p => (p.getId, p))
    val uwPublication: Dataset[(String, Publication)] = spark.read.load(s"$workingDirPath/uwPublication").as[Publication].map(p => (p.getId, p))

    def applyMerge(item:((String, Publication), (String, Publication))) : Publication =
    {
      val crossrefPub = item._1._2
      if (item._2!= null) {
        val otherPub = item._2._2
        if (otherPub != null) {
          crossrefPub.mergeFrom(otherPub)
        }
      }
      crossrefPub
    }
    crossrefPublication.joinWith(uwPublication, crossrefPublication("_1").equalTo(uwPublication("_1")), "left").map(applyMerge).write.mode(SaveMode.Overwrite).save(s"$workingDirPath/firstJoin")
    logger.info("Phase 3) Join Result with ORCID")
    val fj: Dataset[(String, Publication)] = spark.read.load(s"$workingDirPath/firstJoin").as[Publication].map(p => (p.getId, p))
    val orcidPublication: Dataset[(String, Publication)] = spark.read.load(s"$workingDirPath/orcidPublication").as[Publication].map(p => (p.getId, p))
    fj.joinWith(orcidPublication, fj("_1").equalTo(orcidPublication("_1")), "left").map(applyMerge).write.mode(SaveMode.Overwrite).save(s"$workingDirPath/secondJoin")

    logger.info("Phase 3) Join Result with MAG")
    val sj: Dataset[(String, Publication)] = spark.read.load(s"$workingDirPath/secondJoin").as[Publication].map(p => (p.getId, p))

    val magPublication: Dataset[(String, Publication)] = spark.read.load(s"$workingDirPath/magPublication").as[Publication].map(p => (p.getId, p))
    sj.joinWith(magPublication, sj("_1").equalTo(magPublication("_1")), "left").map(applyMerge).write.mode(SaveMode.Overwrite).save(s"$workingDirPath/doiBoostPublication")


    val doiBoostPublication: Dataset[Publication] = spark.read.load(s"$workingDirPath/doiBoostPublication").as[Publication]

    val map = DoiBoostMappingUtil.retrieveHostedByMap()

    doiBoostPublication.filter(p=>DoiBoostMappingUtil.filterPublication(p)).map(p => DoiBoostMappingUtil.fixPublication(p, map)).write.mode(SaveMode.Overwrite).save(s"$workingDirPath/doiBoostPublicationFiltered")


    val affiliationPath = parser.get("affiliationPath")
    val paperAffiliationPath = parser.get("paperAffiliationPath")

    val affiliation = spark.read.load(affiliationPath).where(col("GridId").isNotNull).select(col("AffiliationId"), col("GridId"))

    val paperAffiliation = spark.read.load(paperAffiliationPath).select(col("AffiliationId").alias("affId"), col("PaperId"))


    val a:Dataset[DoiBoostAffiliation] = paperAffiliation
      .joinWith(affiliation, paperAffiliation("affId").equalTo(affiliation("AffiliationId"))).select(col("_1.PaperId"), col("_2.AffiliationId"), col("_2.GridId")).as[DoiBoostAffiliation]



    val magPubs:Dataset[(String,Publication)]= spark.read.load(s"$workingDirPath/doiBoostPublicationFiltered").as[Publication]
      .map(p => (ConversionUtil.extractMagIdentifier(p.getOriginalId.asScala), p))(tupleForJoinEncoder).filter(s =>s._1!= null )


    magPubs.joinWith(a,magPubs("_1").equalTo(a("PaperId"))).flatMap(item => {
      val pub:Publication = item._1._2
      val affiliation = item._2
      val r:Relation = new Relation
      r.setSource(pub.getId)
      r.setTarget(DoiBoostMappingUtil.generateGridAffiliationId(affiliation.GridId))
      r.setRelType("resultOrganization")
      r.setRelClass("hasAuthorInstitution")
      r.setSubRelType("affiliation")
      r.setDataInfo(pub.getDataInfo)
      r.setCollectedfrom(pub.getCollectedfrom)
      val r1:Relation = new Relation
      r1.setTarget(pub.getId)
      r1.setSource(DoiBoostMappingUtil.generateGridAffiliationId(affiliation.GridId))
      r1.setRelType("resultOrganization")
      r1.setRelClass("isAuthorInstitutionOf")
      r1.setSubRelType("affiliation")
      r1.setDataInfo(pub.getDataInfo)
      r1.setCollectedfrom(pub.getCollectedfrom)
      List(r, r1)
    })(mapEncoderRel).write.mode(SaveMode.Overwrite).save(s"$workingDirPath/doiBoostPublicationAffiliation")
  }

}
