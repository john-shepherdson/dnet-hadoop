package eu.dnetlib.doiboost

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.schema.oaf.{Publication, Dataset => OafDataset}
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

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


    val crossrefPublicationPath = parser.get("crossrefPublicationPath")
    val crossrefDatasetPath = parser.get("crossrefDatasetPath")
    val uwPublicationPath = parser.get("uwPublicationPath")
    val magPublicationPath = parser.get("magPublicationPath")
    val orcidPublicationPath = parser.get("orcidPublicationPath")
    val workingDirPath = parser.get("workingDirPath")


    logger.info("Phase 1) repartition and move all the dataset in a same working folder")
//    spark.read.load(crossrefPublicationPath).as(Encoders.bean(classOf[Publication])).map(s => s)(Encoders.kryo[Publication]).write.mode(SaveMode.Overwrite).save(s"$workingDirPath/crossrefPublication")
//    spark.read.load(crossrefDatasetPath).as(Encoders.bean(classOf[OafDataset])).map(s => s)(Encoders.kryo[OafDataset]).write.mode(SaveMode.Overwrite).save(s"$workingDirPath/crossrefDataset")
//    spark.read.load(uwPublicationPath).as(Encoders.bean(classOf[Publication])).map(s => s)(Encoders.kryo[Publication]).write.mode(SaveMode.Overwrite).save(s"$workingDirPath/uwPublication")
//    spark.read.load(orcidPublicationPath).as(Encoders.bean(classOf[Publication])).map(s => s)(Encoders.kryo[Publication]).write.mode(SaveMode.Overwrite).save(s"$workingDirPath/orcidPublication")
//    spark.read.load(magPublicationPath).as(Encoders.bean(classOf[Publication])).map(s => s)(Encoders.kryo[Publication]).write.mode(SaveMode.Overwrite).save(s"$workingDirPath/magPublication")

    implicit val mapEncoderPub: Encoder[Publication] = Encoders.kryo[Publication]
    implicit val mapEncoderDataset: Encoder[OafDataset] = Encoders.kryo[OafDataset]
    implicit val tupleForJoinEncoder: Encoder[(String, Publication)] = Encoders.tuple(Encoders.STRING, mapEncoderPub)

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

    doiBoostPublication.filter(p=>DoiBoostMappingUtil.filterPublication(p)).write.mode(SaveMode.Overwrite).save(s"$workingDirPath/doiBoostPublicationFiltered")




  }

}
