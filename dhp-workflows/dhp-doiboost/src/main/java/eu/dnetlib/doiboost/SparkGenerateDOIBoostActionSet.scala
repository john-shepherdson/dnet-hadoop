package eu.dnetlib.doiboost

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.schema.action.AtomicAction
import eu.dnetlib.dhp.schema.oaf.{Publication, Relation, Dataset => OafDataset}
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{ Encoder, Encoders, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

object SparkGenerateDOIBoostActionSet {
  val logger: Logger = LoggerFactory.getLogger(getClass)
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf()
    val parser = new ArgumentApplicationParser(IOUtils.toString(getClass.getResourceAsStream("/eu/dnetlib/dhp/doiboost/generate_doiboost_as_params.json")))
    parser.parseArgument(args)
    val spark: SparkSession =
      SparkSession
        .builder()
        .config(conf)
        .appName(getClass.getSimpleName)
        .master(parser.get("master")).getOrCreate()

    implicit val mapEncoderPub: Encoder[Publication] = Encoders.kryo[Publication]
    implicit val mapEncoderDataset: Encoder[OafDataset] = Encoders.kryo[OafDataset]
    implicit val mapEncoderRel: Encoder[Relation] = Encoders.kryo[Relation]

    implicit val mapEncoderAtomiAction: Encoder[AtomicAction[OafDataset]] = Encoders.kryo[AtomicAction[OafDataset]]

    val dbPublicationPath           = parser.get("dbPublicationPath")
    val dbDatasetPath               = parser.get("dbDatasetPath")
    val crossRefRelation            = parser.get("crossRefRelation")
    val dbaffiliationRelationPath   = parser.get("dbaffiliationRelationPath")
    val workingDirPath              = parser.get("targetPath")

    spark.read.load(dbDatasetPath).as[OafDataset]
      .map(d =>DoiBoostMappingUtil.fixResult(d))
      .map(d=>DoiBoostMappingUtil.toActionSet(d))(Encoders.tuple(Encoders.STRING, Encoders.STRING))
      .write.mode(SaveMode.Append).save(s"$workingDirPath/actionSet")

    spark.read.load(dbPublicationPath).as[Publication]
      .map(d=>DoiBoostMappingUtil.toActionSet(d))(Encoders.tuple(Encoders.STRING, Encoders.STRING))
      .write.mode(SaveMode.Append).save(s"$workingDirPath/actionSet")

    spark.read.load(crossRefRelation).as[Relation]
      .map(d=>DoiBoostMappingUtil.toActionSet(d))(Encoders.tuple(Encoders.STRING, Encoders.STRING))
      .write.mode(SaveMode.Append).save(s"$workingDirPath/actionSet")

    spark.read.load(dbaffiliationRelationPath).as[Relation]
      .map(d=>DoiBoostMappingUtil.toActionSet(d))(Encoders.tuple(Encoders.STRING, Encoders.STRING))
      .write.mode(SaveMode.Append).save(s"$workingDirPath/actionSet")


//    implicit val mapEncoderPub: Encoder[(String, String)] = Encoders.tuple(Encoders.STRING, Encoders.STRING)
//
//    val d: Dataset[(String, String)] =spark.read.load(s"$workingDirPath/actionSet").as[(String,String)]
//
//
//
//    d.rdd.map(s => (new Text(s._1), new Text(s._2))).saveAsHadoopFile(s"$workingDirPath/rawset_a8c2f90b-a3ae-4d6e-8187-47a437156e18_1590223414", classOf[Text], classOf[Text], classOf[TextOutputFormat[Text,Text]], classOf[GzipCodec])











  }

}
