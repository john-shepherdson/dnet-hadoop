package eu.dnetlib.dhp.actionmanager.scholix

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.schema.oaf.{Oaf, Relation, Result}
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.slf4j.{Logger, LoggerFactory}

import scala.io.Source

object SparkCreateActionset {

  def main(args: Array[String]): Unit = {
    val log: Logger = LoggerFactory.getLogger(getClass)
    val conf: SparkConf = new SparkConf()
    val parser = new ArgumentApplicationParser(Source.fromInputStream(getClass.getResourceAsStream("/eu/dnetlib/dhp/sx/actionset/generate_actionset.json")).mkString)
    parser.parseArgument(args)


    val spark: SparkSession =
      SparkSession
        .builder()
        .config(conf)
        .appName(getClass.getSimpleName)
        .master(parser.get("master")).getOrCreate()


    val sourcePath = parser.get("sourcePath")
    log.info(s"sourcePath  -> $sourcePath")

    val targetPath = parser.get("targetPath")
    log.info(s"targetPath  -> $targetPath")

    val workingDirFolder = parser.get("workingDirFolder")
    log.info(s"workingDirFolder  -> $workingDirFolder")

    implicit val oafEncoders: Encoder[Oaf] = Encoders.kryo[Oaf]
    implicit val resultEncoders: Encoder[Result] = Encoders.kryo[Result]
    implicit val relationEncoders: Encoder[Relation] = Encoders.kryo[Relation]

    import spark.implicits._

    val relation = spark.read.load(s"$sourcePath/relation").as[Relation]

    relation.filter(r => (r.getDataInfo == null || r.getDataInfo.getDeletedbyinference == false) && !r.getRelClass.toLowerCase.contains("merge"))
      .flatMap(r => List(r.getSource, r.getTarget)).distinct().write.mode(SaveMode.Overwrite).save(s"$workingDirFolder/id_relation")


    val idRelation = spark.read.load(s"$workingDirFolder/id_relation").as[String]

    log.info("extract source and target Identifier involved in relations")


    log.info("save relation filtered")

    relation.filter(r => (r.getDataInfo == null || r.getDataInfo.getDeletedbyinference == false) && !r.getRelClass.toLowerCase.contains("merge"))
      .write.mode(SaveMode.Overwrite).save(s"$workingDirFolder/actionSetOaf")

    log.info("saving entities")

    val entities: Dataset[(String, Result)] = spark.read.load(s"$sourcePath/entities/*").as[Result].map(p => (p.getId, p))(Encoders.tuple(Encoders.STRING, resultEncoders))

    entities
      .joinWith(idRelation, entities("_1").equalTo(idRelation("value")))
      .map(p => p._1._2)
      .write.mode(SaveMode.Append).save(s"$workingDirFolder/actionSetOaf")
  }

}
