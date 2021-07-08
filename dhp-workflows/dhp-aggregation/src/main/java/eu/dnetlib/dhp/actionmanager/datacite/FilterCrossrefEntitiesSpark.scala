package eu.dnetlib.dhp.actionmanager.datacite

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup
import eu.dnetlib.dhp.schema.mdstore.MetadataRecord
import eu.dnetlib.dhp.schema.oaf.{Oaf, Result}
import eu.dnetlib.dhp.utils.ISLookupClientFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.io.Source

object FilterCrossrefEntitiesSpark {

  val log: Logger = LoggerFactory.getLogger(getClass.getClass)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf
    val parser = new ArgumentApplicationParser(Source.fromInputStream(getClass.getResourceAsStream("/eu/dnetlib/dhp/actionmanager/datacite/filter_crossref_param.json")).mkString)
    parser.parseArgument(args)
    val master = parser.get("master")
    val sourcePath = parser.get("sourcePath")
    log.info("sourcePath: {}", sourcePath)
    val targetPath = parser.get("targetPath")
    log.info("targetPath: {}", targetPath)



    val spark: SparkSession = SparkSession.builder().config(conf)
      .appName(getClass.getSimpleName)
      .master(master)
      .getOrCreate()



    implicit val oafEncoder: Encoder[Oaf] = Encoders.kryo[Oaf]
    implicit val resEncoder: Encoder[Result] = Encoders.kryo[Result]

    val d:Dataset[Oaf]= spark.read.load(sourcePath).as[Oaf]

    d.filter(r => r.isInstanceOf[Result]).map(r => r.asInstanceOf[Result]).write.mode(SaveMode.Overwrite).save(targetPath)

  }

}
