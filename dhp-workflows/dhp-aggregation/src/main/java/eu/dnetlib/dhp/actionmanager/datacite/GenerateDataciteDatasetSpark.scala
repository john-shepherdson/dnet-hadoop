package eu.dnetlib.dhp.actionmanager.datacite

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup
import eu.dnetlib.dhp.schema.mdstore.MetadataRecord
import eu.dnetlib.dhp.schema.oaf.Oaf
import eu.dnetlib.dhp.utils.ISLookupClientFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.io.Source

object GenerateDataciteDatasetSpark {

  val log: Logger = LoggerFactory.getLogger(GenerateDataciteDatasetSpark.getClass)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf
    val parser = new ArgumentApplicationParser(Source.fromInputStream(getClass.getResourceAsStream("/eu/dnetlib/dhp/actionmanager/datacite/generate_dataset_params.json")).mkString)
    parser.parseArgument(args)
    val master = parser.get("master")
    val sourcePath = parser.get("sourcePath")
    val targetPath = parser.get("targetPath")
    val exportLinks = "true".equalsIgnoreCase(parser.get("exportLinks"))
    val isLookupUrl: String = parser.get("isLookupUrl")
    log.info("isLookupUrl: {}", isLookupUrl)

    val isLookupService = ISLookupClientFactory.getLookUpService(isLookupUrl)
    val vocabularies = VocabularyGroup.loadVocsFromIS(isLookupService)
    val spark: SparkSession = SparkSession.builder().config(conf)
      .appName(GenerateDataciteDatasetSpark.getClass.getSimpleName)
      .master(master)
      .getOrCreate()

    implicit val mrEncoder: Encoder[MetadataRecord] = Encoders.kryo[MetadataRecord]

    implicit val resEncoder: Encoder[Oaf] = Encoders.kryo[Oaf]

    import spark.implicits._

    spark.read.load(sourcePath).as[DataciteType]
      .filter(d => d.isActive)
      .flatMap(d => DataciteToOAFTransformation.generateOAF(d.json, d.timestamp, d.timestamp, vocabularies, exportLinks))
      .filter(d => d != null)
      .write.mode(SaveMode.Overwrite).save(targetPath)
  }
}