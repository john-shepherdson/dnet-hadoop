package eu.dnetlib.dhp.collection.crossref

import eu.dnetlib.dhp.application.AbstractScalaApplication
import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup
import eu.dnetlib.dhp.schema.oaf.{Oaf, Publication, Dataset => OafDataset}
import eu.dnetlib.dhp.utils.ISLookupClientFactory
import org.apache.spark.sql._
import org.slf4j.{Logger, LoggerFactory}

class SparkMapDumpIntoOAF(propertyPath: String, args: Array[String], log: Logger)
    extends AbstractScalaApplication(propertyPath, args, log: Logger) {

  /** Here all the spark applications runs this method
    * where the whole logic of the spark node is defined
    */
  override def run(): Unit = {
    val sourcePath = parser.get("sourcePath")
    log.info("sourcePath: {}", sourcePath)
    val targetPath = parser.get("targetPath")
    log.info("targetPath: {}", targetPath)
    val isLookupUrl: String = parser.get("isLookupUrl")
    log.info("isLookupUrl: {}", isLookupUrl)
    val isLookupService = ISLookupClientFactory.getLookUpService(isLookupUrl)
    val vocabularies = VocabularyGroup.loadVocsFromIS(isLookupService)
    require(vocabularies != null)
    transformCrossref(spark, sourcePath, targetPath, vocabularies)

  }

  def transformCrossref(
    spark: SparkSession,
    sourcePath: String,
    targetPath: String,
    vocabularies: VocabularyGroup
  ): Unit = {
    import spark.implicits._
    val dump = spark.read.text(sourcePath).as[String]
    dump
      .flatMap(s => Crossref2Oaf.convert(s, vocabularies))
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("oafType")
      .option("compression", "gzip")
      .text(targetPath)

  }
}

object SparkMapDumpIntoOAF {

  def main(args: Array[String]): Unit = {
    val logger: Logger = LoggerFactory.getLogger(SparkMapDumpIntoOAF.getClass)

    new SparkMapDumpIntoOAF(
      log = logger,
      args = args,
      propertyPath = "/eu/dnetlib/dhp/collection/crossref/convert_crossref_dump_to_oaf_params.json"
    ).initialize().run()
  }

}
