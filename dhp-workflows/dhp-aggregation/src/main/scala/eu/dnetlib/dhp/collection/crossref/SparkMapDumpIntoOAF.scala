package eu.dnetlib.dhp.collection.crossref

import eu.dnetlib.dhp.application.AbstractScalaApplication
import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup
import eu.dnetlib.dhp.schema.oaf.{Oaf, Publication, Dataset => OafDataset}
import eu.dnetlib.dhp.utils.ISLookupClientFactory
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, lower}
import org.apache.spark.sql.types._
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
    val unpaywallPath = parser.get("unpaywallPath")
    log.info("unpaywallPath: {}", unpaywallPath)
    val isLookupUrl: String = parser.get("isLookupUrl")
    log.info("isLookupUrl: {}", isLookupUrl)
    val isLookupService = ISLookupClientFactory.getLookUpService(isLookupUrl)
    val vocabularies = VocabularyGroup.loadVocsFromIS(isLookupService)
    require(vocabularies != null)
    transformCrossref(spark, sourcePath, targetPath, unpaywallPath, vocabularies)

  }

  def transformUnpayWall(spark: SparkSession, unpaywallPath: String, crossrefPath: String): Dataset[UnpayWall] = {
    val schema = new StructType()
      .add(StructField("doi", StringType))
      .add(StructField("is_oa", BooleanType))
      .add(
        StructField(
          "best_oa_location",
          new StructType()
            .add("host_type", StringType)
            .add("license", StringType)
            .add("url", StringType)
        )
      )
      .add("oa_status", StringType)

    import spark.implicits._
    val cId = spark.read
      .schema(new StructType().add("DOI", StringType))
      .json(crossrefPath)
      .withColumn("doi", lower(col("DOI")))

    val uw = spark.read
      .schema(schema)
      .json(unpaywallPath)
      .withColumn("doi", lower(col("doi")))
      .where("is_oa = true and best_oa_location.url is not null")

    uw.join(cId, uw("doi") === cId("doi"), "leftsemi").as[UnpayWall].cache()

  }

  def transformCrossref(
    spark: SparkSession,
    sourcePath: String,
    targetPath: String,
    unpaywallPath: String,
    vocabularies: VocabularyGroup
  ): Unit = {
    import spark.implicits._
    val dump: Dataset[String] = spark.read.text(sourcePath).as[String]

    val uw = transformUnpayWall(spark, unpaywallPath, sourcePath)

    val crId = dump.map(s => Crossref2Oaf.extract_doi(s))

    crId
      .joinWith(uw, crId("doi") === uw("doi"), "left")
      .flatMap(s => Crossref2Oaf.convert(s._1, s._2, vocabularies))
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
