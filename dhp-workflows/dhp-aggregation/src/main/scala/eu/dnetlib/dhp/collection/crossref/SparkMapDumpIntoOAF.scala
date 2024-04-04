package eu.dnetlib.dhp.collection.crossref

import com.fasterxml.jackson.databind.ObjectMapper
import eu.dnetlib.dhp.application.AbstractScalaApplication
import eu.dnetlib.dhp.collection.crossref.Crossref2Oaf.{TransformationType, mergeUnpayWall}
import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup
import eu.dnetlib.dhp.schema.oaf.{Oaf, Publication, Relation, Result, Dataset => OafDataset}
import eu.dnetlib.dhp.utils.ISLookupClientFactory
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, explode, lower}
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

    val mapper = new ObjectMapper()

    implicit val oafEncoder: Encoder[Oaf] = Encoders.kryo(classOf[Oaf])
    implicit val resultEncoder: Encoder[Result] = Encoders.kryo(classOf[Result])

    val dump: Dataset[String] = spark.read.text(sourcePath).as[String]
    dump
      .flatMap(s => Crossref2Oaf.convert(s, vocabularies, TransformationType.OnlyRelation))
      .as[Oaf]
      .map(r => mapper.writeValueAsString(r))
      .write
      .mode(SaveMode.Overwrite)
      .option("compression", "gzip")
      .text(targetPath)
    val uw = transformUnpayWall(spark, unpaywallPath, sourcePath)
    val resultCrossref: Dataset[(String, Result)] = dump
      .flatMap(s => Crossref2Oaf.convert(s, vocabularies, TransformationType.OnlyResult))
      .as[Oaf]
      .map(r => r.asInstanceOf[Result])
      .map(r => (r.getPid.get(0).getValue, r))(Encoders.tuple(Encoders.STRING, resultEncoder))
    resultCrossref
      .joinWith(uw, resultCrossref("_1").equalTo(uw("doi")), "left")
      .map(k => {
        mergeUnpayWall(k._1._2, k._2)
      })
      .map(r => mapper.writeValueAsString(r))
      .as[Result]
      .write
      .mode(SaveMode.Append)
      .option("compression", "gzip")
      .text(s"$targetPath")

    // Generate affiliation relations:
    spark.read
      .json(sourcePath)
      .select(col("DOI"), explode(col("author.affiliation")).alias("affiliations"))
      .select(col("DOI"), explode(col("affiliations.id")).alias("aids"))
      .where("aids is not null")
      .select(col("DOI"), explode(col("aids")).alias("aff"))
      .select(col("DOI"), col("aff.id").alias("id"), col("aff.id-type").alias("idType"))
      .where(col("idType").like("ROR"))
      .flatMap(r => Crossref2Oaf.generateAffliation(r))
      .write
      .mode(SaveMode.Append)
      .option("compression", "gzip")
      .text(s"$targetPath")

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
