package eu.dnetlib.dhp.collection.mag

import eu.dnetlib.dhp.application.AbstractScalaApplication
import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup
import eu.dnetlib.dhp.schema.oaf.Relation
import eu.dnetlib.dhp.utils.ISLookupClientFactory
import org.apache.spark.sql.{Encoder, Encoders, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}
import org.apache.spark.sql.types._
import org.slf4j.{Logger, LoggerFactory}

class SparkOpenAlexToOAF(propertyPath: String, args: Array[String], log: Logger)
  extends AbstractScalaApplication(propertyPath, args, log: Logger) {

  /** Here all the spark applications runs this method
   * where the whole logic of the spark node is defined
   */
  override def run(): Unit = {
    val openAlexBasePath: String = parser.get("openAlexBasePath")
    log.info("found parameters openAlexBasePath: {}", openAlexBasePath)
    val workingPath: String = parser.get("workingPath")
    log.info("found parameters workingPath: {}", workingPath)
    val mdstoreMagPath: String = parser.get("mdstoreMagPath")
    log.info("found parameters mdstoreMagPath: {}", mdstoreMagPath)
    createWorksUpdatedVersion(spark, openAlexBasePath, workingPath)
    log.info("createWorksUpdatedVersion finished")
      extractMagUsedIds(spark, mdstoreMagPath, workingPath)
    log.info("extractMagUsedIds finished")
    val isLookupUrl: String = parser.get("isLookupUrl")
    log.info("isLookupUrl: {}", isLookupUrl)
    val targetPath = parser.get("targetPath")
    log.info("targetPath: {}", targetPath)
    val isLookupService = ISLookupClientFactory.getLookUpService(isLookupUrl)
    val vocabularies: VocabularyGroup = VocabularyGroup.loadVocsFromIS(isLookupService)
    mapOpenAlexToOAF(spark, s"$workingPath/oAlexFiltered", targetPath, vocabularies)

  }

  private def extractMagUsedIds(spark: SparkSession, mdstoreMagPath: String, workingPath: String): Unit = {
    import spark.implicits._
    val mdStoreContent = spark.read.text(mdstoreMagPath)
    val schema =
      new StructType().add("id", StringType).add("originalId", ArrayType(elementType = StringType, containsNull = true))
    spark.read
      .schema(schema)
      .json(mdStoreContent.where("value not like '%relClass%'").as[String])
      .selectExpr("explode(originalId) as magId")
      .write
      .mode(SaveMode.Overwrite)
      .save(s"$workingPath/magIds")
    val magIds = spark.read.load(s"$workingPath/magIds")
    implicit val encordersWorks: Encoder[OAUtility.OAWorks] = Encoders.product[OAUtility.OAWorks]
    val oaWorks = spark.read.schema(encordersWorks.schema).json(s"$workingPath/openAlexWorksUpdated")

    oaWorks
      .where("ids.mag is not null")
      .join(magIds, magIds("magId") === oaWorks("ids.mag"), "leftSemi")
      .write
      .option("compression", "gzip")
      .json(s"$workingPath/oAlexFiltered")
  }

  def mapOpenAlexToOAF(
                        spark: SparkSession,
                        inputPath: String,
                        targetPath: String,
                        vocabularies: VocabularyGroup
                      ): Unit = {
    import spark.implicits._
    implicit val encordersWorks: Encoder[OAUtility.OAWorks] = Encoders.product[OAUtility.OAWorks]
    implicit val relationEncoder: Encoder[Relation] = Encoders.bean(classOf[Relation])
    val oaWorks = spark.read.schema(encordersWorks.schema).json(inputPath).as[OAUtility.OAWorks]

    oaWorks
      .map(s => OAUtility.convertWorksToResult(s, vocabularies))
      .filter(s => s.nonEmpty)
      .write
      .option("compression", "gzip")
      .mode("Overwrite")
      .text(targetPath)

    oaWorks
      .flatMap(s => OAUtility.extractRelations(s))
      .filter(s => s != null)
      .write
      .mode(SaveMode.Append)
      .option("compression", "gzip")
      .json(s"$targetPath")
  }

  private def createWorksUpdatedVersion(spark: SparkSession, basePath: String, workingPath: String): Unit = {
    implicit val encordersWorks: Encoder[OAUtility.OAWorks] = Encoders.product[OAUtility.OAWorks]
    val works = spark.read.schema(encordersWorks.schema).json(s"$basePath/works")

    val windowSpec = Window.partitionBy("id").orderBy(col("updated_date").desc)
    val retracted = spark.read
      .option("delimiter", ",")
      .option(key = "header", value = true)
      .csv(s"$basePath/merged_ids/works")
      .select("id")

    works
      .join(retracted, works("id") === retracted("id"), "left_anti")
      .withColumn("rank", row_number().over(windowSpec))
      .filter(col("rank") === 1)
      .drop("rank")
      .write
      .mode("OverWrite")
      .option("compression", "gzip")
      .json(s"$workingPath/openAlexWorksUpdated")

  }
}

object SparkOpenAlexToOAF {

  def main(args: Array[String]): Unit = {
    val log = LoggerFactory.getLogger(this.getClass)
    val app =
      new SparkOpenAlexToOAF("/eu/dnetlib/dhp/collection/mag/open_alex_to_oaf_properties.json", args, log).initialize()
    app.run()
  }
}
