package eu.dnetlib.dhp.collection.mag

import com.fasterxml.jackson.databind.ObjectMapper
import eu.dnetlib.dhp.application.AbstractScalaApplication
import eu.dnetlib.dhp.common.Constants.MDSTORE_DATA_PATH
import eu.dnetlib.dhp.schema.mdstore.MDStoreVersion
import eu.dnetlib.dhp.schema.oaf.Relation
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.{Encoder, Encoders, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

class SparkMAGtoOAF(propertyPath: String, args: Array[String], log: Logger)
    extends AbstractScalaApplication(propertyPath, args, log: Logger) {

  /** Here all the spark applications runs this method
    * where the whole logic of the spark node is defined
    */
  override def run(): Unit = {
    val mdstoreOutputVersion = parser.get("mdstoreOutputVersion")
    log.info(s"mdstoreOutputVersion is '$mdstoreOutputVersion'")

    val mapper = new ObjectMapper()
    val cleanedMdStoreVersion = mapper.readValue(mdstoreOutputVersion, classOf[MDStoreVersion])
    val outputBasePath = cleanedMdStoreVersion.getHdfsPath
    log.info(s"outputBasePath is '$outputBasePath'")
    val mdstorePath = s"$outputBasePath$MDSTORE_DATA_PATH"
    val magBasePath: String = parser.get("magBasePath")
    log.info("found parameters magBasePath: {}", magBasePath)
    convertMAG(spark, magBasePath, mdstorePath)
    generateAffiliations(spark, magBasePath, mdstorePath)
    reportTotalSize(mdstorePath, outputBasePath)
  }

  def convertMAG(spark: SparkSession, magBasePath: String, mdStorePath: String): Unit = {
    import spark.implicits._

    spark.read
      .load(s"$magBasePath/mag_denormalized")
      .as[MAGPaper]
      .map(s => MagUtility.convertMAGtoOAF(s))
      .filter(s => s != null)
      .write
      .option("compression", "gzip")
      .mode(SaveMode.Overwrite)
      .text(mdStorePath)

  }

  def generateAffiliations(spark: SparkSession, magBasePath: String, mdStorePath: String): Unit = {

    implicit val relEncoder: Encoder[Relation] = Encoders.bean(classOf[Relation])
    val schema = new StructType()
      .add(StructField("id", StringType))
      .add(StructField("originalId", ArrayType(StringType)))
    val generatedMag =
      spark.read.schema(schema).json(mdStorePath).selectExpr("explode(originalId) as PaperId").distinct()
    val paperAuthorAffiliations = MagUtility
      .loadMagEntity(spark, "PaperAuthorAffiliations", magBasePath)
      .where(col("AffiliationId").isNotNull)
      .select("PaperId", "AffiliationId")
      .distinct
    paperAuthorAffiliations
      .join(generatedMag, paperAuthorAffiliations("PaperId") === generatedMag("PaperId"), "leftsemi")
      .flatMap(r => MagUtility.generateAffiliationRelations(r))
      .write
      .option("compression", "gzip")
      .mode(SaveMode.Append)
      .json(mdStorePath)

  }
}

object SparkMAGtoOAF {

  val log: Logger = LoggerFactory.getLogger(SparkMAGtoOAF.getClass)

  def main(args: Array[String]): Unit = {
    new SparkMAGtoOAF("/eu/dnetlib/dhp/collection/mag/convert_MAG_to_OAF_properties.json", args, log)
      .initialize()
      .run()
  }
}
