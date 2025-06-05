package eu.dnetlib.dhp.sx.bio.ebi

import com.fasterxml.jackson.databind.ObjectMapper
import eu.dnetlib.dhp.application.AbstractScalaApplication
import eu.dnetlib.dhp.common.Constants
import eu.dnetlib.dhp.common.Constants.{MDSTORE_DATA_PATH, MDSTORE_SIZE_PATH}
import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup
import eu.dnetlib.dhp.schema.mdstore.MDStoreVersion
import eu.dnetlib.dhp.sx.bio.pubmed.{PMArticle, PMParser2, PubMedToOaf}
import eu.dnetlib.dhp.transformation.TransformSparkJobNode
import eu.dnetlib.dhp.utils.DHPUtils.writeHdfsFile
import eu.dnetlib.dhp.utils.ISLookupClientFactory
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

class SparkCreatePubmedDump(propertyPath: String, args: Array[String], log: Logger)
    extends AbstractScalaApplication(propertyPath, args, log: Logger) {

  /** Here all the spark applications runs this method
    * where the whole logic of the spark node is defined
    */
  override def run(): Unit = {
    val isLookupUrl: String = parser.get("isLookupUrl")
    log.info("isLookupUrl: {}", isLookupUrl)
    val sourcePath = parser.get("sourcePath")
    log.info(s"SourcePath is '$sourcePath'")
    val mdstoreOutputVersion = parser.get("mdstoreOutputVersion")
    log.info(s"mdstoreOutputVersion is '$mdstoreOutputVersion'")
    val mapper = new ObjectMapper()
    val cleanedMdStoreVersion = mapper.readValue(mdstoreOutputVersion, classOf[MDStoreVersion])
    val outputBasePath = cleanedMdStoreVersion.getHdfsPath
    log.info(s"outputBasePath is '$outputBasePath'")

    val isLookupService = ISLookupClientFactory.getLookUpService(isLookupUrl)
    val vocabularies = VocabularyGroup.loadVocsFromIS(isLookupService)

    createPubmedDump(spark, sourcePath, outputBasePath, vocabularies)

  }

  /** This method creates a dump of the pubmed articles
    * @param spark the spark session
    * @param sourcePath the path of the source file
    * @param targetPath the path of the target file
    * @param vocabularies the vocabularies
    */
  def createPubmedDump(
    spark: SparkSession,
    sourcePath: String,
    targetPath: String,
    vocabularies: VocabularyGroup
  ): Unit = {
    require(spark != null)

    implicit val PMEncoder: Encoder[PMArticle] = Encoders.bean(classOf[PMArticle])

    import spark.implicits._
    val df = spark.read.option("lineSep", "</PubmedArticle>").text(sourcePath)
    val mapper = new ObjectMapper()
    df.as[String]
      .map(s => {
        val id = s.indexOf("<PubmedArticle>")
        if (id >= 0) s"${s.substring(id)}</PubmedArticle>" else null
      })
      .filter(s => s != null)
      .map { i =>
        //remove try catch
        try {
          new PMParser2().parse(i)
        } catch {
          case _: Exception => {
            throw new RuntimeException(s"Error parsing article: $i")
          }
        }
      }
      .dropDuplicates("pmid")
      .map { a =>
        val oaf = PubMedToOaf.convert(a, vocabularies)
        if (oaf != null)
          mapper.writeValueAsString(oaf)
        else
          null
      }
      .as[String]
      .filter(s => s != null)
      .write
      .option("compression", "gzip")
      .mode("overwrite")
      .text(targetPath + MDSTORE_DATA_PATH)

    val mdStoreSize = spark.read.text(targetPath + MDSTORE_DATA_PATH).count
    writeHdfsFile(spark.sparkContext.hadoopConfiguration, "" + mdStoreSize, targetPath + MDSTORE_SIZE_PATH)
  }
}

object SparkCreatePubmedDump {

  def main(args: Array[String]): Unit = {
    val log: Logger = LoggerFactory.getLogger(getClass)

    new SparkCreatePubmedDump("/eu/dnetlib/dhp/sx/bio/ebi/baseline_to_oaf_params.json", args, log).initialize().run()

  }
}
