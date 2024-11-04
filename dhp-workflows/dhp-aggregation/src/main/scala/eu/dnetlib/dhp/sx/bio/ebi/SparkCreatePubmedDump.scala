package eu.dnetlib.dhp.sx.bio.ebi

import com.fasterxml.jackson.databind.ObjectMapper
import eu.dnetlib.dhp.application.{AbstractScalaApplication, ArgumentApplicationParser}
import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup
import eu.dnetlib.dhp.schema.oaf.Oaf
import eu.dnetlib.dhp.sx.bio.pubmed.{PMArticle, PMAuthor, PMJournal, PMParser, PMParser2, PubMedToOaf}
import eu.dnetlib.dhp.utils.ISLookupClientFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import java.io.ByteArrayInputStream
import javax.xml.stream.XMLInputFactory

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
    val targetPath = parser.get("targetPath")
    log.info(s"TargetPath is '$targetPath'")

    val isLookupService = ISLookupClientFactory.getLookUpService(isLookupUrl)
    val vocabularies = VocabularyGroup.loadVocsFromIS(isLookupService)

    createPubmedDump(spark, sourcePath, targetPath, vocabularies)

  }

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
      .text(targetPath)
  }
}

object SparkCreatePubmedDump {

  def main(args: Array[String]): Unit = {
    val log: Logger = LoggerFactory.getLogger(getClass)

    new SparkCreatePubmedDump("/eu/dnetlib/dhp/sx/bio/ebi/baseline_to_oaf_params.json", args, log).initialize().run()

  }
}
