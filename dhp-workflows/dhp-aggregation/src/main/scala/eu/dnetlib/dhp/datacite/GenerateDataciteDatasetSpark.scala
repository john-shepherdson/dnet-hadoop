package eu.dnetlib.dhp.datacite

import com.fasterxml.jackson.databind.ObjectMapper
import eu.dnetlib.dhp.application.AbstractScalaApplication
import eu.dnetlib.dhp.collection.CollectionUtils
import eu.dnetlib.dhp.common.Constants.{MDSTORE_DATA_PATH, MDSTORE_SIZE_PATH}
import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup
import eu.dnetlib.dhp.schema.mdstore.{MDStoreVersion, MetadataRecord}
import eu.dnetlib.dhp.schema.oaf.Oaf
import eu.dnetlib.dhp.utils.DHPUtils.writeHdfsFile
import eu.dnetlib.dhp.utils.ISLookupClientFactory
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

class GenerateDataciteDatasetSpark(propertyPath: String, args: Array[String], log: Logger)
    extends AbstractScalaApplication(propertyPath, args, log: Logger) {

  /** Here all the spark applications runs this method
    * where the whole logic of the spark node is defined
    */
  override def run(): Unit = {

    val sourcePath = parser.get("sourcePath")
    log.info(s"SourcePath is '$sourcePath'")
    val exportLinks = "true".equalsIgnoreCase(parser.get("exportLinks"))
    log.info(s"exportLinks is '$exportLinks'")
    val isLookupUrl: String = parser.get("isLookupUrl")
    log.info("isLookupUrl: {}", isLookupUrl)

    val isLookupService = ISLookupClientFactory.getLookUpService(isLookupUrl)
    val vocabularies = VocabularyGroup.loadVocsFromIS(isLookupService)
    require(vocabularies != null)

    val mdstoreOutputVersion = parser.get("mdstoreOutputVersion")
    log.info(s"mdstoreOutputVersion is '$mdstoreOutputVersion'")

    val mapper = new ObjectMapper()
    val cleanedMdStoreVersion = mapper.readValue(mdstoreOutputVersion, classOf[MDStoreVersion])
    val outputBasePath = cleanedMdStoreVersion.getHdfsPath
    log.info(s"outputBasePath is '$outputBasePath'")
    val targetPath = s"$outputBasePath$MDSTORE_DATA_PATH"
    log.info(s"targetPath is '$targetPath'")

    generateDataciteDataset(sourcePath, exportLinks, vocabularies, targetPath, spark)

    reportTotalSize(targetPath, outputBasePath)
  }

  /** For working with MDStore we need to store in a file on hdfs the size of
    * the current dataset
    * @param targetPath
    * @param outputBasePath
    */
  def reportTotalSize(targetPath: String, outputBasePath: String): Unit = {
    val total_items = spark.read.text(targetPath).count()
    writeHdfsFile(
      spark.sparkContext.hadoopConfiguration,
      s"$total_items",
      outputBasePath + MDSTORE_SIZE_PATH
    )
  }

  /** Generate the transformed and cleaned OAF Dataset from the native one
    *
    * @param sourcePath  sourcePath of the native Dataset in format JSON/Datacite
    * @param exportLinks If true it generates unresolved links
    * @param vocabularies vocabularies for cleaning
    * @param targetPath the targetPath of the result Dataset
    */
  def generateDataciteDataset(
    sourcePath: String,
    exportLinks: Boolean,
    vocabularies: VocabularyGroup,
    targetPath: String,
    spark: SparkSession
  ): Unit = {
    require(spark != null)
    import spark.implicits._

    implicit val mrEncoder: Encoder[MetadataRecord] = Encoders.kryo[MetadataRecord]

    implicit val resEncoder: Encoder[Oaf] = Encoders.kryo[Oaf]
    CollectionUtils.saveDataset(
      spark.read
        .load(sourcePath)
        .as[DataciteType]
        .filter(d => d.isActive)
        .flatMap(d =>
          DataciteToOAFTransformation
            .generateOAF(d.json, d.timestamp, d.timestamp, vocabularies, exportLinks)
        )
        .filter(d => d != null),
      targetPath
    )
  }

}

object GenerateDataciteDatasetSpark {

  val log: Logger = LoggerFactory.getLogger(GenerateDataciteDatasetSpark.getClass)

  def main(args: Array[String]): Unit = {
    new GenerateDataciteDatasetSpark(
      "/eu/dnetlib/dhp/datacite/generate_dataset_params.json",
      args,
      log
    ).initialize().run()
  }
}
