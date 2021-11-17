package eu.dnetlib.dhp.datacite

import com.fasterxml.jackson.databind.ObjectMapper
import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.collection.CollectionUtils.fixRelations
import eu.dnetlib.dhp.common.Constants.{MDSTORE_DATA_PATH, MDSTORE_SIZE_PATH}
import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup
import eu.dnetlib.dhp.schema.mdstore.{MDStoreVersion, MetadataRecord}
import eu.dnetlib.dhp.schema.oaf.Oaf
import eu.dnetlib.dhp.utils.DHPUtils.writeHdfsFile
import eu.dnetlib.dhp.utils.ISLookupClientFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.io.Source

object GenerateDataciteDatasetSpark {

  val log: Logger = LoggerFactory.getLogger(GenerateDataciteDatasetSpark.getClass)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf
    val parser = new ArgumentApplicationParser(Source.fromInputStream(getClass.getResourceAsStream("/eu/dnetlib/dhp/datacite/generate_dataset_params.json")).mkString)
    parser.parseArgument(args)
    val master = parser.get("master")
    val sourcePath = parser.get("sourcePath")
    val exportLinks = "true".equalsIgnoreCase(parser.get("exportLinks"))
    val isLookupUrl: String = parser.get("isLookupUrl")
    log.info("isLookupUrl: {}", isLookupUrl)

    val isLookupService = ISLookupClientFactory.getLookUpService(isLookupUrl)
    val vocabularies = VocabularyGroup.loadVocsFromIS(isLookupService)
    val spark: SparkSession = SparkSession.builder().config(conf)
      .appName(GenerateDataciteDatasetSpark.getClass.getSimpleName)
      .master(master)
      .getOrCreate()

    import spark.implicits._

    implicit val mrEncoder: Encoder[MetadataRecord] = Encoders.kryo[MetadataRecord]

    implicit val resEncoder: Encoder[Oaf] = Encoders.kryo[Oaf]

    val mdstoreOutputVersion = parser.get("mdstoreOutputVersion")
    val mapper = new ObjectMapper()
    val cleanedMdStoreVersion = mapper.readValue(mdstoreOutputVersion, classOf[MDStoreVersion])
    val outputBasePath = cleanedMdStoreVersion.getHdfsPath

    log.info("outputBasePath: {}", outputBasePath)
    val targetPath = s"$outputBasePath/$MDSTORE_DATA_PATH"

    spark.read.load(sourcePath).as[DataciteType]
      .filter(d => d.isActive)
      .flatMap(d => DataciteToOAFTransformation.generateOAF(d.json, d.timestamp, d.timestamp, vocabularies, exportLinks))
      .filter(d => d != null)
      .flatMap(i => fixRelations(i)).filter(i => i != null)
      .write.mode(SaveMode.Overwrite).save(targetPath)

    val total_items = spark.read.load(targetPath).as[Oaf].count()
    writeHdfsFile(spark.sparkContext.hadoopConfiguration, s"$total_items", outputBasePath + MDSTORE_SIZE_PATH)
  }
}
