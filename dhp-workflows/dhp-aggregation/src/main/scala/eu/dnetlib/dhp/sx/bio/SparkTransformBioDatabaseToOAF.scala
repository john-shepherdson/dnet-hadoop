package eu.dnetlib.dhp.sx.bio

import com.fasterxml.jackson.databind.ObjectMapper
import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.collection.CollectionUtils
import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup
import eu.dnetlib.dhp.schema.mdstore.MDStoreVersion
import eu.dnetlib.dhp.schema.oaf.Oaf
import eu.dnetlib.dhp.sx.bio.BioDBToOAF.ScholixResolved
import eu.dnetlib.dhp.utils.DHPUtils.writeHdfsFile
import eu.dnetlib.dhp.utils.ISLookupClientFactory
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import eu.dnetlib.dhp.common.Constants.{MDSTORE_DATA_PATH, MDSTORE_SIZE_PATH}

object SparkTransformBioDatabaseToOAF {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    val log: Logger = LoggerFactory.getLogger(getClass)
    val parser = new ArgumentApplicationParser(
      IOUtils.toString(
        getClass.getResourceAsStream("/eu/dnetlib/dhp/sx/bio/ebi/bio_to_oaf_params.json")
      )
    )
    parser.parseArgument(args)
    val database: String = parser.get("database")
    log.info("database: {}", database)

    val dbPath: String = parser.get("dbPath")
    log.info("dbPath: {}", database)
    val mdstoreOutputVersion = parser.get("mdstoreOutputVersion")
    log.info(s"mdstoreOutputVersion is '$mdstoreOutputVersion'")
    val isLookupUrl: String = parser.get("isLookupUrl")
    log.info("isLookupUrl: {}", isLookupUrl)
    val mapper = new ObjectMapper()
    val cleanedMdStoreVersion = mapper.readValue(mdstoreOutputVersion, classOf[MDStoreVersion])
    val outputBasePath = cleanedMdStoreVersion.getHdfsPath
    log.info(s"outputBasePath is '$outputBasePath'")
    val isLookupService = ISLookupClientFactory.getLookUpService(isLookupUrl)
    val vocabularies = VocabularyGroup.loadVocsFromIS(isLookupService)
    require(vocabularies != null)

    val spark: SparkSession =
      SparkSession
        .builder()
        .config(conf)
        .appName(getClass.getSimpleName)
        .master(parser.get("master"))
        .getOrCreate()
    val sc = spark.sparkContext

    implicit val resultEncoder: Encoder[Oaf] = Encoders.kryo(classOf[Oaf])
    import spark.implicits._
    database.toUpperCase() match {
      case "UNIPROT" =>
        CollectionUtils.saveDataset(
          spark.createDataset(sc.textFile(dbPath).flatMap(i => BioDBToOAF.uniprotToOAF(i, vocabularies))),
          outputBasePath + MDSTORE_DATA_PATH
        )
        val mdStoreSize = spark.read.text(outputBasePath + MDSTORE_DATA_PATH).count
        writeHdfsFile(spark.sparkContext.hadoopConfiguration, "" + mdStoreSize, outputBasePath + MDSTORE_SIZE_PATH)

      case "PDB" =>
        CollectionUtils.saveDataset(
          spark.createDataset(sc.textFile(dbPath).flatMap(i => BioDBToOAF.pdbTOOaf(i, vocabularies))),
          outputBasePath + MDSTORE_DATA_PATH
        )
        val mdStoreSize = spark.read.text(outputBasePath + MDSTORE_DATA_PATH).count
        writeHdfsFile(spark.sparkContext.hadoopConfiguration, "" + mdStoreSize, outputBasePath + MDSTORE_SIZE_PATH)
    }
  }

}
