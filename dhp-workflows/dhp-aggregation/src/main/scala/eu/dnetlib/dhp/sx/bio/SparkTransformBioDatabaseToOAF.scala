package eu.dnetlib.dhp.sx.bio

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.collection.CollectionUtils
import eu.dnetlib.dhp.common.Constants.{MDSTORE_DATA_PATH,MDSTORE_SIZE_PATH}
import eu.dnetlib.dhp.schema.mdstore.MDStoreVersion
import eu.dnetlib.dhp.schema.oaf.Oaf
import eu.dnetlib.dhp.sx.bio.BioDBToOAF.ScholixResolved
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import eu.dnetlib.dhp.utils.DHPUtils.{MAPPER, writeHdfsFile}
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
    log.info("mdstoreOutputVersion: {}", mdstoreOutputVersion)

    val cleanedMdStoreVersion = MAPPER.readValue(mdstoreOutputVersion, classOf[MDStoreVersion])
    val outputBasePath = cleanedMdStoreVersion.getHdfsPath
    log.info("outputBasePath: {}", outputBasePath)

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
          spark.createDataset(sc.textFile(dbPath).flatMap(i => BioDBToOAF.uniprotToOAF(i))),
          s"$outputBasePath/$MDSTORE_DATA_PATH"
        )
      case "PDB" =>
        CollectionUtils.saveDataset(
          spark.createDataset(sc.textFile(dbPath).flatMap(i => BioDBToOAF.pdbTOOaf(i))),
          s"$outputBasePath/$MDSTORE_DATA_PATH"
        )
      case "SCHOLIX" =>
        CollectionUtils.saveDataset(
          spark.read.load(dbPath).as[ScholixResolved].map(i => BioDBToOAF.scholixResolvedToOAF(i)),
          s"$outputBasePath/$MDSTORE_DATA_PATH"
        )
      case "CROSSREF_LINKS" =>
        CollectionUtils.saveDataset(
          spark.createDataset(sc.textFile(dbPath).map(i => BioDBToOAF.crossrefLinksToOaf(i))),
          s"$outputBasePath/$MDSTORE_DATA_PATH"
        )
    }

    val df = spark.read.text(s"$outputBasePath/$MDSTORE_DATA_PATH")
    val mdStoreSize = df.count
    writeHdfsFile(spark.sparkContext.hadoopConfiguration, s"$mdStoreSize", s"$outputBasePath/$MDSTORE_SIZE_PATH")
  }

}
