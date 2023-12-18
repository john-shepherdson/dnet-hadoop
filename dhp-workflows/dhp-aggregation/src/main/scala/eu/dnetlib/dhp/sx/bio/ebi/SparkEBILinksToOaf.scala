package eu.dnetlib.dhp.sx.bio.ebi

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.collection.CollectionUtils
import eu.dnetlib.dhp.schema.oaf.Oaf
import eu.dnetlib.dhp.sx.bio.BioDBToOAF
import eu.dnetlib.dhp.sx.bio.BioDBToOAF.EBILinkItem
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.slf4j.{Logger, LoggerFactory}
import eu.dnetlib.dhp.common.Constants.{MDSTORE_DATA_PATH, MDSTORE_SIZE_PATH}
import eu.dnetlib.dhp.schema.mdstore.MDStoreVersion
import eu.dnetlib.dhp.utils.DHPUtils.{MAPPER, writeHdfsFile}

object SparkEBILinksToOaf {

  def main(args: Array[String]): Unit = {
    val log: Logger = LoggerFactory.getLogger(getClass)
    val conf: SparkConf = new SparkConf()
    val parser = new ArgumentApplicationParser(
      IOUtils.toString(
        getClass.getResourceAsStream("/eu/dnetlib/dhp/sx/bio/ebi/ebi_to_df_params.json")
      )
    )
    parser.parseArgument(args)
    val spark: SparkSession =
      SparkSession
        .builder()
        .config(conf)
        .appName(SparkEBILinksToOaf.getClass.getSimpleName)
        .master(parser.get("master"))
        .getOrCreate()

    import spark.implicits._
    val sourcePath = parser.get("sourcePath")
    log.info(s"sourcePath  -> $sourcePath")
    val mdstoreOutputVersion = parser.get("mdstoreOutputVersion")
    log.info("mdstoreOutputVersion: {}", mdstoreOutputVersion)

    val cleanedMdStoreVersion = MAPPER.readValue(mdstoreOutputVersion, classOf[MDStoreVersion])
    val outputBasePath = cleanedMdStoreVersion.getHdfsPath
    log.info("outputBasePath: {}", outputBasePath)

    implicit val PMEncoder: Encoder[Oaf] = Encoders.kryo(classOf[Oaf])

    val ebLinks: Dataset[EBILinkItem] = spark.read
      .load(sourcePath)
      .as[EBILinkItem]
      .filter(l => l.links != null && l.links.startsWith("{"))

    CollectionUtils.saveDataset(
      ebLinks
        .flatMap(j => BioDBToOAF.parse_ebi_links(j.links))
        .filter(p => BioDBToOAF.EBITargetLinksFilter(p))
        .flatMap(p => BioDBToOAF.convertEBILinksToOaf(p)),
      s"$outputBasePath/$MDSTORE_DATA_PATH"
    )
    val df = spark.read.text(s"$outputBasePath/$MDSTORE_DATA_PATH")
    val mdStoreSize = df.count
    writeHdfsFile(spark.sparkContext.hadoopConfiguration, s"$mdStoreSize", s"$outputBasePath/$MDSTORE_SIZE_PATH")
  }
}
