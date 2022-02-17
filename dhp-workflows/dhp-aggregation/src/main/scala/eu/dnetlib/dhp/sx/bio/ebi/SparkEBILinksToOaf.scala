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
    val targetPath = parser.get("targetPath")
    log.info(s"targetPath  -> $targetPath")
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
      targetPath
    )
  }
}
