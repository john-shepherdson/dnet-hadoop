package eu.dnetlib.dhp.sx.ebi

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.schema.oaf.Oaf
import eu.dnetlib.dhp.sx.bio.BioDBToOAF
import eu.dnetlib.dhp.sx.bio.BioDBToOAF.EBILinkItem
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
object SparkEBILinksToOaf {

  def main(args: Array[String]): Unit = {
    val log: Logger = LoggerFactory.getLogger(SparkEBILinksToOaf.getClass)
    val conf: SparkConf = new SparkConf()
    val parser = new ArgumentApplicationParser(IOUtils.toString(SparkEBILinksToOaf.getClass.getResourceAsStream("/eu/dnetlib/dhp/sx/ebi/ebi_to_df_params.json")))
    parser.parseArgument(args)
    val spark: SparkSession =
      SparkSession
        .builder()
        .config(conf)
        .appName(SparkEBILinksToOaf.getClass.getSimpleName)
        .master(parser.get("master")).getOrCreate()

    val sourcePath = parser.get("sourcePath")
    log.info(s"sourcePath  -> $sourcePath")
    val targetPath = parser.get("targetPath")
    log.info(s"targetPath  -> $targetPath")

    import spark.implicits._
    implicit  val PMEncoder: Encoder[Oaf] = Encoders.kryo(classOf[Oaf])
    val ebLinks:Dataset[EBILinkItem] = spark.read.load(sourcePath).as[EBILinkItem].filter(l => l.links!= null)

    ebLinks.flatMap(j =>BioDBToOAF.parse_ebi_links(j.links))
      .repartition(4000)
      .filter(p => BioDBToOAF.EBITargetLinksFilter(p))
      .flatMap(p => BioDBToOAF.convertEBILinksToOaf(p))
      .write.mode(SaveMode.Overwrite).save(targetPath)
  }
}
