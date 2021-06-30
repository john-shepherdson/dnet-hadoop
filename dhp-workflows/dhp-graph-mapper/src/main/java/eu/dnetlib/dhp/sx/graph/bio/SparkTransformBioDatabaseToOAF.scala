package eu.dnetlib.dhp.sx.graph.bio

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.schema.oaf.{Oaf, Result}
import BioDBToOAF.ScholixResolved
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

object SparkTransformBioDatabaseToOAF {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    val log: Logger = LoggerFactory.getLogger(getClass)
    val parser = new ArgumentApplicationParser(IOUtils.toString(getClass.getResourceAsStream("/eu/dnetlib/dhp/sx/graph/bio/bio_to_oaf_params.json")))
    parser.parseArgument(args)
    val database: String = parser.get("database")
    log.info("database: {}", database)

    val dbPath: String = parser.get("dbPath")
    log.info("dbPath: {}", database)
    val targetPath: String = parser.get("targetPath")
    log.info("targetPath: {}", database)

    val spark: SparkSession =
      SparkSession
        .builder()
        .config(conf)
        .appName(getClass.getSimpleName)
        .master(parser.get("master")).getOrCreate()
    val sc = spark.sparkContext

    implicit  val resultEncoder: Encoder[Oaf] = Encoders.kryo(classOf[Oaf])
    import  spark.implicits._

    database.toUpperCase() match {
      case "UNIPROT" =>
        spark.createDataset(sc.textFile(dbPath).flatMap(i => BioDBToOAF.uniprotToOAF(i))).write.mode(SaveMode.Overwrite).save(targetPath)
      case "PDB"=>
        spark.createDataset(sc.textFile(dbPath).flatMap(i => BioDBToOAF.pdbTOOaf(i))).write.mode(SaveMode.Overwrite).save(targetPath)
      case "SCHOLIX" =>
        spark.read.load(dbPath).as[ScholixResolved].map(i => BioDBToOAF.scholixResolvedToOAF(i)).write.mode(SaveMode.Overwrite).save(targetPath)
      case "CROSSREF_LINKS"=>
        spark.createDataset(sc.textFile(dbPath).map(i => BioDBToOAF.crossrefLinksToOaf(i))).write.mode(SaveMode.Overwrite).save(targetPath)
    }
  }

}
