package eu.dnetlib.dhp.common.author

import eu.dnetlib.dhp.application.AbstractScalaApplication
import eu.dnetlib.dhp.schema.common.{ModelConstants, ModelSupport}
import eu.dnetlib.dhp.utils.{MatchData, ORCIDAuthorEnricher, ORCIDAuthorEnricherResult}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.slf4j.{Logger, LoggerFactory}
import eu.dnetlib.dhp.common.enrichment.Constants.PROPAGATION_DATA_INFO_TYPE

import scala.collection.JavaConverters._

abstract class SparkEnrichWithOrcidAuthors(propertyPath: String, args: Array[String], log: Logger)
    extends AbstractScalaApplication(propertyPath, args, log: Logger) {

  /** Here all the spark applications runs this method
    * where the whole logic of the spark node is defined
    */
  override def run(): Unit = {
    val graphPath = parser.get("graphPath")
    log.info(s"graphPath is '$graphPath'")
    val orcidPath = parser.get("orcidPath")
    log.info(s"orcidPath is '$orcidPath'")
    val targetPath = parser.get("targetPath")
    log.info(s"targetPath is '$targetPath'")
    val workingDir = parser.get("workingDir")
    log.info(s"targetPath is '$workingDir'")
    val classid =
      Option(parser.get("matchingSource")).map(_ => ModelConstants.ORCID_PENDING).getOrElse(ModelConstants.ORCID)

    log.info(s"classid is '$classid'")
    val provenance =
      Option(parser.get("matchingSource")).map(_ => PROPAGATION_DATA_INFO_TYPE).getOrElse("ORCID_ENRICHMENT")
    log.info(s"targetPath is '$workingDir'")

    createTemporaryData(spark, graphPath, orcidPath, workingDir)
    analisys(workingDir, classid, provenance)
    generateGraph(spark, graphPath, workingDir, targetPath)
  }

  private def generateGraph(spark: SparkSession, graphPath: String, workingDir: String, targetPath: String): Unit = {

    ModelSupport.entityTypes.asScala
      .filter(e => ModelSupport.isResult(e._1))
      .foreach(e => {
        val resultType = e._1.name()
        val enc = Encoders.bean(e._2)

        val matched = spark.read
          .schema(Encoders.bean(classOf[ORCIDAuthorEnricherResult]).schema)
          .parquet(s"${workingDir}/${resultType}_matched")
          .selectExpr("id", "enriched_author")

        spark.read
          .schema(enc.schema)
          .json(s"$graphPath/$resultType")
          .join(matched, Seq("id"), "left")
          .withColumn(
            "author",
            when(size(col("enriched_author")).gt(0), col("enriched_author"))
              .otherwise(col("author"))
          )
          .drop("enriched_author")
          .write
          .mode(SaveMode.Overwrite)
          .option("compression", "gzip")
          .json(s"${targetPath}/${resultType}")

      })

  }

  def createTemporaryData(spark: SparkSession, graphPath: String, orcidPath: String, targetPath: String): Unit

  private def analisys(targetPath: String, classid: String, provenance: String): Unit = {
    ModelSupport.entityTypes.asScala
      .filter(e => ModelSupport.isResult(e._1))
      .foreach(e => {
        val resultType = e._1.name()

        spark.read
          .parquet(s"$targetPath/${resultType}_unmatched")
          .where("size(graph_authors) > 0")
          .as[MatchData](Encoders.bean(classOf[MatchData]))
          .map(md => {
            ORCIDAuthorEnricher.enrichOrcid(md.id, md.graph_authors, md.orcid_authors, classid, provenance)
          })(Encoders.bean(classOf[ORCIDAuthorEnricherResult]))
          .write
          .option("compression", "gzip")
          .mode("overwrite")
          .parquet(s"$targetPath/${resultType}_matched")
      })
  }
}
