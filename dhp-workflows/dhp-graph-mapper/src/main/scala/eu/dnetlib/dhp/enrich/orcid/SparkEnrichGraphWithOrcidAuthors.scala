package eu.dnetlib.dhp.enrich.orcid

import eu.dnetlib.dhp.application.AbstractScalaApplication
import eu.dnetlib.dhp.schema.common.ModelSupport
import eu.dnetlib.dhp.schema.oaf._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.slf4j.{Logger, LoggerFactory}

import scala.beans.BeanProperty
import scala.collection.JavaConverters._

case class OrcidAutor(
  @BeanProperty var orcid: String,
  @BeanProperty var familyName: String,
  @BeanProperty var givenName: String,
  @BeanProperty var creditName: String,
  @BeanProperty var otherNames: java.util.List[String]
) {
  def this() = this("null", "null", "null", "null", null)
}

case class MatchData(
  @BeanProperty var id: String,
  @BeanProperty var graph_authors: java.util.List[Author],
  @BeanProperty var orcid_authors: java.util.List[OrcidAutor]
) {
  def this() = this("null", null, null)
}

case class MatchedAuthors(
  @BeanProperty var author: Author,
  @BeanProperty var orcid: OrcidAutor,
  @BeanProperty var `type`: String
)

class SparkEnrichGraphWithOrcidAuthors(propertyPath: String, args: Array[String], log: Logger)
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

    createTemporaryData(graphPath, orcidPath, targetPath)
    analisys(targetPath)
    generateGraph(graphPath, targetPath)
  }

  private def generateGraph(graphPath: String, targetPath: String): Unit = {

    ModelSupport.entityTypes.asScala
      .filter(e => ModelSupport.isResult(e._1))
      .foreach(e => {
        val resultType = e._1.name()
        val enc = Encoders.bean(e._2)

        val matched = spark.read
          .schema(Encoders.bean(classOf[ORCIDAuthorEnricherResult]).schema)
          .parquet(s"${targetPath}/${resultType}_matched")
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

  private def createTemporaryData(graphPath: String, orcidPath: String, targetPath: String): Unit = {
    val orcidAuthors =
      spark.read.load(s"$orcidPath/Authors").select("orcid", "familyName", "givenName", "creditName", "otherNames")

    val orcidWorks = spark.read
      .load(s"$orcidPath/Works")
      .select(col("orcid"), explode(col("pids")).alias("identifier"))
      .where(
        "identifier.schema IN('doi','pmid','pmc','arxiv','handle')" // scopus eid ?
      )

    val orcidWorksWithAuthors = orcidAuthors
      .join(orcidWorks, Seq("orcid"))
      .select(
        lower(col("identifier.schema")).alias("pid_schema"),
        lower(col("identifier.value")).alias("pid_value"),
        struct(
          col("orcid"),
          col("givenName"),
          col("familyName"),
          col("creditName"),
          col("otherNames")
        ).alias("author")
      )
      .cache()

    ModelSupport.entityTypes.asScala
      .filter(e => ModelSupport.isResult(e._1))
      .foreach(e => {
        val resultType = e._1.name()
        val enc = Encoders.bean(e._2)

        val oaEntities = spark.read
          .schema(enc.schema)
          .json(s"$graphPath/$resultType")
          .select(col("id"), col("datainfo"), col("instance"))
          .where("datainfo.deletedbyinference != true")
          .drop("datainfo")
          .withColumn("instances", explode(col("instance")))
          .withColumn("pids", explode(col("instances.pid")))
          .select(
            lower(col("pids.qualifier.classid")).alias("pid_schema"),
            lower(col("pids.value")).alias("pid_value"),
            col("id")
          )

        val orcidDnet = orcidWorksWithAuthors
          .join(
            oaEntities,
            Seq("pid_schema", "pid_value"),
            "inner"
          )
          .groupBy(col("id"))
          .agg(collect_set(col("author")).alias("orcid_authors"))
          .select("id", "orcid_authors")

        val result =
          spark.read.schema(enc.schema).json(s"$graphPath/$resultType").selectExpr("id", "author as graph_authors")

        result
          .join(orcidDnet, Seq("id"))
          .write
          .mode(SaveMode.Overwrite)
          .option("compression", "gzip")
          .parquet(s"$targetPath/${resultType}_unmatched")
      })

    orcidWorksWithAuthors.unpersist()
  }

  private def analisys(targetPath: String): Unit = {
    ModelSupport.entityTypes.asScala
      .filter(e => ModelSupport.isResult(e._1))
      .foreach(e => {
        val resultType = e._1.name()

        spark.read
          .parquet(s"$targetPath/${resultType}_unmatched")
          .where("size(graph_authors) > 0")
          .as[MatchData](Encoders.bean(classOf[MatchData]))
          .map(md => {
            ORCIDAuthorEnricher.enrichOrcid(md.id, md.graph_authors, md.orcid_authors)
          })(Encoders.bean(classOf[ORCIDAuthorEnricherResult]))
          .write
          .option("compression", "gzip")
          .mode("overwrite")
          .parquet(s"$targetPath/${resultType}_matched")
      })
  }
}

object SparkEnrichGraphWithOrcidAuthors {

  val log: Logger = LoggerFactory.getLogger(SparkEnrichGraphWithOrcidAuthors.getClass)

  def main(args: Array[String]): Unit = {
    new SparkEnrichGraphWithOrcidAuthors("/eu/dnetlib/dhp/enrich/orcid/enrich_graph_orcid_parameters.json", args, log)
      .initialize()
      .run()
  }
}
