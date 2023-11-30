package eu.dnetlib.dhp.enrich.orcid

import eu.dnetlib.dhp.application.AbstractScalaApplication
import eu.dnetlib.dhp.oa.merge.AuthorMerger
import eu.dnetlib.dhp.schema.oaf.{OtherResearchProduct, Publication, Result, Software}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.slf4j.{Logger, LoggerFactory}

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
    val orcidPublication: Dataset[Row] = generateOrcidTable(spark, orcidPath)
    enrichResult(
      spark,
      s"$graphPath/publication",
      orcidPublication,
      s"$targetPath/publication",
      Encoders.bean(classOf[Publication])
    )
    enrichResult(
      spark,
      s"$graphPath/dataset",
      orcidPublication,
      s"$targetPath/dataset",
      Encoders.bean(classOf[eu.dnetlib.dhp.schema.oaf.Dataset])
    )
    enrichResult(
      spark,
      s"$graphPath/software",
      orcidPublication,
      s"$targetPath/software",
      Encoders.bean(classOf[Software])
    )
    enrichResult(
      spark,
      s"$graphPath/otherresearchproduct",
      orcidPublication,
      s"$targetPath/otherresearchproduct",
      Encoders.bean(classOf[OtherResearchProduct])
    )
  }

  private def enrichResult[T <: Result](
    spark: SparkSession,
    graphPath: String,
    orcidPublication: Dataset[Row],
    outputPath: String,
    enc: Encoder[T]
  ): Unit = {

    val entities = spark.read
      .schema(enc.schema)
      .json(graphPath)
      .select(col("id"), col("datainfo"), col("instance"))
      .where("datainfo.deletedbyinference = false")
      .drop("datainfo")
      .withColumn("instances", explode(col("instance")))
      .withColumn("pids", explode(col("instances.pid")))
      .select(
        col("pids.qualifier.classid").alias("pid_schema"),
        col("pids.value").alias("pid_value"),
        col("id").alias("dnet_id")
      )

    val orcidDnet = orcidPublication
      .join(
        entities,
        lower(col("schema")).equalTo(lower(col("pid_schema"))) &&
        lower(col("value")).equalTo(lower(col("pid_value"))),
        "inner"
      )
      .groupBy(col("dnet_id"))
      .agg(collect_set(orcidPublication("author")).alias("orcid_authors"))
      .select("dnet_id", "orcid_authors")
      .cache()
    orcidDnet.count()
    val result = spark.read.schema(enc.schema).json(graphPath).as[T](enc)

    result
      .joinWith(orcidDnet, result("id").equalTo(orcidDnet("dnet_id")), "left")
      .map {
        case (r: T, null) =>
          r
        case (p: T, r: Row) =>
          p.setAuthor(AuthorMerger.enrichOrcid(p.getAuthor, AuthorEnricher.toOAFAuthor(r)))
          p
      }(enc)
      .write
      .mode(SaveMode.Overwrite)
      .option("compression", "gzip")
      .json(outputPath)
  }

  private def generateOrcidTable(spark: SparkSession, inputPath: String): Dataset[Row] = {
    val orcidAuthors =
      spark.read.load(s"$inputPath/Authors").select("orcid", "familyName", "givenName", "creditName", "otherNames")
    val orcidWorks = spark.read
      .load(s"$inputPath/Works")
      .select(col("orcid"), explode(col("pids")).alias("identifier"))
      .where(
        "identifier.schema = 'doi' or identifier.schema ='pmid' or identifier.schema ='pmc' or identifier.schema ='arxiv' or identifier.schema ='handle'"
      )
    val orcidPublication = orcidAuthors
      .join(orcidWorks, orcidAuthors("orcid").equalTo(orcidWorks("orcid")))
      .select(
        col("identifier.schema").alias("schema"),
        col("identifier.value").alias("value"),
        struct(orcidAuthors("orcid").alias("orcid"), col("givenName"), col("familyName")).alias("author")
      )
    orcidPublication.cache()
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
