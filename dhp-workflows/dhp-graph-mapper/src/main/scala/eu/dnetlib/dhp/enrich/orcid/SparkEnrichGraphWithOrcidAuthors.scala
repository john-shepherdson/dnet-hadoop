package eu.dnetlib.dhp.enrich.orcid

import com.fasterxml.jackson.databind.ObjectMapper
import eu.dnetlib.dhp.application.AbstractScalaApplication
import eu.dnetlib.dhp.oa.merge.AuthorMerger
import eu.dnetlib.dhp.schema.oaf.{Author, DataInfo, Instance, Publication, StructuredProperty}
import org.apache.spark.sql.{Dataset, Encoder, Encoders, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, collect_set, concat, explode, expr, first, flatten, lower, size, struct}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.spark.sql.types._

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
    enrichResult(spark, graphPath, orcidPath, targetPath)
  }

  def enrichResult(spark: SparkSession, graphPath: String, orcidPath: String, outputPath: String): Unit = {
    val orcidPublication = generateOrcidTable(spark, orcidPath)
    implicit val publicationEncoder = Encoders.bean(classOf[Publication])

    val aschema = new StructType()
      .add("id", StringType)
      .add("dataInfo", Encoders.bean(classOf[DataInfo]).schema)
      .add(
        "author",Encoders.bean(classOf[Author]).schema

      )

    val schema = new StructType()
      .add("id", StringType)
      .add("dataInfo", Encoders.bean(classOf[DataInfo]).schema)
      .add(
        "instance",
        ArrayType(new StructType().add("pid", ArrayType(Encoders.bean(classOf[StructuredProperty]).schema)))
      )
    val entities = spark.read
      .schema(schema)
      .json(graphPath)
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

    val publication = spark.read.schema(publicationEncoder.schema).json(graphPath).as[Publication]

    publication
      .joinWith(orcidDnet, publication("id").equalTo(orcidDnet("dnet_id")), "left")
      .map {
        case (p: Publication, null) => {
          p
        }
        case (p: Publication, r: Row) =>
          p.setAuthor(AuthorMerger.enrichOrcid2(p.getAuthor, AuthorEnricher.toOAFAuthor(r)))
          p
      }
      .write
      .mode(SaveMode.Overwrite)
      .option("compression", "gzip")
      .json(outputPath)
  }

  def generateOrcidTable(spark: SparkSession, inputPath: String): Dataset[Row] = {
    val orcidAuthors =
      spark.read.load(s"$inputPath/Authors").select("orcid", "familyName", "givenName", "creditName", "otherNames")
    val orcidWorks = spark.read
      .load(s"$inputPath/Works")
      .select(col("orcid"), explode(col("pids")).alias("identifier"))
      .where(
        "identifier.schema = 'doi' or identifier.schema ='pmid' or identifier.schema ='pmc' or identifier.schema ='arxiv' or identifier.schema ='handle'"
      )
    orcidAuthors
      .join(orcidWorks, orcidAuthors("orcid").equalTo(orcidWorks("orcid")))
      .select(
        col("identifier.schema").alias("schema"),
        col("identifier.value").alias("value"),
        struct(orcidAuthors("orcid").alias("orcid"), col("givenName"), col("familyName")).alias("author")
      )
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
