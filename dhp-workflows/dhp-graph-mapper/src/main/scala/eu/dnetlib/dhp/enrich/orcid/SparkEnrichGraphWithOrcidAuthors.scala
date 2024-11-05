package eu.dnetlib.dhp.enrich.orcid

import eu.dnetlib.dhp.common.author.SparkEnrichWithOrcidAuthors
import eu.dnetlib.dhp.schema.common.ModelSupport
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

class SparkEnrichGraphWithOrcidAuthors(propertyPath: String, args: Array[String], log: Logger)
    extends SparkEnrichWithOrcidAuthors(propertyPath, args, log: Logger) {

  override def createTemporaryData(spark:SparkSession, graphPath: String, orcidPath: String, targetPath: String): Unit = {
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
}

  object SparkEnrichGraphWithOrcidAuthors {

    val log: Logger = LoggerFactory.getLogger(SparkEnrichGraphWithOrcidAuthors.getClass)

    def main(args: Array[String]): Unit = {
      new SparkEnrichGraphWithOrcidAuthors("/eu/dnetlib/dhp/enrich/orcid/enrich_graph_orcid_parameters.json", args, log)
        .initialize()
        .run()
    }
  }

