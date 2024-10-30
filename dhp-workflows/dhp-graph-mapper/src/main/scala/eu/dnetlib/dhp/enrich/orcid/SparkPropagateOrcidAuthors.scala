package eu.dnetlib.dhp.enrich.orcid

import eu.dnetlib.dhp.schema.common.ModelSupport
import eu.dnetlib.dhp.schema.oaf.{Relation, Result}
import eu.dnetlib.dhp.utils.OrcidAuthor
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

class SparkPropagateOrcidAuthors(propertyPath: String, args: Array[String], log: Logger)
    extends SparkEnrichGraphWithOrcidAuthors(propertyPath, args, log: Logger) {

  override def createTemporaryData(graphPath: String, orcidPath: String, targetPath: String): Unit = {
    val relEnc = Encoders.bean(classOf[Relation])

    ModelSupport.entityTypes.asScala
      .filter(e => ModelSupport.isResult(e._1))
      .foreach(e => {
        val resultType = e._1.name()
        val enc = Encoders.bean(e._2)

        val orcidDnet = spark.read
          .load("$graphPath/$resultType")
          .as[Result]
          .map(
            result =>
              (
                result.getId,
                result.getAuthor.asScala.map(a => OrcidAuthor("extract ORCID", a.getSurname, a.getName, a.getFullname, null))
            )
          )
          .where("size(_2) > 0")
          .selectExpr("_1 as id", "_2 as orcid_authors")

        val result =
          spark.read.schema(enc.schema).json(s"$graphPath/$resultType").selectExpr("id", "author as graph_authors")

        val supplements = spark.read.schema(relEnc.schema).json(s"$graphPath/relation").where("relclass IN('isSupplementedBy', 'isSupplementOf')").selectExpr("source as id", "target")

        result
          .join(supplements, Seq("id"))
          .join(orcidDnet, orcidDnet("id") === col("target"))
          .drop("target")
          .write
          .mode(SaveMode.Overwrite)
          .option("compression", "gzip")
          .parquet(s"$targetPath/${resultType}_unmatched")
      })
  }
}

object SparkPropagateOrcidAuthors {

  val log: Logger = LoggerFactory.getLogger(SparkPropagateOrcidAuthors.getClass)

  def main(args: Array[String]): Unit = {
    new SparkPropagateOrcidAuthors("/eu/dnetlib/dhp/enrich/orcid/enrich_graph_orcid_parameters.json", args, log)
      .initialize()
      .run()
  }
}
