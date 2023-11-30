package eu.dnetlib.dhp.enrich.orcid

import eu.dnetlib.dhp.schema.oaf.{Author, Publication}
import org.apache.spark.sql.{Column, Encoder, Encoders, Row, SparkSession}
import org.junit.jupiter.api.Test
import org.slf4j.{Logger, LoggerFactory}
import org.apache.spark.sql.functions._

class EnrichOrcidTest {

  val log: Logger = LoggerFactory.getLogger(getClass)

  def test() = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
//    spark.sparkContext.setLogLevel("ERROR")

//    new SparkEnrichGraphWithOrcidAuthors(null, null, null)
//      .enrichResult(
//        spark,
//        "/Users/sandro/orcid_test/publication",
//        "",
//        "/tmp/graph/",
//        Encoders.bean(classOf[Publication])
//      )

    val schema = Encoders.bean(classOf[Publication]).schema
//
//    val simplifyAuthor = udf((r: Seq[Row]) => {
//      r
//        .map(k =>
//          AuthorPid(
//            k.getAs[String]("fullname"),
//            k.getAs[Seq[Row]]("pid")
//              .map(p => Pid(p.getAs[Row]("qualifier").getAs[String]("classid"), p.getAs[String]("value")))
//              .toList
//          )
//        )
//        .filter(l => l.pids.nonEmpty)
//        .toList
//    })
//
//    val wrong_orcid_intersection = udf((a: Seq[Row]) => {
//      a.map(author => {
//        val pids_with_orcid: Seq[Row] = author
//          .getAs[Seq[Row]]("pids")
//          .filter(p =>
//            p.getAs[String]("pidScheme") != null && p.getAs[String]("pidScheme").toLowerCase.contains("orcid")
//          )
//        if (pids_with_orcid.exists(p => p.getAs[String]("pidScheme").equals("ORCID"))) {
//          if (pids_with_orcid.map(p => p.getAs[String]("pidValue").toLowerCase).distinct.size > 1) {
//            AuthorPid(
//              author.getAs[String]("fullName"),
//              pids_with_orcid.map(p => Pid(p.getAs[String]("pidScheme"), p.getAs[String]("pidValue"))).toList
//            )
//
//          } else
//            null
//        } else
//          null
//      }).filter(author => author != null)
//    })

    Encoders
    import spark.implicits._

//    val enriched = spark.read
//      .schema(schema)
//      .json("/Users/sandro/orcid_test/publication_enriched")
//      .select(col("id"), explode(col("author")).as("authors"))
//      .withColumn("ap", col("authors.pid.qualifier.classid"))
//      .withColumn("dp", col("authors.pid.datainfo.provenanceAction.classid"))
//
//      .show()

  }

}
