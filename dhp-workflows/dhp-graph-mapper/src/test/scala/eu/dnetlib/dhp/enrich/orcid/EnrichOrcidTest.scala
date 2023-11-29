package eu.dnetlib.dhp.enrich.orcid
import eu.dnetlib.dhp.schema.oaf.Publication
import org.apache.spark.sql.{Column, Encoder, Encoders, Row, SparkSession}
import org.junit.jupiter.api.Test
import org.slf4j.{Logger, LoggerFactory}
import org.apache.spark.sql.functions._


case class Pid(pidScheme: String, pidValue: String) {}

case class AuthorPid(fullName: String, pids: List[Pid]) {}

case class PubSummary(id: String, authorWithPids: List[AuthorPid])

class EnrichOrcidTest {

  val log: Logger = LoggerFactory.getLogger(getClass)


  def orcid_intersection_wrong(p: PubSummary): PubSummary = {

    if (p.authorWithPids.isEmpty)
      null
    else {
      val incorrectAuthor = p.authorWithPids.filter(a => a.pids.filter(p => p.pidScheme != null && p.pidScheme.toLowerCase.contains("orcid")).map(p => p.pidValue.toLowerCase).distinct.size > 1)
      if (incorrectAuthor.nonEmpty) {
        PubSummary(p.id, incorrectAuthor)
      }
      else {
        null
      }
    }
  }



  def test() = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val schema = Encoders.bean(classOf[Publication]).schema


    val simplifyAuthor = udf((r: Seq[Row]) => {
      r
        .map(k =>
          AuthorPid(k.getAs[String]("fullname"),
            k.getAs[Seq[Row]]("pid")
              .map(
                p => Pid(p.getAs[Row]("qualifier").getAs[String]("classid"), p.getAs[String]("value"))
              ).toList)
        ).filter(l => l.pids.nonEmpty)
        .toList
    }
    )

    val wrong_orcid_intersection = udf((a: Seq[Row]) => {
      a.map(author => {
        val pids_with_orcid: Seq[Row] = author.getAs[Seq[Row]]("pids").filter(p => p.getAs[String]("pidScheme")!= null && p.getAs[String]("pidScheme").toLowerCase.contains("orcid"))
        if (pids_with_orcid.exists(p => p.getAs[String]("pidScheme").equals("ORCID"))) {
          if (pids_with_orcid.map(p => p.getAs[String]("pidValue").toLowerCase).distinct.size > 1) {
            AuthorPid(author.getAs[String]("fullName"),pids_with_orcid.map(p => Pid(p.getAs[String]("pidScheme"),p.getAs[String]("pidValue"))).toList )

          }
          else
            null
        } else

          null
      }).filter(author => author != null)
    })
    val enriched = spark.read.schema(schema).json("/Users/sandro/orcid_test/publication_enriched").select(col("id"), simplifyAuthor(col("author")).alias("authors"))
      .select(col("id"), wrong_orcid_intersection(col("authors")).alias("wi")).where("wi is not null")
    enriched.show(20, 1000, true)
  }


}
