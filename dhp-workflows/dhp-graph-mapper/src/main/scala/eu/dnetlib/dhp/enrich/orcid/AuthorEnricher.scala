package eu.dnetlib.dhp.enrich.orcid

import eu.dnetlib.dhp.schema.oaf.{Author, Publication}
import eu.dnetlib.dhp.schema.sx.OafUtils
import org.apache.spark.sql.Row

import scala.collection.JavaConverters._

object AuthorEnricher extends Serializable {

  def createAuthor(givenName: String, familyName: String, orcid: String): Author = {
    val a = new Author
    a.setName(givenName)
    a.setSurname(familyName)
    a.setFullname(s"$givenName $familyName")
    a.setPid(List(OafUtils.createSP(orcid, "ORCID", "ORCID")).asJava)
    a

  }

  def toOAFAuthor(r: Row): java.util.List[Author] = {
    r.getList[Row](1)
      .asScala
      .map(s => createAuthor(s.getAs[String]("givenName"), s.getAs[String]("familyName"), s.getAs[String]("orcid")))
      .toList
      .asJava
  }

//  def enrichAuthor(p:Publication,r:Row): Unit = {
//    val k:Map[String, OAuthor] =r.getList[Row](1).asScala.map(s => (s.getAs[String]("orcid"), OAuthor(s.getAs[String]("givenName") ,s.getAs[String]("familyName") ))).groupBy(_._1).mapValues(_.map(_._2).head)
//    println(k)
//
//
//
//  }

}
