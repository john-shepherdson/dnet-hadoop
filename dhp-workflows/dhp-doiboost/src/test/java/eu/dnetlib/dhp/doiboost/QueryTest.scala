package eu.dnetlib.dhp.doiboost

import eu.dnetlib.dhp.schema.oaf.{Publication, Relation}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import org.codehaus.jackson.map.{ObjectMapper, SerializationConfig}
import org.json4s
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._

import scala.collection.JavaConverters._
class QueryTest {

  def extract_payload(input:String) :String = {

    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json: json4s.JValue = parse(input)


    compact(render((json \ "payload")))



  }


  def has_ands(r:Relation) :Boolean = {

    r.getCollectedfrom!= null && r.getCollectedfrom.asScala.count(k => k.getValue.contains("Australian")) > 0

  }

  def hasInstanceWithUrl(p:Publication):Boolean = {
    val c = p.getInstance.asScala.map(i => i.getUrl!= null && !i.getUrl.isEmpty).size
    !(!p.getInstance.isEmpty && c == p.getInstance().size)
  }


  def hasNullAccessRights(p:Publication):Boolean = {
    val c = p.getInstance.asScala.map(i => i.getAccessright!= null && i.getAccessright.getClassname.nonEmpty).size
    !p.getInstance.isEmpty && c == p.getInstance().size()
  }



  def extractId(input:String):String = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json: json4s.JValue = parse(input)
    (json \ "id").extractOrElse[String](null)


  }


  def myQuery(spark:SparkSession, sc:SparkContext): Unit = {
    implicit val mapEncoderPub: Encoder[Publication] = Encoders.kryo[Publication]
    val mapper = new ObjectMapper()
    mapper.getSerializationConfig.enable(SerializationConfig.Feature.INDENT_OUTPUT)

    val ds:Dataset[Publication] = spark.read.load("/tmp/p").as[Publication]


    val sc = spark.sparkContext


    ds.filter(p =>p.getBestaccessright!= null && p.getBestaccessright.getClassname.nonEmpty).count()
    val typologies =List("dataset","datasource","organization","otherresearchproduct","project","publication","software")
    val basePath ="/tt"

    typologies.map(tp => sc.textFile(s"$basePath/dataset").map(s =>extractId(tp) ).distinct.count()).sum()

  }

}
