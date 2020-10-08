package eu.dnetlib.dhp.doiboost
import eu.dnetlib.dhp.schema.oaf.Project
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{col, sum}
import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import org.codehaus.jackson.map.ObjectMapper
import org.json4s.DefaultFormats
import org.json4s
import org.json4s.DefaultFormats
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods._
import scala.::
import scala.collection.JavaConverters._
class QueryTest {

  def extract_payload(input:String) :String = {

    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json: json4s.JValue = parse(input)


    compact(render((json \ "payload")))



  }


  def myQuery(spark:SparkSession, sc:SparkContext): Unit = {
    implicit val mapEncoderPub: Encoder[Project] = Encoders.kryo[Project]


//    val ds:Dataset[Project] = spark.createDataset(sc.sequenceFile("", classOf[Text], classOf[Text])
//      .map(_._2.toString)
//      .map(s => new ObjectMapper().readValue(s, classOf[Project])))
//
//      ds.write.saveAsTable()



  }

}
