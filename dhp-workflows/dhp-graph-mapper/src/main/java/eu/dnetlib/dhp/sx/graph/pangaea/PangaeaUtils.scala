package eu.dnetlib.dhp.sx.graph.pangaea

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}
import org.json4s
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import java.util.regex.Pattern
import scala.language.postfixOps
import scala.xml.{Elem, Node, XML}

case class PangaeaDataModel(identifier:String, title:List[String], objectType:List[String], creator:List[String],
                            publisher:List[String], dataCenter :List[String],subject :List[String], language:String,
                            rights:String, parent:String,relation :List[String],linkage:List[(String,String)] ) {}

object PangaeaUtils {


  def toDataset(input:String):PangaeaDataModel = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json: json4s.JValue = parse(input)
    val xml= (json \ "xml").extract[String]
    parseXml(xml)
  }

  def findDOIInRelation( input:List[String]):List[String] = {
    val pattern = Pattern.compile("\\b(10[.][0-9]{4,}(?:[.][0-9]+)*\\/(?:(?![\"&\\'<>])\\S)+)\\b")
    input.map(i => {
      val matcher = pattern.matcher(i)
      if (matcher.find())
        matcher.group(0)
      else
        null
    }).filter(i => i!= null)
  }

  def attributeOpt(attribute: String, node:Node): Option[String] =
    node.attribute(attribute) flatMap (_.headOption) map (_.text)

  def extractLinkage(node:Elem):List[(String, String)] = {
    (node \ "linkage").map(n =>(attributeOpt("type",n), n.text)).filter(t => t._1.isDefined).map(t=> (t._1.get, t._2))(collection.breakOut)
  }

  def parseXml(input:String):PangaeaDataModel = {
    val xml = XML.loadString(input)

    val identifier = (xml \ "identifier").text
    val title :List[String] = (xml \ "title").map(n => n.text)(collection.breakOut)
    val pType :List[String] = (xml \ "type").map(n => n.text)(collection.breakOut)
    val creators:List[String] = (xml \ "creator").map(n => n.text)(collection.breakOut)
    val publisher :List[String] = (xml \ "publisher").map(n => n.text)(collection.breakOut)
    val dataCenter :List[String] = (xml \ "dataCenter").map(n => n.text)(collection.breakOut)
    val subject :List[String] = (xml \ "subject").map(n => n.text)(collection.breakOut)
    val language= (xml \ "language").text
    val rights= (xml \ "rights").text
    val parentIdentifier= (xml \ "parentIdentifier").text
    val relation :List[String] = (xml \ "relation").map(n => n.text)(collection.breakOut)
    val relationFiltered = findDOIInRelation(relation)
    val linkage:List[(String,String)] = extractLinkage(xml)

    PangaeaDataModel(identifier,title, pType, creators,publisher, dataCenter, subject, language, rights, parentIdentifier, relationFiltered, linkage)
  }


  def getDatasetAggregator(): Aggregator[(String, PangaeaDataModel), PangaeaDataModel, PangaeaDataModel] =   new Aggregator[(String, PangaeaDataModel), PangaeaDataModel, PangaeaDataModel]{


      override def zero: PangaeaDataModel = null

      override def reduce(b: PangaeaDataModel, a: (String, PangaeaDataModel)): PangaeaDataModel = {
        if (b == null)
          a._2
        else {
          if (a == null)
            b
          else {
            if (b.title != null && b.title.nonEmpty)
              b
              else
              a._2

          }
        }
      }

      override def merge(b1: PangaeaDataModel, b2: PangaeaDataModel): PangaeaDataModel = {
        if (b1 == null)
          b2
        else {
          if (b2 == null)
            b1
          else {
            if (b1.title != null && b1.title.nonEmpty)
              b1
            else
              b2

          }
        }
      }
      override def finish(reduction: PangaeaDataModel): PangaeaDataModel = reduction

      override def bufferEncoder: Encoder[PangaeaDataModel] = Encoders.kryo[PangaeaDataModel]

      override def outputEncoder: Encoder[PangaeaDataModel] = Encoders.kryo[PangaeaDataModel]
    }




}