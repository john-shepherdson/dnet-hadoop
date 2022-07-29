package eu.dnetlib.dhp.datacite

import org.json4s.DefaultFormats
import org.json4s.JsonAST.{JField, JObject, JString}
import org.json4s.jackson.JsonMethods.parse

object DataciteUtilityTest {

  def convertToOAF(input: String): String = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json = parse(input)

    val isRelation: String = (json \\ "source").extractOrElse("NULL")

    if (isRelation != "NULL") {
      return "Relation"
    }

    val iType: List[String] = for {
      JObject(instance)                             <- json \\ "instance"
      JField("instancetype", JObject(instancetype)) <- instance
      JField("classname", JString(classname))       <- instancetype

    } yield classname

    val l: String = iType.head.toLowerCase()
    l
  }

}
