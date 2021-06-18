package eu.dnetlib.dhp.sx.bio

import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils
import eu.dnetlib.dhp.schema.oaf.{Dataset, Oaf}
import org.json4s.DefaultFormats
import org.json4s.JsonAST.{JField, JObject, JString}
import org.json4s.jackson.JsonMethods.parse

object PDBToOAF {

  def convert(input:String):List[Oaf]= {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json = parse(input)
    val pdb = (json \ "pdb").extract[String]

    if (pdb.isEmpty)
      return List()

    val d = new Dataset

    d.setPid(List(OafMapperUtils.structuredProperty()))





    List()

  }


}
