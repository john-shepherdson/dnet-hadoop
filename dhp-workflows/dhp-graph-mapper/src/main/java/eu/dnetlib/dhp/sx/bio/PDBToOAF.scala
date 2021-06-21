package eu.dnetlib.dhp.sx.bio

import eu.dnetlib.dhp.schema.common.ModelConstants
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils
import eu.dnetlib.dhp.schema.oaf.{DataInfo, Dataset, Oaf}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import scala.collection.JavaConverters._
object PDBToOAF {

  val dataInfo: DataInfo = OafMapperUtils.dataInfo(false, null, false, false, ModelConstants.PROVENANCE_ACTION_SET_QUALIFIER, "0.9")
  def convert(input:String):List[Oaf]= {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json = parse(input)
    val pdb = (json \ "pdb").extract[String]

    if (pdb.isEmpty)
      return List()

    val d = new Dataset

    d.setPid(
      List(
        OafMapperUtils.structuredProperty(pdb, "pdb", "pdb", ModelConstants.DNET_PID_TYPES, ModelConstants.DNET_PID_TYPES,dataInfo)
      ).asJava
    )




    List(d)

  }


}
