package eu.dnetlib.dhp.sx.bio

import eu.dnetlib.dhp.schema.common.ModelConstants
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils
import eu.dnetlib.dhp.schema.oaf.{Author, DataInfo, Dataset, Instance, KeyValue, Oaf, Relation}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

import scala.collection.JavaConverters._
object PDBToOAF {

  val dataInfo: DataInfo = OafMapperUtils.dataInfo(false, null, false, false, ModelConstants.PROVENANCE_ACTION_SET_QUALIFIER, "0.9")

  val collectedFrom: KeyValue = OafMapperUtils.keyValue("10|opendoar____::d1c373ab1570cfb9a7dbb53c186b37a2", "Protein Data Bank")

  def convert(input:String):List[Oaf]= {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json = parse(input)
    val pdb = (json \ "pdb").extract[String].toLowerCase

    if (pdb.isEmpty)
      return List()

    val d = new Dataset

    d.setPid(
      List(
        OafMapperUtils.structuredProperty(pdb, "pdb", "pdb", ModelConstants.DNET_PID_TYPES, ModelConstants.DNET_PID_TYPES,dataInfo)
      ).asJava
    )

    d.setCollectedfrom(List(collectedFrom).asJava)
    d.setDataInfo(dataInfo)
    d.setId(OafMapperUtils.createOpenaireId(50,s"pdb_________::$pdb", true))
    d.setOriginalId(List(pdb).asJava)

    val title = (json \ "title").extractOrElse[String](null)

    if (title== null)
      return List()
    d.setTitle(List(OafMapperUtils.structuredProperty(title, ModelConstants.MAIN_TITLE_QUALIFIER, dataInfo)).asJava)


    val authors:List[String] = (json \ "authors").extractOrElse[List[String]](null)

    if (authors!= null)
      {
        val convertedAuthors = authors.zipWithIndex.map{a =>

          val res = new Author
          res.setFullname(a._1)
          res.setRank(a._2+1)
          res
        }

        d.setAuthor(convertedAuthors.asJava)
      }

    val i = new Instance

    i.setPid(d.getPid)
    i.setUrl(List(s"https://www.rcsb.org/structure/$pdb").asJava)
    i.setInstancetype(OafMapperUtils.qualifier("0046", "Bioentity", ModelConstants.DNET_PUBLICATION_RESOURCE, ModelConstants.DNET_PUBLICATION_RESOURCE))

    i.setCollectedfrom(collectedFrom)
    d.setInstance(List(i).asJava)
    val pmid = (json \ "pmid").extractOrElse[String](null)

    if (pmid != null) {
      val rel = new Relation
      rel.setCollectedfrom(List(collectedFrom).asJava)
      rel.setDataInfo(dataInfo)

      rel.setRelType("resultResult")
      rel.setSubRelType("supplement")
      rel.setRelClass("IsSupplementTo")

      rel.setSource(d.getId)
      rel.setTarget(s"unresolved::$pmid::pmid")
      return List(d,rel)
    }
    List(d)
  }
}
