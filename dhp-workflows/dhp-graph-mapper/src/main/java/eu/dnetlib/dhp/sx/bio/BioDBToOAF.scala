package eu.dnetlib.dhp.sx.bio

import eu.dnetlib.dhp.schema.common.ModelConstants
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils
import eu.dnetlib.dhp.schema.oaf.{Author, DataInfo, Dataset, Instance, KeyValue, Oaf, Relation, StructuredProperty}
import org.json4s.DefaultFormats
import org.json4s.JsonAST.{JField, JObject, JString}
import org.json4s.jackson.JsonMethods.{compact, parse, render}

import scala.collection.JavaConverters._

object BioDBToOAF {

  case class EBILinkItem(id: Long, links: String) {}

  case class EBILinks(relType: String, date: String, title: String, pmid: String, targetPid: String, targetPidType: String, targetUrl: String) {}

  val dataInfo: DataInfo = OafMapperUtils.dataInfo(false, null, false, false, ModelConstants.PROVENANCE_ACTION_SET_QUALIFIER, "0.9")

  val PDB_COLLECTED_FROM: KeyValue = OafMapperUtils.keyValue("10|opendoar____::d1c373ab1570cfb9a7dbb53c186b37a2", "Protein Data Bank")
  val UNIPROT_COLLECTED_FROM: KeyValue = OafMapperUtils.keyValue("10|re3data_____::296e1abaf1302897a6838d3588cd0310", "UniProtKB/Swiss-Prot")
  val SUBJ_CLASS = "Keywords"
  UNIPROT_COLLECTED_FROM.setDataInfo(dataInfo)
  PDB_COLLECTED_FROM.setDataInfo(dataInfo)

  val EBI_COLLECTED_FROM: KeyValue = OafMapperUtils.keyValue("10|opendoar____::83e60e09c222f206c725385f53d7e567c", "EMBL-EBIs Protein Data Bank in Europe (PDBe)")


  case class UniprotDate(date: String, date_info: String) {}

  def uniprotToOAF(input: String): List[Oaf] = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json = parse(input)
    val pid = (json \ "pid").extract[String]

    val d = new Dataset

    d.setPid(
      List(
        OafMapperUtils.structuredProperty(pid, "uniprot", "uniprot", ModelConstants.DNET_PID_TYPES, ModelConstants.DNET_PID_TYPES, dataInfo)
      ).asJava
    )

    d.setDataInfo(dataInfo)
    d.setId(OafMapperUtils.createOpenaireId(50, s"uniprot_____::$pid", true))
    d.setCollectedfrom(List(UNIPROT_COLLECTED_FROM).asJava)

    val title: String = (json \ "title").extractOrElse[String](null)

    if (title != null)
      d.setTitle(List(OafMapperUtils.structuredProperty(title, ModelConstants.MAIN_TITLE_QUALIFIER, dataInfo)).asJava)

    d.setOriginalId(List(pid).asJava)
    val i = new Instance

    i.setPid(d.getPid)
    i.setUrl(List(s"https://www.uniprot.org/uniprot/$pid").asJava)
    i.setInstancetype(OafMapperUtils.qualifier("0046", "Bioentity", ModelConstants.DNET_PUBLICATION_RESOURCE, ModelConstants.DNET_PUBLICATION_RESOURCE))

    i.setCollectedfrom(UNIPROT_COLLECTED_FROM)
    d.setInstance(List(i).asJava)

    val dates: List[UniprotDate] = for {
      JObject(dateOBJ) <- json \ "dates"
      JField("date", JString(date)) <- dateOBJ
      JField("date_info", JString(date_info)) <- dateOBJ
    } yield UniprotDate(date, date_info)

    val subjects: List[String] = (json \\ "subjects").extractOrElse[List[String]](null)


    if (subjects != null) {
      d.setSubject(
        subjects.map(s =>
          OafMapperUtils.structuredProperty(s, SUBJ_CLASS, SUBJ_CLASS, ModelConstants.DNET_SUBJECT_TYPOLOGIES, ModelConstants.DNET_SUBJECT_TYPOLOGIES, null)
        ).asJava)
    }


    if (dates.nonEmpty) {
      val i_date = dates.find(d => d.date_info.contains("entry version"))
      if (i_date.isDefined) {
        i.setDateofacceptance(OafMapperUtils.field(i_date.get.date, dataInfo))
        d.setDateofacceptance(OafMapperUtils.field(i_date.get.date, dataInfo))
      }
      val relevant_dates: List[StructuredProperty] = dates.filter(d => !d.date_info.contains("entry version"))
        .map(date => OafMapperUtils.structuredProperty(date.date, "UNKNOWN", "UNKNOWN", ModelConstants.DNET_DATACITE_DATE, ModelConstants.DNET_DATACITE_DATE, dataInfo))
      if (relevant_dates != null && relevant_dates.nonEmpty)
        d.setRelevantdate(relevant_dates.asJava)
      d.setDateofacceptance(OafMapperUtils.field(i_date.get.date, dataInfo))
    }


    val references_pmid: List[String] = for {
      JObject(reference) <- json \ "references"
      JField("PubMed", JString(pid)) <- reference
    } yield pid

    val references_doi: List[String] = for {
      JObject(reference) <- json \ "references"
      JField(" DOI", JString(pid)) <- reference
    } yield pid


    if (references_pmid != null && references_pmid.nonEmpty) {
      val rel = createRelation(references_pmid.head, "pmid", d.getId, UNIPROT_COLLECTED_FROM, "relationship", "isRelatedTo")
      rel.getCollectedfrom
      List(d, rel)
    }
    else if (references_doi != null && references_doi.nonEmpty) {
      val rel = createRelation(references_doi.head, "doi", d.getId, UNIPROT_COLLECTED_FROM, "relationship", "isRelatedTo")
      List(d, rel)
    }


    else
      List(d)
  }

  def createRelation(pid: String, pidType: String, sourceId: String, collectedFrom: KeyValue, subRelType:String, relClass:String):Relation = {

    val rel = new Relation
    rel.setCollectedfrom(List(PDB_COLLECTED_FROM).asJava)
    rel.setDataInfo(dataInfo)

    rel.setRelType("resultResult")
    rel.setSubRelType(subRelType)
    rel.setRelClass(relClass)

    rel.setSource(sourceId)
    rel.setTarget(s"unresolved::$pid::$pidType")

    rel.getTarget.startsWith("unresolved")
    rel.setCollectedfrom(List(collectedFrom).asJava)
    rel

  }


  def createSupplementaryRelation(pid: String, pidType: String, sourceId: String, collectedFrom: KeyValue): Relation = {
    createRelation(pid,pidType,sourceId,collectedFrom, "supplement","IsSupplementTo")
  }


  def pdbTOOaf(input: String): List[Oaf] = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json = parse(input)
    val pdb = (json \ "pdb").extract[String].toLowerCase

    if (pdb.isEmpty)
      return List()

    val d = new Dataset

    d.setPid(
      List(
        OafMapperUtils.structuredProperty(pdb, "pdb", "Protein Data Bank Identifier", ModelConstants.DNET_PID_TYPES, ModelConstants.DNET_PID_TYPES, dataInfo)
      ).asJava
    )

    d.setCollectedfrom(List(PDB_COLLECTED_FROM).asJava)
    d.setDataInfo(dataInfo)
    d.setId(OafMapperUtils.createOpenaireId(50, s"pdb_________::$pdb", true))
    d.setOriginalId(List(pdb).asJava)

    val title = (json \ "title").extractOrElse[String](null)

    if (title == null)
      return List()
    d.setTitle(List(OafMapperUtils.structuredProperty(title, ModelConstants.MAIN_TITLE_QUALIFIER, dataInfo)).asJava)

    val authors: List[String] = (json \ "authors").extractOrElse[List[String]](null)

    if (authors != null) {
      val convertedAuthors = authors.zipWithIndex.map { a =>

        val res = new Author
        res.setFullname(a._1)
        res.setRank(a._2 + 1)
        res
      }

      d.setAuthor(convertedAuthors.asJava)
    }

    val i = new Instance

    i.setPid(d.getPid)
    i.setUrl(List(s"https://www.rcsb.org/structure/$pdb").asJava)
    i.setInstancetype(OafMapperUtils.qualifier("0046", "Bioentity", ModelConstants.DNET_PUBLICATION_RESOURCE, ModelConstants.DNET_PUBLICATION_RESOURCE))

    i.setCollectedfrom(PDB_COLLECTED_FROM)
    d.setInstance(List(i).asJava)

    val pmid = (json \ "pmid").extractOrElse[String](null)

    if (pmid != null)
      List(d, createSupplementaryRelation(pmid, "pmid", d.getId, PDB_COLLECTED_FROM))
    else
      List(d)
  }


  def extractEBILinksFromDump(input: String): EBILinkItem = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json = parse(input)

    val pmid = (json \ "publication" \ "pmid").extract[String]
    val links = (json \ "links").extract[JObject]
    EBILinkItem(pmid.toLong, compact(render(links)))
  }


  def EBITargetLinksFilter(input: EBILinks): Boolean = {

    input.targetPidType.equalsIgnoreCase("ena") ||   input.targetPidType.equalsIgnoreCase("pdb") ||  input.targetPidType.equalsIgnoreCase("uniprot")

  }


  def parse_ebi_links(input: String): List[EBILinks] = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json = parse(input)
    val pmid = (json \ "request" \ "id").extract[String]
    for {
      JObject(link) <- json \\ "Link"
      JField("Target", JObject(target)) <- link
      JField("RelationshipType", JObject(relType)) <- link
      JField("Name", JString(relation)) <- relType
      JField("PublicationDate", JString(publicationDate)) <- link
      JField("Title", JString(title)) <- target
      JField("Identifier", JObject(identifier)) <- target
      JField("IDScheme", JString(idScheme)) <- identifier
      JField("IDURL", JString(idUrl)) <- identifier
      JField("ID", JString(id)) <- identifier

    } yield EBILinks(relation, publicationDate, title, pmid, id, idScheme, idUrl)
  }


  def convertEBILinksToOaf(input: EBILinks): List[Oaf] = {
    val d = new Dataset
    d.setCollectedfrom(List(EBI_COLLECTED_FROM).asJava)
    d.setDataInfo(dataInfo)
    d.setTitle(List(OafMapperUtils.structuredProperty(input.title, ModelConstants.MAIN_TITLE_QUALIFIER, dataInfo)).asJava)

    val nsPrefix = input.targetPidType.toLowerCase.padTo(12, '_')

    d.setId(OafMapperUtils.createOpenaireId(50, s"$nsPrefix::${input.targetPid.toLowerCase}", true))
    d.setOriginalId(List(input.targetPid.toLowerCase).asJava)


    d.setPid(
      List(
        OafMapperUtils.structuredProperty(input.targetPid.toLowerCase, input.targetPidType.toLowerCase, "Protein Data Bank Identifier", ModelConstants.DNET_PID_TYPES, ModelConstants.DNET_PID_TYPES, dataInfo)
      ).asJava
    )

    val i = new Instance

    i.setPid(d.getPid)
    i.setUrl(List(input.targetUrl).asJava)
    i.setInstancetype(OafMapperUtils.qualifier("0046", "Bioentity", ModelConstants.DNET_PUBLICATION_RESOURCE, ModelConstants.DNET_PUBLICATION_RESOURCE))

    i.setCollectedfrom(EBI_COLLECTED_FROM)
    d.setInstance(List(i).asJava)
    i.setDateofacceptance(OafMapperUtils.field(input.date, dataInfo))
    d.setDateofacceptance(OafMapperUtils.field(input.date, dataInfo))

    List(d, createRelation(input.pmid, "pmid", d.getId, EBI_COLLECTED_FROM,"relationship", "isRelatedTo"))
  }
}
