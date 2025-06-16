package eu.dnetlib.dhp.sx.bio

import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup
import eu.dnetlib.dhp.schema.common.ModelConstants
import eu.dnetlib.dhp.schema.oaf._
import eu.dnetlib.dhp.schema.oaf.utils.{GraphCleaningFunctions, IdentifierFactory, OafMapperUtils}
import org.json4s.DefaultFormats
import org.json4s.JsonAST.{JField, JObject, JString}
import org.json4s.jackson.JsonMethods.{compact, parse, render}

import java.time.LocalDate
import scala.collection.JavaConverters._

object BioDBToOAF {

  case class EBILinkItem(id: Long, links: String) {}

  case class EBILinks(
    relType: String,
    date: String,
    title: String,
    pmid: String,
    targetPid: String,
    targetPidType: String,
    targetUrl: String
  ) {}

  case class UniprotDate(date: String, date_info: String) {}

  case class ScholixResolved(
    pid: String,
    pidType: String,
    typology: String,
    tilte: List[String],
    datasource: List[String],
    date: List[String],
    authors: List[String]
  ) {}

  val DATA_INFO: DataInfo = OafMapperUtils.dataInfo(
    false,
    null,
    false,
    false,
    ModelConstants.PROVENANCE_ACTION_SET_QUALIFIER,
    "0.9"
  )
  val SUBJ_CLASS = "Keywords"

  val DATE_RELATION_KEY = "RelationDate"

  val resolvedURL: Map[String, String] = Map(
    "genbank"            -> "https://www.ncbi.nlm.nih.gov/nuccore/",
    "ncbi-n"             -> "https://www.ncbi.nlm.nih.gov/nuccore/",
    "ncbi-wgs"           -> "https://www.ncbi.nlm.nih.gov/nuccore/",
    "ncbi-p"             -> "https://www.ncbi.nlm.nih.gov/protein/",
    "ena"                -> "https://www.ebi.ac.uk/ena/browser/view/",
    "clinicaltrials.gov" -> "https://clinicaltrials.gov/ct2/show/",
    "onim"               -> "https://omim.org/entry/",
    "refseq"             -> "https://www.ncbi.nlm.nih.gov/nuccore/",
    "geo"                -> "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc="
  )

  val collectedFromMap: Map[String, KeyValue] = {
    val PDBCollectedFrom: KeyValue = OafMapperUtils.keyValue(
      "10|opendoar____::d1c373ab1570cfb9a7dbb53c186b37a2",
      "Protein Data Bank"
    )
    val enaCollectedFrom: KeyValue = OafMapperUtils.keyValue(
      "10|re3data_____::c2a591f440598b63d854556beaf01591",
      "European Nucleotide Archive"
    )
    val ncbiCollectedFrom: KeyValue = OafMapperUtils.keyValue(
      "10|re3data_____::7d4f90870fe1e493232c9e86c43ae6f6",
      "NCBI Nucleotide"
    )
    val UNIPROTCollectedFrom: KeyValue = OafMapperUtils.keyValue(
      "10|re3data_____::296e1abaf1302897a6838d3588cd0310",
      "UniProtKB/Swiss-Prot"
    )
    val ElsevierCollectedFrom: KeyValue =
      OafMapperUtils.keyValue("10|openaire____::8f87e10869299a5fe80b315695296b88", "Elsevier")
    val springerNatureCollectedFrom: KeyValue = OafMapperUtils.keyValue(
      "10|openaire____::6e380d9cf51138baec8480f5a0ce3a2e",
      "Springer Nature"
    )
    val EBICollectedFrom: KeyValue = OafMapperUtils.keyValue(
      "10|opendoar____::3e60e09c222f206c725385f53d7e567c",
      "EMBL-EBIs Protein Data Bank in Europe (PDBe)"
    )
    val pubmedCollectedFrom: KeyValue =
      OafMapperUtils.keyValue(ModelConstants.EUROPE_PUBMED_CENTRAL_ID, "Europe PubMed Central")

    UNIPROTCollectedFrom.setDataInfo(DATA_INFO)
    PDBCollectedFrom.setDataInfo(DATA_INFO)
    ElsevierCollectedFrom.setDataInfo(DATA_INFO)
    EBICollectedFrom.setDataInfo(DATA_INFO)
    pubmedCollectedFrom.setDataInfo(DATA_INFO)
    enaCollectedFrom.setDataInfo(DATA_INFO)
    ncbiCollectedFrom.setDataInfo(DATA_INFO)
    springerNatureCollectedFrom.setDataInfo(DATA_INFO)

    Map(
      "uniprot"                     -> UNIPROTCollectedFrom,
      "pdb"                         -> PDBCollectedFrom,
      "elsevier"                    -> ElsevierCollectedFrom,
      "ebi"                         -> EBICollectedFrom,
      "Springer Nature"             -> springerNatureCollectedFrom,
      "NCBI Nucleotide"             -> ncbiCollectedFrom,
      "European Nucleotide Archive" -> enaCollectedFrom,
      "Europe PMC"                  -> pubmedCollectedFrom
    )
  }

  def uniprotToOAF(input: String, vocabularies: VocabularyGroup): List[Oaf] = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json = parse(input)
    val pid = (json \ "pid").extract[String]

    val d = new Dataset

    d.setPid(
      List(
        OafMapperUtils.structuredProperty(
          pid,
          "uniprot",
          "uniprot",
          ModelConstants.DNET_PID_TYPES,
          ModelConstants.DNET_PID_TYPES,
          DATA_INFO
        )
      ).asJava
    )

    d.setDataInfo(DATA_INFO)
    d.setId(OafMapperUtils.createOpenaireId(50, s"uniprot_____::$pid", true))
    d.setCollectedfrom(List(collectedFromMap("uniprot")).asJava)

    val title: String = (json \ "title").extractOrElse[String](null)

    if (title != null)
      d.setTitle(
        List(
          OafMapperUtils.structuredProperty(title, ModelConstants.MAIN_TITLE_QUALIFIER, DATA_INFO)
        ).asJava
      )

    d.setOriginalId(List(pid).asJava)
    val i = new Instance

    i.setPid(d.getPid)
    i.setUrl(List(s"https://www.uniprot.org/uniprot/$pid").asJava)
    i.setInstancetype(
      OafMapperUtils.qualifier(
        "0046",
        "Bioentity",
        ModelConstants.DNET_PUBLICATION_RESOURCE,
        ModelConstants.DNET_PUBLICATION_RESOURCE
      )
    )
    val itm = new InstanceTypeMapping
    itm.setOriginalType("Bioentity")
    itm.setVocabularyName(ModelConstants.OPENAIRE_COAR_RESOURCE_TYPES_3_1)
    i.setInstanceTypeMapping(List(itm).asJava)
    i.setCollectedfrom(collectedFromMap("uniprot"))
    d.setInstance(List(i).asJava)

    val dates: List[UniprotDate] = for {
      JObject(dateOBJ)                        <- json \ "dates"
      JField("date", JString(date))           <- dateOBJ
      JField("date_info", JString(date_info)) <- dateOBJ
    } yield UniprotDate(GraphCleaningFunctions.cleanDate(date), date_info)

    val subjects: List[String] = (json \\ "subjects").extractOrElse[List[String]](null)

    if (subjects != null) {
      d.setSubject(
        subjects
          .map(s =>
            OafMapperUtils.subject(
              s,
              SUBJ_CLASS,
              SUBJ_CLASS,
              ModelConstants.DNET_SUBJECT_TYPOLOGIES,
              ModelConstants.DNET_SUBJECT_TYPOLOGIES,
              null
            )
          )
          .asJava
      )
    }
    var i_date: Option[UniprotDate] = None

    if (dates.nonEmpty) {
      i_date = dates.find(d => d.date_info.contains("entry version"))
      if (i_date.isDefined) {
        i.setDateofacceptance(OafMapperUtils.field(i_date.get.date, DATA_INFO))
        d.setDateofacceptance(OafMapperUtils.field(i_date.get.date, DATA_INFO))
      }
      val relevant_dates: List[StructuredProperty] = dates
        .filter(d => !d.date_info.contains("entry version"))
        .map(date =>
          OafMapperUtils.structuredProperty(
            date.date,
            ModelConstants.UNKNOWN,
            ModelConstants.UNKNOWN,
            ModelConstants.DNET_DATACITE_DATE,
            ModelConstants.DNET_DATACITE_DATE,
            DATA_INFO
          )
        )
      if (relevant_dates != null && relevant_dates.nonEmpty)
        d.setRelevantdate(relevant_dates.asJava)
      d.setDateofacceptance(OafMapperUtils.field(i_date.get.date, DATA_INFO))
    }

    val references_pmid: List[String] = for {
      JObject(reference)             <- json \ "references"
      JField("PubMed", JString(pid)) <- reference
    } yield pid

    val references_doi: List[String] = for {
      JObject(reference)          <- json \ "references"
      JField("DOI", JString(pid)) <- reference
    } yield pid
    var relations: List[Relation] = List()
    if (references_pmid != null && references_pmid.nonEmpty) {
      val rel = createRelation(
        references_pmid.head,
        "pmid",
        d.getId,
        collectedFromMap("uniprot"),
        ModelConstants.SUPPLEMENT,
        ModelConstants.IS_SUPPLEMENTED_BY,
        if (i_date.isDefined) i_date.get.date else null
      )
      rel.getCollectedfrom
      relations = relations ::: List(rel)
    }
    if (references_doi != null && references_doi.nonEmpty) {
      val rel = createRelation(
        references_doi.head,
        "doi",
        d.getId,
        collectedFromMap("uniprot"),
        ModelConstants.SUPPLEMENT,
        ModelConstants.IS_SUPPLEMENTED_BY,
        if (i_date.isDefined) i_date.get.date else null
      )
      relations = relations ::: List(rel)
    }
    List(d) ::: relations
  }

  def createRelation(
    pid: String,
    pidType: String,
    sourceId: String,
    collectedFrom: KeyValue,
    subRelType: String,
    relClass: String,
    date: String
  ): Relation = {

    val rel = new Relation
    rel.setCollectedfrom(List(collectedFromMap("pdb")).asJava)
    rel.setDataInfo(DATA_INFO)

    rel.setRelType(ModelConstants.RESULT_RESULT)
    rel.setSubRelType(subRelType)
    rel.setRelClass(relClass)

    rel.setSource(sourceId)
    rel.setTarget(IdentifierFactory.idFromPid("50", pidType, pid, true))
    val dateProps: KeyValue = OafMapperUtils.keyValue(DATE_RELATION_KEY, date)

    rel.setProperties(List(dateProps).asJava)

    rel.setCollectedfrom(List(collectedFrom).asJava)
    rel

  }

  def createSupplementaryRelation(
    pid: String,
    pidType: String,
    sourceId: String,
    collectedFrom: KeyValue,
    date: String
  ): Relation = {
    createRelation(
      pid,
      pidType,
      sourceId,
      collectedFrom,
      ModelConstants.SUPPLEMENT,
      ModelConstants.IS_SUPPLEMENT_TO,
      date
    )
  }

  def pdbTOOaf(input: String, vocabularies: VocabularyGroup): List[Oaf] = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json = parse(input)
    val pdb = (json \ "pdb").extract[String].toLowerCase

    if (pdb.isEmpty)
      return List()

    val d = new Dataset

    d.setPid(
      List(
        OafMapperUtils.structuredProperty(
          pdb,
          "pdb",
          "Protein Data Bank Identifier",
          ModelConstants.DNET_PID_TYPES,
          ModelConstants.DNET_PID_TYPES,
          DATA_INFO
        )
      ).asJava
    )

    d.setDateofcollection(LocalDate.now().toString)
    d.setDateoftransformation(LocalDate.now().toString)
    d.setCollectedfrom(List(collectedFromMap("pdb")).asJava)
    d.setDataInfo(DATA_INFO)
    d.setId(OafMapperUtils.createOpenaireId(50, s"pdb_________::$pdb", true))
    d.setOriginalId(List(pdb).asJava)

    val title = (json \ "title").extractOrElse[String](null)

    if (title == null)
      return List()
    d.setTitle(
      List(
        OafMapperUtils.structuredProperty(
          title.toLowerCase().capitalize,
          ModelConstants.MAIN_TITLE_QUALIFIER,
          DATA_INFO
        )
      ).asJava
    )

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
    val inputDate: String = (json \ "date").extractOrElse[String](null)
    if (inputDate != null) {
      d.setDateofacceptance(OafMapperUtils.field(inputDate, DATA_INFO))
    }

    i.setPid(d.getPid)
    i.setUrl(List(s"https://www.rcsb.org/structure/$pdb").asJava)
    i.setInstancetype(
      OafMapperUtils.qualifier(
        "0046",
        "Bioentity",
        ModelConstants.DNET_PUBLICATION_RESOURCE,
        ModelConstants.DNET_PUBLICATION_RESOURCE
      )
    )
    val itm = new InstanceTypeMapping
    itm.setOriginalType("Bioentity")
    itm.setVocabularyName(ModelConstants.OPENAIRE_COAR_RESOURCE_TYPES_3_1)
    i.setInstanceTypeMapping(List(itm).asJava)
    i.setCollectedfrom(collectedFromMap("pdb"))
    d.setInstance(List(i).asJava)
    var relations: List[Oaf] = List()
    val doi = (json \ "doi").extractOrElse[String](null)

    if (doi != null) {
      relations = relations ::: List(
        createRelation(
          doi,
          "doi",
          d.getId,
          collectedFromMap("pdb"),
          ModelConstants.SUPPLEMENT,
          ModelConstants.IS_SUPPLEMENTED_BY,
          if (inputDate != null) inputDate else null
        )
      )
    }

    val pmid = (json \ "pmid").extractOrElse[String](null)

    if (pmid != null) {
      relations = relations ::: List(
        createRelation(
          pmid,
          "pmid",
          d.getId,
          collectedFromMap("pdb"),
          ModelConstants.SUPPLEMENT,
          ModelConstants.IS_SUPPLEMENTED_BY,
          if (inputDate != null) inputDate else null
        )
      )

    }

    List(d) ::: relations
  }

  def extractEBILinksFromDump(input: String): EBILinkItem = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json = parse(input)

    val pmid = (json \ "publication" \ "pmid").extract[String]
    val links = (json \ "links").extract[JObject]
    EBILinkItem(pmid.toLong, compact(render(links)))
  }

  def EBITargetLinksFilter(input: EBILinks): Boolean = {

    input.targetPidType.equalsIgnoreCase("ena") || input.targetPidType.equalsIgnoreCase(
      "pdb"
    ) || input.targetPidType.equalsIgnoreCase("uniprot")

  }

  def parse_ebi_links(input: String): List[EBILinks] = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json = parse(input)
    val pmid = (json \ "request" \ "id").extract[String]
    for {
      JObject(link)                                       <- json \\ "Link"
      JField("Target", JObject(target))                   <- link
      JField("RelationshipType", JObject(relType))        <- link
      JField("Name", JString(relation))                   <- relType
      JField("PublicationDate", JString(publicationDate)) <- link
      JField("Title", JString(title))                     <- target
      JField("Identifier", JObject(identifier))           <- target
      JField("IDScheme", JString(idScheme))               <- identifier
      JField("IDURL", JString(idUrl))                     <- identifier
      JField("ID", JString(id))                           <- identifier

    } yield EBILinks(
      relation,
      GraphCleaningFunctions.cleanDate(publicationDate),
      title,
      pmid,
      id,
      idScheme,
      idUrl
    )
  }

  def convertEBILinksToOaf(input: EBILinks): List[Oaf] = {
    val d = new Dataset
    d.setCollectedfrom(List(collectedFromMap("ebi")).asJava)
    d.setDataInfo(DATA_INFO)
    d.setTitle(
      List(
        OafMapperUtils.structuredProperty(
          input.title,
          ModelConstants.MAIN_TITLE_QUALIFIER,
          DATA_INFO
        )
      ).asJava
    )

    val nsPrefix = input.targetPidType.toLowerCase.padTo(12, '_')

    d.setId(OafMapperUtils.createOpenaireId(50, s"$nsPrefix::${input.targetPid.toLowerCase}", true))
    d.setOriginalId(List(input.targetPid.toLowerCase).asJava)

    d.setPid(
      List(
        OafMapperUtils.structuredProperty(
          input.targetPid.toLowerCase,
          input.targetPidType.toLowerCase,
          "Protein Data Bank Identifier",
          ModelConstants.DNET_PID_TYPES,
          ModelConstants.DNET_PID_TYPES,
          DATA_INFO
        )
      ).asJava
    )

    val i = new Instance

    i.setPid(d.getPid)
    i.setUrl(List(input.targetUrl).asJava)
    i.setInstancetype(
      OafMapperUtils.qualifier(
        "0046",
        "Bioentity",
        ModelConstants.DNET_PUBLICATION_RESOURCE,
        ModelConstants.DNET_PUBLICATION_RESOURCE
      )
    )
    val itm = new InstanceTypeMapping
    itm.setOriginalType("Bioentity")
    itm.setVocabularyName(ModelConstants.OPENAIRE_COAR_RESOURCE_TYPES_3_1)
    i.setInstanceTypeMapping(List(itm).asJava)

    i.setCollectedfrom(collectedFromMap("ebi"))
    d.setInstance(List(i).asJava)
    i.setDateofacceptance(
      OafMapperUtils.field(GraphCleaningFunctions.cleanDate(input.date), DATA_INFO)
    )
    d.setDateofacceptance(
      OafMapperUtils.field(GraphCleaningFunctions.cleanDate(input.date), DATA_INFO)
    )

    List(
      d,
      createRelation(
        input.pmid,
        "pmid",
        d.getId,
        collectedFromMap("ebi"),
        ModelConstants.RELATIONSHIP,
        ModelConstants.IS_RELATED_TO,
        GraphCleaningFunctions.cleanDate(input.date)
      )
    )
  }
}
