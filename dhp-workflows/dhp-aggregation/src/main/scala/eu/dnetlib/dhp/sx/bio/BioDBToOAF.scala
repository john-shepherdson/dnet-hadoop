package eu.dnetlib.dhp.sx.bio

import eu.dnetlib.dhp.schema.common.ModelConstants
import eu.dnetlib.dhp.schema.oaf.utils.{GraphCleaningFunctions, OafMapperUtils}
import eu.dnetlib.dhp.schema.oaf._
import org.json4s.DefaultFormats
import org.json4s.JsonAST.{JField, JObject, JString}
import org.json4s.jackson.JsonMethods.{compact, parse, render}
import collection.JavaConverters._

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

  def crossrefLinksToOaf(input: String): Oaf = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json = parse(input)
    val source_pid = (json \ "Source" \ "Identifier" \ "ID").extract[String].toLowerCase
    val source_pid_type = (json \ "Source" \ "Identifier" \ "IDScheme").extract[String].toLowerCase

    val target_pid = (json \ "Target" \ "Identifier" \ "ID").extract[String].toLowerCase
    val target_pid_type = (json \ "Target" \ "Identifier" \ "IDScheme").extract[String].toLowerCase

    val relation_semantic = (json \ "RelationshipType" \ "Name").extract[String]

    val date = GraphCleaningFunctions.cleanDate((json \ "LinkedPublicationDate").extract[String])

    createRelation(
      target_pid,
      target_pid_type,
      generate_unresolved_id(source_pid, source_pid_type),
      collectedFromMap("elsevier"),
      "relationship",
      relation_semantic,
      date
    )

  }

  def scholixResolvedToOAF(input: ScholixResolved): Oaf = {

    val d = new Dataset

    d.setPid(
      List(
        OafMapperUtils.structuredProperty(
          input.pid.toLowerCase,
          input.pidType.toLowerCase,
          input.pidType.toLowerCase,
          ModelConstants.DNET_PID_TYPES,
          ModelConstants.DNET_PID_TYPES,
          DATA_INFO
        )
      ).asJava
    )

    d.setDataInfo(DATA_INFO)

    val nsPrefix = input.pidType.toLowerCase.padTo(12, '_')
    d.setId(OafMapperUtils.createOpenaireId(50, s"$nsPrefix::${input.pid.toLowerCase}", true))

    if (input.tilte != null && input.tilte.nonEmpty)
      d.setTitle(
        List(
          OafMapperUtils.structuredProperty(
            input.tilte.head,
            ModelConstants.MAIN_TITLE_QUALIFIER,
            DATA_INFO
          )
        ).asJava
      )

    d.setOriginalId(List(input.pid).asJava)
    val i = new Instance

    i.setPid(d.getPid)

    if (resolvedURL.contains(input.pidType)) {
      i.setUrl(List(s"${resolvedURL(input.pidType)}${input.pid}").asJava)
    }

    if (input.pidType.equalsIgnoreCase("clinicaltrials.gov"))
      i.setInstancetype(
        OafMapperUtils.qualifier(
          "0037",
          "Clinical Trial",
          ModelConstants.DNET_PUBLICATION_RESOURCE,
          ModelConstants.DNET_PUBLICATION_RESOURCE
        )
      )
    else
      i.setInstancetype(
        OafMapperUtils.qualifier(
          "0046",
          "Bioentity",
          ModelConstants.DNET_PUBLICATION_RESOURCE,
          ModelConstants.DNET_PUBLICATION_RESOURCE
        )
      )

    if (input.datasource == null || input.datasource.isEmpty)
      return null

    val ds = input.datasource.head
    d.setCollectedfrom(List(collectedFromMap(ds)).asJava)
    i.setCollectedfrom(collectedFromMap(ds))
    d.setInstance(List(i).asJava)

    if (input.authors != null && input.authors.nonEmpty) {
      val authors = input.authors.map(a => {
        val authorOAF = new Author
        authorOAF.setFullname(a)
        authorOAF
      })
      d.setAuthor(authors.asJava)
    }
    if (input.date != null && input.date.nonEmpty) {
      val dt = input.date.head
      i.setDateofacceptance(OafMapperUtils.field(GraphCleaningFunctions.cleanDate(dt), DATA_INFO))
      d.setDateofacceptance(OafMapperUtils.field(GraphCleaningFunctions.cleanDate(dt), DATA_INFO))
    }
    d
  }

  def uniprotToOAF(input: String): List[Oaf] = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json = parse(input)
    val pid = (json \ "pid").extract[String].trim()

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
            OafMapperUtils.structuredProperty(
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
      JObject(reference)           <- json \ "references"
      JField(" DOI", JString(pid)) <- reference
    } yield pid

    if (references_pmid != null && references_pmid.nonEmpty) {
      val rel = createRelation(
        references_pmid.head,
        "pmid",
        d.getId,
        collectedFromMap("uniprot"),
        ModelConstants.RELATIONSHIP,
        ModelConstants.IS_RELATED_TO,
        if (i_date.isDefined) i_date.get.date else null
      )
      rel.getCollectedfrom
      List(d, rel)
    } else if (references_doi != null && references_doi.nonEmpty) {
      val rel = createRelation(
        references_doi.head,
        "doi",
        d.getId,
        collectedFromMap("uniprot"),
        ModelConstants.RELATIONSHIP,
        ModelConstants.IS_RELATED_TO,
        if (i_date.isDefined) i_date.get.date else null
      )
      List(d, rel)
    } else
      List(d)
  }

  def generate_unresolved_id(pid: String, pidType: String): String = {
    s"unresolved::$pid::$pidType"
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
    rel.setTarget(s"unresolved::$pid::$pidType")

    val dateProps: KeyValue = OafMapperUtils.keyValue(DATE_RELATION_KEY, date)

    rel.setProperties(List(dateProps).asJava)

    rel.getTarget.startsWith("unresolved")
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

  def pdbTOOaf(input: String): List[Oaf] = {
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

    d.setCollectedfrom(List(collectedFromMap("pdb")).asJava)
    d.setDataInfo(DATA_INFO)
    d.setId(OafMapperUtils.createOpenaireId(50, s"pdb_________::$pdb", true))
    d.setOriginalId(List(pdb).asJava)

    val title = (json \ "title").extractOrElse[String](null)

    if (title == null)
      return List()
    d.setTitle(
      List(
        OafMapperUtils.structuredProperty(title, ModelConstants.MAIN_TITLE_QUALIFIER, DATA_INFO)
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

    i.setCollectedfrom(collectedFromMap("pdb"))
    d.setInstance(List(i).asJava)

    val pmid = (json \ "pmid").extractOrElse[String](null)

    if (pmid != null)
      List(d, createSupplementaryRelation(pmid, "pmid", d.getId, collectedFromMap("pdb"), null))
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
