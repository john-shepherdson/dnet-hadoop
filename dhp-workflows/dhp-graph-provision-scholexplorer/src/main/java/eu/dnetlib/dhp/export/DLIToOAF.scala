package eu.dnetlib.dhp.export

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import eu.dnetlib.dhp.common.PacePerson
import eu.dnetlib.dhp.schema.oaf.{Author, DataInfo, Dataset, Field, Instance, KeyValue, Publication, Qualifier, Relation, StructuredProperty}
import eu.dnetlib.dhp.schema.scholexplorer.{DLIDataset, DLIPublication, DLIRelation}
import org.apache.commons.lang3.StringUtils

import scala.collection.JavaConverters._


case class DLIExternalReference(id: String, url: String, sitename: String, label: String, pid: String, classId: String) {}

object DLIToOAF {


  val collectedFromMap: Map[String, KeyValue] = Map(
    "dli_________::r3d100010527" -> generateKeyValue("10|re3data_____::c2a591f440598b63d854556beaf01591", "European Nucleotide Archive"),
    "dli_________::r3d100010255" -> generateKeyValue("10|re3data_____::480d275ed6f9666ee76d6a1215eabf26", "Inter-university Consortium for Political and Social Research"),
    "dli_________::r3d100011868" -> generateKeyValue("10|re3data_____::db814dc656a911b556dba42a331cebe9", "Mendeley Data"),
    "dli_________::elsevier" -> generateKeyValue("10|openaire____::8f87e10869299a5fe80b315695296b88", "Elsevier"),
    "dli_________::openaire" -> generateKeyValue("10|infrastruct_::f66f1bd369679b5b077dcdf006089556", "OpenAIRE"),
    "dli_________::thomsonreuters" -> generateKeyValue("10|openaire____::081b82f96300b6a6e3d282bad31cb6e2", "Crossref"),
    "dli_________::r3d100010216" -> generateKeyValue("10|re3data_____::0fd79429de04343dbbec705d9b5f429f", "4TU.Centre for Research Data"),
    "dli_________::r3d100010134" -> generateKeyValue("10|re3data_____::9633d1e8c4309c833c2c442abeb0cfeb", "PANGAEA"),
    "dli_________::ieee" -> generateKeyValue("10|openaire____::081b82f96300b6a6e3d282bad31cb6e2", "Crossref"),
    "dli_________::r3d100010197" -> generateKeyValue("10|re3data_____::9fd1d79973f7fda60cbe1d82e3819a68", "The Cambridge Structural Database"),
    "dli_________::nature" -> generateKeyValue("10|openaire____::6e380d9cf51138baec8480f5a0ce3a2e", "Springer Nature"),
    "dli_________::datacite" -> generateKeyValue("10|openaire____::9e3be59865b2c1c335d32dae2fe7b254", "Datacite"),
    "dli_________::r3d100010578" -> generateKeyValue("10|re3data_____::c4d751f29a7568011a4c80136b30b444", "IEDA"),
    "dli_________::r3d100010464" -> generateKeyValue("10|re3data_____::23e2a81591099828f6b83a1c83150666", "Research Data Australia"),
    "dli_________::r3d100010327" -> generateKeyValue("10|re3data_____::a644620b81135243dc9acc15d2362246", "Worldwide Protein Data Bank"),
    "dli_________::pubmed" -> generateKeyValue("10|opendoar____::eda80a3d5b344bc40f3bc04f65b7a357", "PubMed Central"),
    "dli_________::europe_pmc__" -> generateKeyValue("10|opendoar____::8b6dd7db9af49e67306feb59a8bdc52c", "Europe PubMed Central"),
    "dli_________::crossref" -> generateKeyValue("10|openaire____::081b82f96300b6a6e3d282bad31cb6e2", "Crossref")
  )


  val relationTypeMapping: Map[String, (String, String)] = Map(
    "IsReferencedBy" -> ("isRelatedTo", "relationship"),
    "References" -> ("isRelatedTo", "relationship"),
    "IsRelatedTo" -> ("isRelatedTo", "relationship"),
    "IsSupplementedBy" -> ("IsSupplementedBy", "supplement"),
    "Cites" -> ("cites", "citation"),
    "Unknown" -> ("isRelatedTo", "relationship"),
    "IsSourceOf" -> ("isRelatedTo", "relationship"),
    "IsCitedBy" -> ("IsCitedBy", "citation"),
    "Reviews" -> ("reviews", "review"),
    "Describes" -> ("isRelatedTo", "relationship"),
    "HasAssociationWith" -> ("isRelatedTo", "relationship")
  )

  val expectecdPidType = List("uniprot", "ena", "chembl", "ncbi-n", "ncbi-p", "genbank", "pdb", "url")


  val filteredURL = List(
    "www.ebi.ac.uk",
    "www.uniprot.org",
    "f1000.com",
    "en.wikipedia.org",
    "flybase.org",
    "www.yeastgenome.org",
    "research.bioinformatics.udel.edu",
    "cancer.sanger.ac.uk",
    "www.iedb.org",
    "www.crd.york.ac.uk",
    "www.wormbase.org",
    "web.expasy.org",
    "www.hal.inserm.fr",
    "sabiork.h-its.org",
    "zfin.org",
    "www.pombase.org",
    "www.guidetopharmacology.org",
    "reactome.org"
  )


  def filterPid(p: StructuredProperty): Boolean = {
    if (expectecdPidType.contains(p.getQualifier.getClassname) && p.getQualifier.getClassname.equalsIgnoreCase("url"))
      if (filteredURL.exists(u => p.getValue.contains(u)))
        return true
      else
        return false
    expectecdPidType.contains(p.getQualifier.getClassname)
  }


  def extractTitle(titles: java.util.List[StructuredProperty]): String = {

    if (titles == null)
      return null

    val label = titles.asScala.map(p => p.getValue).find(p => p.nonEmpty)
    label.orNull
  }

  def convertDLIDatasetToExternalReference(dataset: DLIDataset): DLIExternalReference = {
    val currentId = generateId(dataset.getId)
    val pids = dataset.getPid.asScala.filter(filterPid)

    if (pids == null || pids.isEmpty)
      return null

    val pid: StructuredProperty = pids.head


    pid.getQualifier.getClassname match {
      case "uniprot" => DLIExternalReference(generateId(dataset.getId), s"https://www.uniprot.org/uniprot/${pid.getValue}", "UniProt", extractTitle(dataset.getTitle), pid.getValue, "accessionNumber")
      case "ena" =>
        if(pid.getValue!= null && pid.getValue.nonEmpty && pid.getValue.length>7)
          DLIExternalReference(generateId(dataset.getId), s"https://www.ebi.ac.uk/ena/data/view/${pid.getValue.substring(0, 8)}", "European Nucleotide Archive", extractTitle(dataset.getTitle), pid.getValue, "accessionNumber")
        else
          null
      case "chembl" => DLIExternalReference(generateId(dataset.getId), s"https://www.ebi.ac.uk/chembl/compound_report_card/${pid.getValue}", "ChEMBL", extractTitle(dataset.getTitle), pid.getValue, "accessionNumber")
      case "ncbi-n" => DLIExternalReference(generateId(dataset.getId), s"https://www.ncbi.nlm.nih.gov/nuccore/${pid.getValue}", "Nucleotide Database", extractTitle(dataset.getTitle), pid.getValue, "accessionNumber")
      case "ncbi-p" => DLIExternalReference(generateId(dataset.getId), s"https://www.ncbi.nlm.nih.gov/nuccore/${pid.getValue}", "Nucleotide Database", extractTitle(dataset.getTitle), pid.getValue, "accessionNumber")
      case "genbank" => DLIExternalReference(generateId(dataset.getId), s"https://www.ncbi.nlm.nih.gov/nuccore/${pid.getValue}", "GenBank", extractTitle(dataset.getTitle), pid.getValue, "accessionNumber")
      case "pdb" => DLIExternalReference(generateId(dataset.getId), s"https://www.ncbi.nlm.nih.gov/nuccore/${pid.getValue}", "Protein Data Bank", extractTitle(dataset.getTitle), pid.getValue, "accessionNumber")
      case "url" => DLIExternalReference(generateId(dataset.getId), pid.getValue, "", extractTitle(dataset.getTitle), pid.getValue, "url")

    }


  }


  def convertDLIPublicationToOAF(p: DLIPublication): Publication = {

    val result = new Publication
    result.setId(generateId(p.getId))
    result.setDataInfo(generateDataInfo(invisibile = true))
    if (p.getCollectedfrom == null || p.getCollectedfrom.size() == 0 || (p.getCollectedfrom.size() == 1 && p.getCollectedfrom.get(0) == null))
      return null

    result.setCollectedfrom(p.getCollectedfrom.asScala.map(c => collectedFromMap.getOrElse(c.getKey, null)).asJava)
    result.setPid(p.getPid)
    result.setDateofcollection(p.getDateofcollection)
    result.setOriginalId(p.getPid.asScala.map(p => p.getValue).asJava)
    result.setDateoftransformation(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")))
    if (p.getAuthor == null || p.getAuthor.isEmpty)
      return null
    result.setAuthor(p.getAuthor.asScala.map(convertAuthor).asJava)
    result.setResulttype(createQualifier(p.getResulttype.getClassid, p.getResulttype.getClassname, "dnet:result_typologies", "dnet:result_typologies"))

    if (p.getSubject != null)
      result.setSubject(p.getSubject.asScala.map(convertSubject).asJava)

    if (p.getTitle == null || p.getTitle.isEmpty)
      return null

    result.setTitle(List(patchTitle(p.getTitle.get(0))).asJava)

    if (p.getRelevantdate == null || p.getRelevantdate.size() == 0)
      return null

    result.setRelevantdate(p.getRelevantdate.asScala.map(patchRelevantDate).asJava)


    result.setDescription(p.getDescription)

    result.setDateofacceptance(asField(p.getRelevantdate.get(0).getValue))
    result.setPublisher(p.getPublisher)
    result.setSource(p.getSource)
    result.setBestaccessright(createQualifier("UNKNOWN", "not available", "dnet:access_modes", "dnet:access_modes"))

    val dois = result.getPid.asScala.filter(p => "doi".equalsIgnoreCase(p.getQualifier.getClassname)).map(p => p.getValue)
    if (dois.isEmpty)
      return null


    val i: Instance = createInstance(s"https://dx.doi.org/${dois.head}", firstInstanceOrNull(p.getInstance()), result.getDateofacceptance)

    if (i != null)
      result.setInstance(List(i).asJava)

    result
  }


  def convertDLIRelation(r: DLIRelation): Relation = {

    val result = new Relation
    if (!relationTypeMapping.contains(r.getRelType))
      return null

    if (r.getCollectedFrom == null || r.getCollectedFrom.size() == 0 || (r.getCollectedFrom.size() == 1 && r.getCollectedFrom.get(0) == null))
      return null
    val t = relationTypeMapping.get(r.getRelType)

    result.setRelType("resultResult")
    result.setRelClass(t.get._1)
    result.setSubRelType(t.get._2)
    result.setCollectedfrom(r.getCollectedFrom.asScala.map(c => collectedFromMap.getOrElse(c.getKey, null)).filter(p => p != null).asJava)
    result.setSource(generateId(r.getSource))
    result.setTarget(generateId(r.getTarget))

    if (result.getSource.equals(result.getTarget))
      return null
    result.setDataInfo(generateDataInfo())

    result
  }


  def convertDLIDatasetTOOAF(d: DLIDataset): Dataset = {

    if (d.getCollectedfrom == null || d.getCollectedfrom.size() == 0 || (d.getCollectedfrom.size() == 1 && d.getCollectedfrom.get(0) == null))
      return null
    val result: Dataset = new Dataset
    result.setId(generateId(d.getId))
    result.setDataInfo(generateDataInfo())
    result.setCollectedfrom(d.getCollectedfrom.asScala.map(c => collectedFromMap.getOrElse(c.getKey, null)).asJava)


    result.setPid(d.getPid)

    val fpids = result.getPid.asScala.filter(p => "doi".equalsIgnoreCase(p.getQualifier.getClassname) ||
      "pdb".equalsIgnoreCase(p.getQualifier.getClassname)
    ).map(p => p.getValue)

    if (fpids == null || fpids.isEmpty)
      return null


    result.setDateofcollection(d.getDateofcollection)
    result.setOriginalId(d.getPid.asScala.map(d => d.getValue).asJava)
    result.setDateoftransformation(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")))
    if (d.getAuthor == null || d.getAuthor.isEmpty)
      return null
    result.setAuthor(d.getAuthor.asScala.map(convertAuthor).asJava)
    result.setResulttype(createQualifier(d.getResulttype.getClassid, d.getResulttype.getClassname, "dnet:result_typologies", "dnet:result_typologies"))

    if (d.getSubject != null)
      result.setSubject(d.getSubject.asScala.map(convertSubject).asJava)

    if (d.getTitle == null || d.getTitle.isEmpty)
      return null

    result.setTitle(List(patchTitle(d.getTitle.get(0))).asJava)

    if (d.getRelevantdate == null || d.getRelevantdate.size() == 0)
      return null

    result.setRelevantdate(d.getRelevantdate.asScala.map(patchRelevantDate).asJava)


    result.setDescription(d.getDescription)

    result.setDateofacceptance(asField(d.getRelevantdate.get(0).getValue))
    result.setPublisher(d.getPublisher)
    result.setSource(d.getSource)
    result.setBestaccessright(createQualifier("UNKNOWN", "not available", "dnet:access_modes", "dnet:access_modes"))


    val instance_urls = if (fpids.head.length < 5) s"https://www.rcsb.org/structure/${fpids.head}" else s"https://dx.doi.org/${fpids.head}"

    val i: Instance = createInstance(instance_urls, firstInstanceOrNull(d.getInstance()), result.getDateofacceptance, true)
    if (i != null)
      result.setInstance(List(i).asJava)

    result
  }


  def firstInstanceOrNull(instances: java.util.List[Instance]): Instance = {

    if (instances == null || instances.size() == 0)
      return null
    instances.get(0)

  }


  def createInstance(url: String, originalInstance: Instance, doa: Field[String], dataset: Boolean = false): Instance = {

    val i = new Instance
    i.setUrl(List(url).asJava)
    if (dataset)
      i.setInstancetype(createQualifier("0021", "Dataset", "dnet:publication_resource", "dnet:publication_resource"))
    else
      i.setInstancetype(createQualifier("0000", "UNKNOWN", "dnet:publication_resource", "dnet:publication_resource"))
    if (originalInstance != null && originalInstance.getHostedby != null)
      i.setHostedby(originalInstance.getHostedby)

    i.setAccessright(createQualifier("UNKNOWN", "not available", "dnet:access_modes", "dnet:access_modes"))
    i.setDateofacceptance(doa)

    i


  }


  def patchRelevantDate(d: StructuredProperty): StructuredProperty = {
    d.setQualifier(createQualifier("UNKNOWN", "dnet:dataCite_date"))
    d

  }

  def patchTitle(t: StructuredProperty): StructuredProperty = {
    t.setQualifier(createQualifier("main title", "dnet:dataCite_title"))
    t
  }


  def convertSubject(s: StructuredProperty): StructuredProperty = {
    s.setQualifier(createQualifier("keyword", "dnet:subject_classification_typologies"))
    s


  }


  def convertAuthor(a: Author): Author = {
    if (a == null)
      return a
    val p = new PacePerson(a.getFullname, false)
    if (p.isAccurate) {
      a.setName(p.getNameString)
      a.setSurname(p.getSurnameString)
    }
    a
  }


  def generateId(id: String): String = {
    val md5 = if (id.contains("::")) StringUtils.substringAfter(id, "::") else StringUtils.substringAfter(id, "|")
    s"50|scholix_____::$md5"
  }


  def generateKeyValue(key: String, value: String): KeyValue = {
    val kv: KeyValue = new KeyValue()
    kv.setKey(key)
    kv.setValue(value)
    kv.setDataInfo(generateDataInfo("0.9"))
    kv
  }


  def generateDataInfo(trust: String = "0.9", invisibile: Boolean = false): DataInfo = {
    val di = new DataInfo
    di.setDeletedbyinference(false)
    di.setInferred(false)
    di.setInvisible(false)
    di.setTrust(trust)
    di.setProvenanceaction(createQualifier("sysimport:actionset", "dnet:provenanceActions"))
    di
  }

  def createQualifier(cls: String, sch: String): Qualifier = {
    createQualifier(cls, cls, sch, sch)
  }


  def createQualifier(classId: String, className: String, schemeId: String, schemeName: String): Qualifier = {
    val q: Qualifier = new Qualifier
    q.setClassid(classId)
    q.setClassname(className)
    q.setSchemeid(schemeId)
    q.setSchemename(schemeName)
    q
  }


  def asField[T](value: T): Field[T] = {
    val tmp = new Field[T]
    tmp.setValue(value)
    tmp


  }

}
