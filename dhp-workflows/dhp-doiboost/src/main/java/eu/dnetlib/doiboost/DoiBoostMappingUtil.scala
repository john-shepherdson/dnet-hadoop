package eu.dnetlib.doiboost

import eu.dnetlib.dhp.schema.action.AtomicAction
import eu.dnetlib.dhp.schema.oaf.{DataInfo, Dataset, Field, Instance, KeyValue, Oaf, Publication, Qualifier, Relation, Result, StructuredProperty}
import eu.dnetlib.dhp.utils.DHPUtils
import org.apache.commons.lang3.StringUtils
import org.codehaus.jackson.map.ObjectMapper
import org.json4s
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.io.Source


case class HostedByItemType(id: String, officialName: String, issn: String, eissn: String, lissn: String, openAccess: Boolean) {}

case class DoiBoostAffiliation(PaperId:Long, AffiliationId:Long, GridId:String){}

object DoiBoostMappingUtil {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  //STATIC STRING
  val MAG = "microsoft"
  val MAG_NAME = "Microsoft Academic Graph"
  val ORCID = "ORCID"
  val CROSSREF = "Crossref"
  val UNPAYWALL = "UnpayWall"
  val GRID_AC = "grid.ac"
  val WIKPEDIA = "wikpedia"
  val doiBoostNSPREFIX = "doiboost____"
  val OPENAIRE_PREFIX = "openaire____"
  val SEPARATOR = "::"
  val DNET_LANGUAGES = "dnet:languages"
  val PID_TYPES = "dnet:pid_types"

  val invalidName = List(",", "none none", "none, none", "none &na;", "(:null)", "test test test", "test test", "test", "&na; &na;")

  def toActionSet(item:Oaf) :(String, String) = {
    val mapper = new ObjectMapper()

    item match {
      case dataset: Dataset =>
        val a: AtomicAction[Dataset] = new AtomicAction[Dataset]
        a.setClazz(classOf[Dataset])
        a.setPayload(dataset)
        (dataset.getClass.getCanonicalName, mapper.writeValueAsString(a))
      case publication: Publication =>
        val a: AtomicAction[Publication] = new AtomicAction[Publication]
        a.setClazz(classOf[Publication])
        a.setPayload(publication)
        (publication.getClass.getCanonicalName, mapper.writeValueAsString(a))
      case relation: Relation =>
        val a: AtomicAction[Relation] = new AtomicAction[Relation]
        a.setClazz(classOf[Relation])
        a.setPayload(relation)
        (relation.getClass.getCanonicalName, mapper.writeValueAsString(a))
      case _ =>
        null
    }

  }


  def retrieveHostedByMap(): Map[String, HostedByItemType] = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    val jsonMap = Source.fromInputStream(getClass.getResourceAsStream("/eu/dnetlib/dhp/doiboost/hbMap.json")).mkString
    lazy val json: json4s.JValue = parse(jsonMap)
    json.extract[Map[String, HostedByItemType]]
  }

  def retrieveHostedByItem(issn: String, eissn: String, lissn: String, hostedByMap: Map[String, HostedByItemType]): HostedByItemType = {
    if (issn != null && issn.nonEmpty && hostedByMap.contains(issn))
      return hostedByMap(issn)

    if (eissn != null && eissn.nonEmpty && hostedByMap.contains(eissn))
      return hostedByMap(eissn)

    if (lissn != null && lissn.nonEmpty && hostedByMap.contains(lissn))
      return hostedByMap(lissn)

    null

  }

  def generateGridAffiliationId(gridId:String) :String = {
    s"10|grid________::${DHPUtils.md5(gridId.toLowerCase().trim())}"
  }


  def fixResult(result: Dataset) :Dataset = {
    val instanceType = result.getInstance().asScala.find(i => i.getInstancetype != null && i.getInstancetype.getClassid.nonEmpty)
    if (instanceType.isDefined) {
      result.getInstance().asScala.foreach(i => i.setInstancetype(instanceType.get.getInstancetype))
    }
    result.getInstance().asScala.foreach(i => {
      val hb = new KeyValue
      hb.setValue("Unknown Repository")
      hb.setKey(s"10|$OPENAIRE_PREFIX::55045bd2a65019fd8e6741a755395c8c")
      i.setHostedby(hb)
    })
    result
  }

  def fixPublication(publication: Publication, hostedByMap: Map[String, HostedByItemType]): Publication = {
    val issn = if (publication.getJournal == null) null else publication.getJournal.getIssnPrinted
    val eissn =if (publication.getJournal == null) null else  publication.getJournal.getIssnOnline
    val lissn =if (publication.getJournal == null) null else  publication.getJournal.getIssnLinking

    val instanceType = publication.getInstance().asScala.find(i => i.getInstancetype != null && i.getInstancetype.getClassid.nonEmpty)

    if (instanceType.isDefined) {
      publication.getInstance().asScala.foreach(i => i.setInstancetype(instanceType.get.getInstancetype))
    }

    val item = retrieveHostedByItem(issn, eissn, lissn, hostedByMap)
    publication.getInstance().asScala.foreach(i => {
      val hb = new KeyValue
      if (item != null) {
        hb.setValue(item.officialName)
        hb.setKey(generateDSId(item.id))
        if (item.openAccess)
          i.setAccessright(createQualifier("Open", "dnet:access_modes"))
        publication.setBestaccessright(createQualifier("Open", "dnet:access_modes"))
      }
      else {
        hb.setValue("Unknown Repository")
        hb.setKey(s"10|$OPENAIRE_PREFIX::55045bd2a65019fd8e6741a755395c8c")
      }
      i.setHostedby(hb)
    })

    val ar = publication.getInstance().asScala.filter(i => i.getInstancetype != null && i.getAccessright!= null && i.getAccessright.getClassid!= null).map(f=> f.getAccessright.getClassid)
    if (ar.nonEmpty) {
      if(ar.contains("Open")){
        publication.setBestaccessright(createQualifier("Open", "dnet:access_modes"))
      }
      else {
        publication.setBestaccessright(createQualifier(ar.head, "dnet:access_modes"))
      }
    }
    publication
  }


  def generateDSId(input: String): String = {

    val b = StringUtils.substringBefore(input, "::")
    val a = StringUtils.substringAfter(input, "::")
    s"10|${b}::${DHPUtils.md5(a)}"
  }


  def generateDataInfo(): DataInfo = {
    generateDataInfo("0.9")
  }


  def filterPublication(publication: Publication): Boolean = {

    //Case empty publication
    if (publication == null)
      return false

    //Case publication with no title
    if (publication.getTitle == null || publication.getTitle.size == 0)
      return false


    val s = publication.getTitle.asScala.count(p => p.getValue != null
      && p.getValue.nonEmpty && !p.getValue.equalsIgnoreCase("[NO TITLE AVAILABLE]"))

    if (s == 0)
      return false

    // fixes #4360 (test publisher)
    val publisher = if (publication.getPublisher != null) publication.getPublisher.getValue else null

    if (publisher != null && (publisher.equalsIgnoreCase("Test accounts") || publisher.equalsIgnoreCase("CrossRef Test Account"))) {
      return false;
    }

    //Publication with no Author
    if (publication.getAuthor == null || publication.getAuthor.size() == 0)
      return false


    //filter invalid author
    val authors = publication.getAuthor.asScala.map(s => {
      if (s.getFullname.nonEmpty) {
        s.getFullname
      }
      else
        s"${
          s.getName
        } ${
          s.getSurname
        }"
    })

    val c = authors.count(isValidAuthorName)
    if (c == 0)
      return false

    // fixes #4368
    if (authors.count(s => s.equalsIgnoreCase("Addie Jackson")) > 0 && "Elsevier BV".equalsIgnoreCase(publication.getPublisher.getValue))
      return false

    true
  }


  def isValidAuthorName(fullName: String): Boolean = {
    if (fullName == null || fullName.isEmpty)
      return false
    if (invalidName.contains(fullName.toLowerCase.trim))
      return false
    true
  }


  def generateDataInfo(trust: String): DataInfo = {
    val di = new DataInfo
    di.setDeletedbyinference(false)
    di.setInferred(false)
    di.setInvisible(false)
    di.setTrust(trust)
    di.setProvenanceaction(createQualifier("sysimport:actionset", "dnet:provenanceActions"))
    di
  }


  def createSP(value: String, classId: String, schemeId: String): StructuredProperty = {
    val sp = new StructuredProperty
    sp.setQualifier(createQualifier(classId, schemeId))
    sp.setValue(value)
    sp

  }

  def createSP(value: String, classId: String, schemeId: String, dataInfo: DataInfo): StructuredProperty = {
    val sp = new StructuredProperty
    sp.setQualifier(createQualifier(classId, schemeId))
    sp.setValue(value)
    sp.setDataInfo(dataInfo)
    sp

  }

  def createCrossrefCollectedFrom(): KeyValue = {

    val cf = new KeyValue
    cf.setValue(CROSSREF)
    cf.setKey("10|" + OPENAIRE_PREFIX + SEPARATOR + DHPUtils.md5(CROSSREF.toLowerCase))
    cf

  }


  def createUnpayWallCollectedFrom(): KeyValue = {

    val cf = new KeyValue
    cf.setValue(UNPAYWALL)
    cf.setKey("10|" + OPENAIRE_PREFIX + SEPARATOR + DHPUtils.md5(UNPAYWALL.toLowerCase))
    cf

  }

  def createORIDCollectedFrom(): KeyValue = {

    val cf = new KeyValue
    cf.setValue(ORCID)
    cf.setKey("10|" + OPENAIRE_PREFIX + SEPARATOR + DHPUtils.md5(ORCID.toLowerCase))
    cf

  }


  def generateIdentifier (oaf: Result, doi: String): String = {
    val id = DHPUtils.md5 (doi.toLowerCase)
    if (oaf.isInstanceOf[Dataset] )
      return s"60|${
        doiBoostNSPREFIX
      }${
        SEPARATOR
      }${
        id
      }"
    s"50|${
      doiBoostNSPREFIX
    }${
      SEPARATOR
    }${
      id
    }"
  }


  def createMAGCollectedFrom(): KeyValue = {

    val cf = new KeyValue
    cf.setValue(MAG_NAME)
    cf.setKey("10|" + OPENAIRE_PREFIX + SEPARATOR + DHPUtils.md5(MAG))
    cf

  }

  def createQualifier(clsName: String, clsValue: String, schName: String, schValue: String): Qualifier = {
    val q = new Qualifier
    q.setClassid(clsName)
    q.setClassname(clsValue)
    q.setSchemeid(schName)
    q.setSchemename(schValue)
    q
  }

  def createQualifier(cls: String, sch: String): Qualifier = {
    createQualifier(cls, cls, sch, sch)
  }


  def asField[T](value: T): Field[T] = {
    val tmp = new Field[T]
    tmp.setValue(value)
    tmp


  }


}
