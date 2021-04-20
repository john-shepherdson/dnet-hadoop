package eu.dnetlib.dhp.actionmanager.datacite

import com.fasterxml.jackson.databind.ObjectMapper
import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup
import eu.dnetlib.dhp.schema.action.AtomicAction
import eu.dnetlib.dhp.schema.common.ModelConstants
import eu.dnetlib.dhp.schema.oaf.{Author, DataInfo, Instance, KeyValue, Oaf, OafMapperUtils, OtherResearchProduct, Publication, Qualifier, Relation, Result, Software, StructuredProperty, Dataset => OafDataset}
import eu.dnetlib.dhp.schema.common.ModelConstants
import eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory
import eu.dnetlib.dhp.schema.oaf.{AccessRight, Author, DataInfo, Instance, KeyValue, Oaf, OafMapperUtils, OtherResearchProduct, Publication, Qualifier, Relation, Result, Software, StructuredProperty, Dataset => OafDataset}
import eu.dnetlib.dhp.utils.DHPUtils
import org.apache.commons.lang3.StringUtils
import org.json4s.DefaultFormats
import org.json4s.JsonAST.{JField, JObject, JString}
import org.json4s.jackson.JsonMethods.parse

import java.nio.charset.CodingErrorAction
import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.{Date, Locale}
import java.util.regex.Pattern
import scala.collection.JavaConverters._
import scala.io.{Codec, Source}

case class DataciteType(doi: String, timestamp: Long, isActive: Boolean, json: String) {}

case class NameIdentifiersType(nameIdentifierScheme: Option[String], schemeUri: Option[String], nameIdentifier: Option[String]) {}

case class CreatorType(nameType: Option[String], nameIdentifiers: Option[List[NameIdentifiersType]], name: Option[String], familyName: Option[String], givenName: Option[String], affiliation: Option[List[String]]) {}

case class TitleType(title: Option[String], titleType: Option[String], lang: Option[String]) {}

case class SubjectType(subject: Option[String], subjectScheme: Option[String]) {}

case class DescriptionType(descriptionType: Option[String], description: Option[String]) {}

case class FundingReferenceType(funderIdentifierType: Option[String], awardTitle: Option[String], awardUri: Option[String], funderName: Option[String], funderIdentifier: Option[String], awardNumber: Option[String]) {}

case class DateType(date: Option[String], dateType: Option[String]) {}

case class HostedByMapType(openaire_id: String, datacite_name: String, official_name: String, similarity: Option[Float]) {}

object DataciteToOAFTransformation {

  implicit val codec: Codec = Codec("UTF-8")
  codec.onMalformedInput(CodingErrorAction.REPLACE)
  codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

  val DOI_CLASS = "doi"
  val SUBJ_CLASS = "keywords"


  val j_filter: List[String] = {
    val s = Source.fromInputStream(getClass.getResourceAsStream("datacite_filter")).mkString
    s.lines.toList
  }

  val mapper = new ObjectMapper()
  val unknown_repository: HostedByMapType = HostedByMapType(ModelConstants.UNKNOWN_REPOSITORY_ORIGINALID, ModelConstants.UNKNOWN_REPOSITORY.getValue, ModelConstants.UNKNOWN_REPOSITORY.getValue, Some(1.0F))

  val dataInfo: DataInfo = generateDataInfo("0.9")
  val DATACITE_COLLECTED_FROM: KeyValue = OafMapperUtils.keyValue(ModelConstants.DATACITE_ID, "Datacite")

  val hostedByMap: Map[String, HostedByMapType] = {
    val s = Source.fromInputStream(getClass.getResourceAsStream("hostedBy_map.json")).mkString
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json: org.json4s.JValue = parse(s)
    json.extract[Map[String, HostedByMapType]]
  }

  val df_en: DateTimeFormatter = DateTimeFormatter.ofPattern("[MM-dd-yyyy][MM/dd/yyyy][dd-MM-yy][dd-MMM-yyyy][dd/MMM/yyyy][dd-MMM-yy][dd/MMM/yy][dd-MM-yy][dd/MM/yy][dd-MM-yyyy][dd/MM/yyyy][yyyy-MM-dd][yyyy/MM/dd]", Locale.ENGLISH)
  val df_it: DateTimeFormatter = DateTimeFormatter.ofPattern("[dd-MM-yyyy][dd/MM/yyyy]", Locale.ITALIAN)

  val funder_regex: List[(Pattern, String)] = List(
    (Pattern.compile("(info:eu-repo/grantagreement/ec/h2020/)(\\d\\d\\d\\d\\d\\d)(.*)", Pattern.MULTILINE | Pattern.CASE_INSENSITIVE), "40|corda__h2020::"),
    (Pattern.compile("(info:eu-repo/grantagreement/ec/fp7/)(\\d\\d\\d\\d\\d\\d)(.*)", Pattern.MULTILINE | Pattern.CASE_INSENSITIVE), "40|corda_______::")

  )

  val Date_regex: List[Pattern] = List(
    //Y-M-D
    Pattern.compile("(18|19|20)\\d\\d([- /.])(0[1-9]|1[012])\\2(0[1-9]|[12][0-9]|3[01])", Pattern.MULTILINE),
    //M-D-Y
    Pattern.compile("((0[1-9]|1[012])|([1-9]))([- /.])(0[1-9]|[12][0-9]|3[01])([- /.])(18|19|20)?\\d\\d", Pattern.MULTILINE),
    //D-M-Y
    Pattern.compile("(?:(?:31(/|-|\\.)(?:0?[13578]|1[02]|(?:Jan|Mar|May|Jul|Aug|Oct|Dec)))\\1|(?:(?:29|30)(/|-|\\.)(?:0?[1,3-9]|1[0-2]|(?:Jan|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec))\\2))(?:(?:1[6-9]|[2-9]\\d)?\\d{2})|(?:29(/|-|\\.)(?:0?2|(?:Feb))\\3(?:(?:(?:1[6-9]|[2-9]\\d)?(?:0[48]|[2468][048]|[13579][26])|(?:(?:16|[2468][048]|[3579][26])00))))|(?:0?[1-9]|1\\d|2[0-8])(/|-|\\.)(?:(?:0?[1-9]|(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep))|(?:1[0-2]|(?:Oct|Nov|Dec)))\\4(?:(?:1[6-9]|[2-9]\\d)?\\d{2})", Pattern.MULTILINE),
    //Y
    Pattern.compile("(19|20)\\d\\d", Pattern.MULTILINE)
  )


  def filter_json(json: String): Boolean = {
    j_filter.exists(f => json.contains(f))
  }

  def toActionSet(item: Oaf): (String, String) = {
    val mapper = new ObjectMapper()

    item match {
      case dataset: OafDataset =>
        val a: AtomicAction[OafDataset] = new AtomicAction[OafDataset]
        a.setClazz(classOf[OafDataset])
        a.setPayload(dataset)
        (dataset.getClass.getCanonicalName, mapper.writeValueAsString(a))
      case publication: Publication =>
        val a: AtomicAction[Publication] = new AtomicAction[Publication]
        a.setClazz(classOf[Publication])
        a.setPayload(publication)
        (publication.getClass.getCanonicalName, mapper.writeValueAsString(a))
      case software: Software =>
        val a: AtomicAction[Software] = new AtomicAction[Software]
        a.setClazz(classOf[Software])
        a.setPayload(software)
        (software.getClass.getCanonicalName, mapper.writeValueAsString(a))
      case orp: OtherResearchProduct =>
        val a: AtomicAction[OtherResearchProduct] = new AtomicAction[OtherResearchProduct]
        a.setClazz(classOf[OtherResearchProduct])
        a.setPayload(orp)
        (orp.getClass.getCanonicalName, mapper.writeValueAsString(a))

      case relation: Relation =>
        val a: AtomicAction[Relation] = new AtomicAction[Relation]
        a.setClazz(classOf[Relation])
        a.setPayload(relation)
        (relation.getClass.getCanonicalName, mapper.writeValueAsString(a))
      case _ =>
        null
    }

  }


  def embargo_end(embargo_end_date: String): Boolean = {
    val dt = LocalDate.parse(embargo_end_date, DateTimeFormatter.ofPattern("[yyyy-MM-dd]"))
    val td = LocalDate.now()
    td.isAfter(dt)
  }


  def extract_date(input: String): Option[String] = {
    val d = Date_regex.map(pattern => {
      val matcher = pattern.matcher(input)
      if (matcher.find())
        matcher.group(0)
      else
        null
    }
    ).find(s => s != null)

    if (d.isDefined) {
      val a_date = if (d.get.length == 4) s"01-01-${d.get}" else d.get
      try {
        return Some(LocalDate.parse(a_date, df_en).toString)
      } catch {
        case _: Throwable => try {
          return Some(LocalDate.parse(a_date, df_it).toString)
        } catch {
          case _: Throwable =>
            return None
        }
      }
    }
    d
  }

  def getTypeQualifier(resourceType: String, resourceTypeGeneral: String, schemaOrg: String, vocabularies: VocabularyGroup): (Qualifier, Qualifier) = {
    if (resourceType != null && resourceType.nonEmpty) {
      val typeQualifier = vocabularies.getSynonymAsQualifier(ModelConstants.DNET_PUBLICATION_RESOURCE, resourceType)
      if (typeQualifier != null)
        return (typeQualifier, vocabularies.getSynonymAsQualifier(ModelConstants.DNET_RESULT_TYPOLOGIES, typeQualifier.getClassid))
    }
    if (schemaOrg != null && schemaOrg.nonEmpty) {
      val typeQualifier = vocabularies.getSynonymAsQualifier(ModelConstants.DNET_PUBLICATION_RESOURCE, schemaOrg)
      if (typeQualifier != null)
        return (typeQualifier, vocabularies.getSynonymAsQualifier(ModelConstants.DNET_RESULT_TYPOLOGIES, typeQualifier.getClassid))

    }
    if (resourceTypeGeneral != null && resourceTypeGeneral.nonEmpty) {
      val typeQualifier = vocabularies.getSynonymAsQualifier(ModelConstants.DNET_PUBLICATION_RESOURCE, resourceTypeGeneral)
      if (typeQualifier != null)
        return (typeQualifier, vocabularies.getSynonymAsQualifier(ModelConstants.DNET_RESULT_TYPOLOGIES, typeQualifier.getClassid))

    }
    null
  }


  def getResult(resourceType: String, resourceTypeGeneral: String, schemaOrg: String, vocabularies: VocabularyGroup): Result = {
    val typeQualifiers: (Qualifier, Qualifier) = getTypeQualifier(resourceType, resourceTypeGeneral, schemaOrg, vocabularies)
    if (typeQualifiers == null)
      return null
    val i = new Instance
    i.setInstancetype(typeQualifiers._1)
    typeQualifiers._2.getClassname match {
      case "dataset" =>
        val r = new OafDataset
        r.setInstance(List(i).asJava)
        return r
      case "publication" =>
        val r = new Publication
        r.setInstance(List(i).asJava)
        return r
      case "software" =>
        val r = new Software
        r.setInstance(List(i).asJava)
        return r
      case "other" =>
        val r = new OtherResearchProduct
        r.setInstance(List(i).asJava)
        return r
    }
    null
  }


  def available_date(input: String): Boolean = {

    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json: org.json4s.JValue = parse(input)
    val l: List[String] = for {
      JObject(dates) <- json \\ "dates"
      JField("dateType", JString(dateTypes)) <- dates
    } yield dateTypes

    l.exists(p => p.equalsIgnoreCase("available"))

  }


  /**
   * As describe in ticket #6377
   * when the result come from figshare we need to remove subject
   * and set Access rights OPEN.
   * @param r
   */
  def fix_figshare(r: Result): Unit = {

    if (r.getInstance() != null) {
      val hosted_by_figshare = r.getInstance().asScala.exists(i => i.getHostedby != null && "figshare".equalsIgnoreCase(i.getHostedby.getValue))
      if (hosted_by_figshare) {
        r.getInstance().asScala.foreach(i => i.setAccessright(ModelConstants.OPEN_ACCESS_RIGHT()))
        val l: List[StructuredProperty] = List()
        r.setSubject(l.asJava)
      }
    }


  }

  def generateOAFDate(dt: String, q: Qualifier): StructuredProperty = {
    OafMapperUtils.structuredProperty(dt, q, null)
  }

  def generateRelation(sourceId: String, targetId: String, relClass: String, cf: KeyValue, di: DataInfo): Relation = {

    val r = new Relation
    r.setSource(sourceId)
    r.setTarget(targetId)
    r.setRelType(ModelConstants.RESULT_PROJECT)
    r.setRelClass(relClass)
    r.setSubRelType(ModelConstants.OUTCOME)
    r.setCollectedfrom(List(cf).asJava)
    r.setDataInfo(di)
    r


  }

  def get_projectRelation(awardUri: String, sourceId: String): List[Relation] = {
    val match_pattern = funder_regex.find(s => s._1.matcher(awardUri).find())

    if (match_pattern.isDefined) {
      val m = match_pattern.get._1
      val p = match_pattern.get._2
      val grantId = m.matcher(awardUri).replaceAll("$2")
      val targetId = s"$p${DHPUtils.md5(grantId)}"
      List(
        generateRelation(sourceId, targetId, "isProducedBy", DATACITE_COLLECTED_FROM, dataInfo),
        generateRelation(targetId, sourceId, "produces", DATACITE_COLLECTED_FROM, dataInfo)
      )
    }
    else
      List()

  }


  def generateOAF(input: String, ts: Long, dateOfCollection: Long, vocabularies: VocabularyGroup): List[Oaf] = {
    if (filter_json(input))
      return List()

    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json = parse(input)

    val resourceType = (json \ "attributes" \ "types" \ "resourceType").extractOrElse[String](null)
    val resourceTypeGeneral = (json \ "attributes" \ "types" \ "resourceTypeGeneral").extractOrElse[String](null)
    val schemaOrg = (json \ "attributes" \ "types" \ "schemaOrg").extractOrElse[String](null)

    val doi = (json \ "attributes" \ "doi").extract[String]
    if (doi.isEmpty)
      return List()

    //Mapping type based on vocabularies dnet:publication_resource and dnet:result_typologies
    val result = getResult(resourceType, resourceTypeGeneral, schemaOrg, vocabularies)
    if (result == null)
      return List()


    val doi_q = OafMapperUtils.qualifier("doi", "doi", ModelConstants.DNET_PID_TYPES, ModelConstants.DNET_PID_TYPES)
    val pid = OafMapperUtils.structuredProperty(doi, doi_q, dataInfo)
    result.setPid(List(pid).asJava)
    result.setId(OafMapperUtils.createOpenaireId(50, s"datacite____::$doi", true))
    result.setOriginalId(List(doi).asJava)

    val d = new Date(dateOfCollection * 1000)
    val ISO8601FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ", Locale.US)


    result.setDateofcollection(ISO8601FORMAT.format(d))
    result.setDateoftransformation(ISO8601FORMAT.format(ts))
    result.setDataInfo(dataInfo)

    val creators = (json \\ "creators").extractOrElse[List[CreatorType]](List())


    val authors = creators.zipWithIndex.map { case (c, idx) =>
      val a = new Author
      a.setFullname(c.name.orNull)
      a.setName(c.givenName.orNull)
      a.setSurname(c.familyName.orNull)
      if (c.nameIdentifiers != null && c.nameIdentifiers.isDefined && c.nameIdentifiers.get != null) {
        a.setPid(c.nameIdentifiers.get.map(ni => {
          val q = if (ni.nameIdentifierScheme.isDefined) vocabularies.getTermAsQualifier(ModelConstants.DNET_PID_TYPES, ni.nameIdentifierScheme.get.toLowerCase()) else null
          if (ni.nameIdentifier != null && ni.nameIdentifier.isDefined) {
            OafMapperUtils.structuredProperty(ni.nameIdentifier.get, q, dataInfo)
          }
          else
            null

        }
        )
          .asJava)
      }
      if (c.affiliation.isDefined)
        a.setAffiliation(c.affiliation.get.filter(af => af.nonEmpty).map(af => OafMapperUtils.field(af, dataInfo)).asJava)
      a.setRank(idx + 1)
      a
    }


    val titles: List[TitleType] = (json \\ "titles").extractOrElse[List[TitleType]](List())

    result.setTitle(titles.filter(t => t.title.nonEmpty).map(t => {
      if (t.titleType.isEmpty) {
        OafMapperUtils.structuredProperty(t.title.get, ModelConstants.MAIN_TITLE_QUALIFIER, null)
      } else {
        OafMapperUtils.structuredProperty(t.title.get, t.titleType.get, t.titleType.get, ModelConstants.DNET_DATACITE_TITLE, ModelConstants.DNET_DATACITE_TITLE, null)
      }
    }).asJava)

    if (authors == null || authors.isEmpty || !authors.exists(a => a != null))
      return List()
    result.setAuthor(authors.asJava)

    val dates = (json \\ "dates").extract[List[DateType]]
    val publication_year = (json \\ "publicationYear").extractOrElse[String](null)

    val i_date = dates
      .filter(d => d.date.isDefined && d.dateType.isDefined)
      .find(d => d.dateType.get.equalsIgnoreCase("issued"))
      .map(d => extract_date(d.date.get))
    val a_date: Option[String] = dates
      .filter(d => d.date.isDefined && d.dateType.isDefined && d.dateType.get.equalsIgnoreCase("available"))
      .map(d => extract_date(d.date.get))
      .find(d => d != null && d.isDefined)
      .map(d => d.get)

    if (a_date.isDefined) {
      result.setEmbargoenddate(OafMapperUtils.field(a_date.get, null))
    }
    if (i_date.isDefined && i_date.get.isDefined) {
      result.setDateofacceptance(OafMapperUtils.field(i_date.get.get, null))
      result.getInstance().get(0).setDateofacceptance(OafMapperUtils.field(i_date.get.get, null))
    }
    else if (publication_year != null) {
      result.setDateofacceptance(OafMapperUtils.field(s"01-01-$publication_year", null))
      result.getInstance().get(0).setDateofacceptance(OafMapperUtils.field(s"01-01-$publication_year", null))
    }


    result.setRelevantdate(dates.filter(d => d.date.isDefined && d.dateType.isDefined)
      .map(d => (extract_date(d.date.get), d.dateType.get))
      .filter(d => d._1.isDefined)
      .map(d => (d._1.get, vocabularies.getTermAsQualifier(ModelConstants.DNET_DATACITE_DATE, d._2.toLowerCase())))
      .filter(d => d._2 != null)
      .map(d => generateOAFDate(d._1, d._2)).asJava)

    val subjects = (json \\ "subjects").extract[List[SubjectType]]

    result.setSubject(subjects.filter(s => s.subject.nonEmpty)
      .map(s =>
        OafMapperUtils.structuredProperty(s.subject.get, SUBJ_CLASS, SUBJ_CLASS, ModelConstants.DNET_SUBJECT_TYPOLOGIES, ModelConstants.DNET_SUBJECT_TYPOLOGIES, null)
      ).asJava)


    result.setCollectedfrom(List(DATACITE_COLLECTED_FROM).asJava)

    val descriptions = (json \\ "descriptions").extract[List[DescriptionType]]

    result.setDescription(
      descriptions
        .filter(d => d.description.isDefined).
        map(d =>
          OafMapperUtils.field(d.description.get, null)
        ).filter(s => s != null).asJava)


    val publisher = (json \\ "publisher").extractOrElse[String](null)
    if (publisher != null)
      result.setPublisher(OafMapperUtils.field(publisher, null))


    val language: String = (json \\ "language").extractOrElse[String](null)

    if (language != null)
      result.setLanguage(vocabularies.getSynonymAsQualifier(ModelConstants.DNET_LANGUAGES, language))


    val instance = result.getInstance().get(0)

    val client = (json \ "relationships" \ "client" \\ "id").extractOpt[String]

    val accessRights: List[String] = for {
      JObject(rightsList) <- json \\ "rightsList"
      JField("rightsUri", JString(rightsUri)) <- rightsList
    } yield rightsUri

    val aRights: Option[AccessRight] = accessRights.map(r => {
      vocabularies.getSynonymAsQualifier(ModelConstants.DNET_ACCESS_MODES, r)
    }).find(q => q != null).map(q => {
      val a = new AccessRight
      a.setClassid(q.getClassid)
      a.setClassname(q.getClassname)
      a.setSchemeid(q.getSchemeid)
      a.setSchemename(q.getSchemename)
      a
    })


    val access_rights_qualifier = if (aRights.isDefined) aRights.get else OafMapperUtils.accessRight(ModelConstants.UNKNOWN, ModelConstants.NOT_AVAILABLE, ModelConstants.DNET_ACCESS_MODES, ModelConstants.DNET_ACCESS_MODES)

    if (client.isDefined) {
      val hb = hostedByMap.getOrElse(client.get.toUpperCase(), unknown_repository)
      instance.setHostedby(OafMapperUtils.keyValue(generateDSId(hb.openaire_id), hb.official_name))
      instance.setCollectedfrom(DATACITE_COLLECTED_FROM)
      instance.setUrl(List(s"https://dx.doi.org/$doi").asJava)
      instance.setAccessright(access_rights_qualifier)
      instance.setPid(result.getPid)
      val license = accessRights
        .find(r => r.startsWith("http") && r.matches(".*(/licenses|/publicdomain|unlicense\\.org/|/legal-and-data-protection-notices|/download/license|/open-government-licence).*"))
      if (license.isDefined)
        instance.setLicense(OafMapperUtils.field(license.get, null))
    }

    val awardUris: List[String] = for {
      JObject(fundingReferences) <- json \\ "fundingReferences"
      JField("awardUri", JString(awardUri)) <- fundingReferences
    } yield awardUri

    val relations: List[Relation] = awardUris.flatMap(a => get_projectRelation(a, result.getId)).filter(r => r != null)
    fix_figshare(result)
    result.setId(IdentifierFactory.createIdentifier(result))
    if (result.getId == null)
      return List()
    if (relations != null && relations.nonEmpty) {
      List(result) ::: relations
    }
    else
      List(result)
  }

  def generateDataInfo(trust: String): DataInfo = {
    val di = new DataInfo
    di.setDeletedbyinference(false)
    di.setInferred(false)
    di.setInvisible(false)
    di.setTrust(trust)
    di.setProvenanceaction(ModelConstants.PROVENANCE_ACTION_SET_QUALIFIER)
    di
  }

  def generateDSId(input: String): String = {
    val b = StringUtils.substringBefore(input, "::")
    val a = StringUtils.substringAfter(input, "::")
    s"10|$b::${DHPUtils.md5(a)}"
  }


}