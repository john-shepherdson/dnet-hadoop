package eu.dnetlib.dhp.datacite

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.collect.Lists
import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup
import eu.dnetlib.dhp.datacite.DataciteModelConstants._
import eu.dnetlib.dhp.schema.action.AtomicAction
import eu.dnetlib.dhp.schema.common.ModelConstants
import eu.dnetlib.dhp.schema.oaf.utils.{IdentifierFactory, OafMapperUtils}
import eu.dnetlib.dhp.schema.oaf.{Dataset => OafDataset, _}
import eu.dnetlib.dhp.utils.DHPUtils
import org.apache.commons.lang3.StringUtils
import org.json4s.DefaultFormats
import org.json4s.JsonAST.{JField, JObject, JString}
import org.json4s.jackson.JsonMethods.parse

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.chrono.ThaiBuddhistDate
import java.time.format.DateTimeFormatter
import java.util.{Date, Locale}
import scala.collection.JavaConverters._
import scala.io.Source

object DataciteToOAFTransformation {

  case class HostedByMapType(
    openaire_id: String,
    datacite_name: String,
    official_name: String,
    similarity: Option[Float]
  ) {}

  val mapper = new ObjectMapper()

  val unknown_repository: HostedByMapType = HostedByMapType(
    ModelConstants.UNKNOWN_REPOSITORY_ORIGINALID,
    ModelConstants.UNKNOWN_REPOSITORY.getValue,
    ModelConstants.UNKNOWN_REPOSITORY.getValue,
    Some(1.0f)
  )

  val hostedByMap: Map[String, HostedByMapType] = {
    val s = Source.fromInputStream(getClass.getResourceAsStream("hostedBy_map.json")).mkString
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json: org.json4s.JValue = parse(s)
    json.extract[Map[String, HostedByMapType]]
  }

  /** This method should skip record if json contains invalid text
    * defined in file datacite_filter
    *
    * @param record : not parsed Datacite record
    * @param json : parsed record
    * @return True if the record should be skipped
    */
  def skip_record(record: String, json: org.json4s.JValue): Boolean = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    datacite_filter.exists(f => record.contains(f)) || (json \\ "publisher")
      .extractOrElse[String]("")
      .equalsIgnoreCase("FAIRsharing")

  }

  @deprecated("this method will be removed", "dhp")
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

  /** This utility method indicates whether the embargo date has been reached
    * @param embargo_end_date
    * @return True if the embargo date has been reached, false otherwise
    */
  def embargo_end(embargo_end_date: String): Boolean = {
    val dt = LocalDate.parse(embargo_end_date, DateTimeFormatter.ofPattern("[yyyy-MM-dd]"))
    val td = LocalDate.now()
    td.isAfter(dt)
  }

  def extract_date(input: String): Option[String] = {
    val d = Date_regex
      .map(pattern => {
        val matcher = pattern.matcher(input)
        if (matcher.find())
          matcher.group(0)
        else
          null
      })
      .find(s => s != null)

    if (d.isDefined) {
      val a_date = if (d.get.length == 4) s"01-01-${d.get}" else d.get
      try {
        return Some(LocalDate.parse(a_date, df_en).toString)
      } catch {
        case _: Throwable =>
          try {
            return Some(LocalDate.parse(a_date, df_it).toString)
          } catch {
            case _: Throwable =>
              return None
          }
      }
    }
    d
  }

  def fix_thai_date(input: String, format: String): String = {
    try {
      val a_date = LocalDate.parse(input, DateTimeFormatter.ofPattern(format))
      val d = ThaiBuddhistDate.of(a_date.getYear, a_date.getMonth.getValue, a_date.getDayOfMonth)
      LocalDate.from(d).toString
    } catch {
      case _: Throwable => ""
    }
  }

  /** *
    * Use the vocabulary dnet:publication_resource to find a synonym to one of these terms and get the instance.type.
    * Using the dnet:result_typologies vocabulary, we look up the instance.type synonym
    * to generate one of the following main entities:
    *  - publication
    *  - dataset
    *  - software
    *  - otherresearchproduct
    *
    * @param resourceType
    * @param resourceTypeGeneral
    * @param schemaOrg
    * @param vocabularies
    * @return
    */
  def getTypeQualifier(
    resourceType: String,
    resourceTypeGeneral: String,
    schemaOrg: String,
    vocabularies: VocabularyGroup
  ): (Qualifier, Qualifier) = {
    if (resourceType != null && resourceType.nonEmpty) {
      val typeQualifier =
        vocabularies.getSynonymAsQualifier(ModelConstants.DNET_PUBLICATION_RESOURCE, resourceType)
      if (typeQualifier != null)
        return (
          typeQualifier,
          vocabularies.getSynonymAsQualifier(
            ModelConstants.DNET_RESULT_TYPOLOGIES,
            typeQualifier.getClassid
          )
        )
    }
    if (schemaOrg != null && schemaOrg.nonEmpty) {
      val typeQualifier =
        vocabularies.getSynonymAsQualifier(ModelConstants.DNET_PUBLICATION_RESOURCE, schemaOrg)
      if (typeQualifier != null)
        return (
          typeQualifier,
          vocabularies.getSynonymAsQualifier(
            ModelConstants.DNET_RESULT_TYPOLOGIES,
            typeQualifier.getClassid
          )
        )

    }
    if (resourceTypeGeneral != null && resourceTypeGeneral.nonEmpty) {
      val typeQualifier = vocabularies.getSynonymAsQualifier(
        ModelConstants.DNET_PUBLICATION_RESOURCE,
        resourceTypeGeneral
      )
      if (typeQualifier != null)
        return (
          typeQualifier,
          vocabularies.getSynonymAsQualifier(
            ModelConstants.DNET_RESULT_TYPOLOGIES,
            typeQualifier.getClassid
          )
        )

    }
    null
  }

  def getResult(
    resourceType: String,
    resourceTypeGeneral: String,
    schemaOrg: String,
    vocabularies: VocabularyGroup
  ): Result = {
    val typeQualifiers: (Qualifier, Qualifier) =
      getTypeQualifier(resourceType, resourceTypeGeneral, schemaOrg, vocabularies)
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
      JObject(dates)                         <- json \\ "dates"
      JField("dateType", JString(dateTypes)) <- dates
    } yield dateTypes

    l.exists(p => p.equalsIgnoreCase("available"))

  }

  /** As describe in ticket #6377
    * when the result come from figshare we need to remove subject
    * and set Access rights OPEN.
    *
    * @param r
    */
  def fix_figshare(r: Result): Unit = {

    if (r.getInstance() != null) {
      val hosted_by_figshare = r
        .getInstance()
        .asScala
        .exists(i => i.getHostedby != null && "figshare".equalsIgnoreCase(i.getHostedby.getValue))
      if (hosted_by_figshare) {
        r.getInstance().asScala.foreach(i => i.setAccessright(ModelConstants.OPEN_ACCESS_RIGHT()))
        val l: List[Subject] = List()
        r.setSubject(l.asJava)
      }
    }

  }

  def generateOAFDate(dt: String, q: Qualifier): StructuredProperty = {
    OafMapperUtils.structuredProperty(dt, q)
  }

  def generateRelation(
                        sourceId: String,
                        targetId: String,
                        relClass: String,
                        collectedFrom: KeyValue,
                        di: DataInfo
  ): Relation = {
    val r = new Relation
    r.setSource(sourceId)
    r.setTarget(targetId)
    r.setRelType(ModelConstants.RESULT_PROJECT)
    r.setRelClass(relClass)
    r.setSubRelType(ModelConstants.OUTCOME)
    r.setProvenance(Lists.newArrayList(OafMapperUtils.getProvenance(collectedFrom, di)))
    r
  }

  def get_projectRelation(awardUri: String, sourceId: String): List[Relation] = {
    val match_pattern = funder_regex.find(s => s._1.matcher(awardUri).find())

    if (match_pattern.isDefined) {
      val m = match_pattern.get._1
      val p = match_pattern.get._2
      val grantId = m.matcher(awardUri).replaceAll("$2")
      val targetId = s"$p${DHPUtils.md5(grantId)}"
      List(generateRelation(sourceId, targetId, "isProducedBy", DATACITE_COLLECTED_FROM, relDataInfo))
    } else
      List()

  }

  def generateOAF(
    input: String,
    ts: Long,
    dateOfCollection: Long,
    vocabularies: VocabularyGroup,
    exportLinks: Boolean
  ): List[Oaf] = {

    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json = parse(input)

    if (skip_record(input, json))
      return List()

    val resourceType = (json \ "attributes" \ "types" \ "resourceType").extractOrElse[String](null)
    val resourceTypeGeneral =
      (json \ "attributes" \ "types" \ "resourceTypeGeneral").extractOrElse[String](null)
    val schemaOrg = (json \ "attributes" \ "types" \ "schemaOrg").extractOrElse[String](null)

    val doi = (json \ "attributes" \ "doi").extract[String]
    if (doi.isEmpty)
      return List()

    //Mapping type based on vocabularies dnet:publication_resource and dnet:result_typologies
    val result = getResult(resourceType, resourceTypeGeneral, schemaOrg, vocabularies)
    if (result == null)
      return List()

    // DOI is mapped on a PID inside a Instance object
    val doi_q = OafMapperUtils.qualifier(
      "doi",
      "doi",
      ModelConstants.DNET_PID_TYPES
    )
    val pid = OafMapperUtils.structuredProperty(doi, doi_q)
    result.setPid(List(pid).asJava)

    // This identifiere will be replaced in a second moment using the PID logic generation
    result.setId(IdentifierFactory.createOpenaireId(50, s"datacite____::$doi", true))
    result.setOriginalId(List(doi).asJava)

    val d = new Date(dateOfCollection * 1000)
    val ISO8601FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ", Locale.US)

    result.setDateofcollection(ISO8601FORMAT.format(d))
    result.setDateoftransformation(ISO8601FORMAT.format(d))
    result.setDataInfo(dataInfo)

    val creators = (json \\ "creators").extractOrElse[List[CreatorType]](List())

    val authors = creators.zipWithIndex.map { case (c, idx) =>
      val a = new Author
      a.setFullname(c.name.orNull)
      a.setName(c.givenName.orNull)
      a.setSurname(c.familyName.orNull)
      if (c.nameIdentifiers != null && c.nameIdentifiers.isDefined && c.nameIdentifiers.get != null) {
        a.setPid(
          c.nameIdentifiers.get
            .map(ni => {
              val q =
                if (ni.nameIdentifierScheme.isDefined)
                  vocabularies.getTermAsQualifier(
                    ModelConstants.DNET_PID_TYPES,
                    ni.nameIdentifierScheme.get.toLowerCase()
                  )
                else null
              if (ni.nameIdentifier != null && ni.nameIdentifier.isDefined) {
                OafMapperUtils.authorPid(ni.nameIdentifier.get, q, relDataInfo)
              } else
                null

            })
            .asJava
        )
      }
      a.setRank(idx + 1)
      a
    }

    if (authors == null || authors.isEmpty || !authors.exists(a => a != null))
      return List()
    result.setAuthor(authors.asJava)

    val titles: List[TitleType] = (json \\ "titles").extractOrElse[List[TitleType]](List())

    result.setTitle(
      titles
        .filter(t => t.title.nonEmpty)
        .map(t => {
          if (t.titleType.isEmpty) {
            OafMapperUtils
              .structuredProperty(t.title.get, ModelConstants.MAIN_TITLE_QUALIFIER)
          } else {
            OafMapperUtils.structuredProperty(
              t.title.get,
              t.titleType.get,
              t.titleType.get,
              ModelConstants.DNET_DATACITE_TITLE
            )
          }
        })
        .asJava
    )

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
      if (doi.startsWith("10.14457")) {
        val date = fix_thai_date(a_date.get, "[yyyy-MM-dd]")
        result.setEmbargoenddate(date)
      } else {
        result.setEmbargoenddate(a_date.get)
      }
    }
    if (i_date.isDefined && i_date.get.isDefined) {
      if (doi.startsWith("10.14457")) {
        val date = fix_thai_date(i_date.get.get, "[yyyy-MM-dd]")
        result.setDateofacceptance(date)
        result
          .getInstance()
          .get(0)
          .setDateofacceptance(date)
      } else {
        result.setDateofacceptance(i_date.get.get)
        result.getInstance().get(0).setDateofacceptance(i_date.get.get)
      }
    } else if (publication_year != null) {
      val date = s"01-01-$publication_year"
      if (doi.startsWith("10.14457")) {
        val fdate = fix_thai_date(date, "[dd-MM-yyyy]")
        result.setDateofacceptance(fdate)
        result
          .getInstance()
          .get(0)
          .setDateofacceptance(fdate)
      } else {
        result.setDateofacceptance(date)
        result
          .getInstance()
          .get(0)
          .setDateofacceptance(date)
      }
    }

    result.setRelevantdate(
      dates
        .filter(d => d.date.isDefined && d.dateType.isDefined)
        .map(d => (extract_date(d.date.get), d.dateType.get))
        .filter(d => d._1.isDefined)
        .map(d =>
          (
            d._1.get,
            vocabularies.getTermAsQualifier(ModelConstants.DNET_DATACITE_DATE, d._2.toLowerCase())
          )
        )
        .filter(d => d._2 != null)
        .map(d => generateOAFDate(d._1, d._2))
        .asJava
    )

    val subjects = (json \\ "subjects").extract[List[SubjectType]]

    result.setSubject(
      subjects
        .filter(s => s.subject.nonEmpty)
        .map(s =>
          OafMapperUtils.subject(
            s.subject.get,
            SUBJ_CLASS,
            SUBJ_CLASS,
            ModelConstants.DNET_SUBJECT_TYPOLOGIES,
            relDataInfo
          )
        )
        .asJava
    )

    result.setCollectedfrom(List(DATACITE_COLLECTED_FROM).asJava)

    val descriptions = (json \\ "descriptions").extract[List[DescriptionType]]

    result.setDescription(
      descriptions
        .filter(d => d.description.isDefined)
        .map(d => d.description.get)
        .filter(s => s != null)
        .asJava
    )

    val publisher = (json \\ "publisher").extractOrElse[String](null)
    if (publisher != null)
      result.setPublisher(OafMapperUtils.publisher(publisher))

    val language: String = (json \\ "language").extractOrElse[String](null)

    if (language != null)
      result.setLanguage(
        vocabularies.getSynonymAsQualifier(ModelConstants.DNET_LANGUAGES, language)
      )

    val instance = result.getInstance().get(0)

    val client = (json \ "relationships" \ "client" \\ "id").extractOpt[String]

    val accessRights: List[String] = for {
      JObject(rightsList)                     <- json \\ "rightsList"
      JField("rightsUri", JString(rightsUri)) <- rightsList
    } yield rightsUri

    val aRights: Option[AccessRight] = accessRights
      .map(r => {
        vocabularies.getSynonymAsQualifier(ModelConstants.DNET_ACCESS_MODES, r)
      })
      .find(q => q != null)
      .map(q => {
        val a = new AccessRight
        a.setClassid(q.getClassid)
        a.setClassname(q.getClassname)
        a.setSchemeid(q.getSchemeid)
        a
      })

    val access_rights_qualifier =
      if (aRights.isDefined) aRights.get
      else
        OafMapperUtils.accessRight(
          ModelConstants.UNKNOWN,
          ModelConstants.NOT_AVAILABLE,
          ModelConstants.DNET_ACCESS_MODES,
          ModelConstants.DNET_ACCESS_MODES
        )

    if (client.isDefined) {

      val hb = hostedByMap.getOrElse(client.get.toUpperCase(), unknown_repository)
      instance.setHostedby(OafMapperUtils.keyValue(generateDSId(hb.openaire_id), hb.official_name))

      instance.setCollectedfrom(DATACITE_COLLECTED_FROM)
      instance.setUrl(List(s"https://dx.doi.org/$doi").asJava)
      instance.setAccessright(access_rights_qualifier)
      instance.setPid(result.getPid)
      val license = accessRights
        .find(r =>
          r.startsWith("http") && r.matches(
            ".*(/licenses|/publicdomain|unlicense\\.org/|/legal-and-data-protection-notices|/download/license|/open-government-licence).*"
          )
        )
      if (license.isDefined)
        instance.setLicense(OafMapperUtils.license(license.get))
    }

    val awardUris: List[String] = for {
      JObject(fundingReferences)            <- json \\ "fundingReferences"
      JField("awardUri", JString(awardUri)) <- fundingReferences
    } yield awardUri

    val oid = result.getId
    result.setId(IdentifierFactory.createIdentifier(result))
    if (!result.getId.equalsIgnoreCase(oid)) {
      result.setOriginalId((oid :: List(doi)).asJava)
    }

    var relations: List[Relation] =
      awardUris.flatMap(a => get_projectRelation(a, result.getId)).filter(r => r != null)

    fix_figshare(result)

    if (result.getId == null)
      return List()

    if (exportLinks) {
      val rels: List[RelatedIdentifierType] = for {
        JObject(relIdentifier)                                          <- json \\ "relatedIdentifiers"
        JField("relationType", JString(relationType))                   <- relIdentifier
        JField("relatedIdentifierType", JString(relatedIdentifierType)) <- relIdentifier
        JField("relatedIdentifier", JString(relatedIdentifier))         <- relIdentifier
      } yield RelatedIdentifierType(relationType, relatedIdentifier, relatedIdentifierType)

      relations = relations ::: generateRelations(
        rels,
        result.getId,
        if (i_date.isDefined && i_date.get.isDefined) i_date.get.get else null
      )
    }
    if (relations != null && relations.nonEmpty) {
      List(result) ::: relations
    } else
      List(result)
  }

  private def generateRelations(
    rels: List[RelatedIdentifierType],
    id: String,
    date: String
  ): List[Relation] = {
    rels
      .filter(r =>
        subRelTypeMapping
          .contains(r.relationType) && (r.relatedIdentifierType.equalsIgnoreCase("doi") ||
        r.relatedIdentifierType.equalsIgnoreCase("pmid") ||
        r.relatedIdentifierType.equalsIgnoreCase("arxiv"))
      )
      .map(r => {
        val rel = new Relation

        rel.setProvenance(Lists.newArrayList(OafMapperUtils.getProvenance(DATACITE_COLLECTED_FROM, relDataInfo)))

        val subRelType = subRelTypeMapping(r.relationType).relType
        rel.setRelType(REL_TYPE_VALUE)
        rel.setSubRelType(subRelType)
        rel.setRelClass(r.relationType)

        val dateProps: KeyValue = OafMapperUtils.keyValue(DATE_RELATION_KEY, date)

        rel.setProperties(List(dateProps).asJava)

        rel.setSource(id)
        rel.setTarget(
          DHPUtils.generateUnresolvedIdentifier(r.relatedIdentifier, r.relatedIdentifierType)
        )

        rel
      })
  }

  def generateDSId(input: String): String = {
    val b = StringUtils.substringBefore(input, "::")
    val a = StringUtils.substringAfter(input, "::")
    s"10|$b::${DHPUtils.md5(a)}"
  }

}
