package eu.dnetlib.dhp.collection.crossref

import com.fasterxml.jackson.databind.ObjectMapper
import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup
import eu.dnetlib.dhp.schema.common.ModelConstants
import eu.dnetlib.dhp.schema.oaf._
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils.{field, qualifier, structuredProperty, subject}
import eu.dnetlib.dhp.schema.oaf.utils.{
  DoiCleaningRule,
  GraphCleaningFunctions,
  IdentifierFactory,
  OafMapperUtils,
  PidType
}
import eu.dnetlib.dhp.utils.DHPUtils
import org.apache.commons.lang.StringUtils
import org.json4s
import org.json4s.DefaultFormats
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods._
import org.slf4j.{Logger, LoggerFactory}

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.io.Source
import scala.util.matching.Regex

case class CrossrefDT(doi: String, json: String, timestamp: Long) {}

case class mappingAffiliation(name: String) {}

case class mappingAuthor(
  given: Option[String],
  family: Option[String],
  sequence: Option[String],
  ORCID: Option[String],
  affiliation: Option[mappingAffiliation]
) {}

case class funderInfo(id: String, uri: String, name: String, synonym: List[String]) {}

case class mappingFunder(name: String, DOI: Option[String], award: Option[List[String]]) {}

case class CrossrefResult(oafType: String, body: String) {}

case class UnpayWall(doi: String, is_oa: Boolean, best_oa_location: UnpayWallOALocation, oa_status: String) {}

case class UnpayWallOALocation(license: Option[String], url: String, host_type: Option[String]) {}

case object Crossref2Oaf {
  val logger: Logger = LoggerFactory.getLogger(Crossref2Oaf.getClass)
  val mapper = new ObjectMapper

  val irishFunder: List[funderInfo] = {
    val s = Source
      .fromInputStream(getClass.getResourceAsStream("/eu/dnetlib/dhp/collection/crossref/irish_funder.json"))
      .mkString
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json: org.json4s.JValue = parse(s)
    json.extract[List[funderInfo]]
  }

  val invalidName = List(
    ",",
    "none none",
    "none, none",
    "none &na;",
    "(:null)",
    "test test test",
    "test test",
    "test",
    "&na; &na;"
  )

  def getIrishId(doi: String): Option[String] = {
    val id = doi.split("/").last
    irishFunder
      .find(f => id.equalsIgnoreCase(f.id) || (f.synonym.nonEmpty && f.synonym.exists(s => s.equalsIgnoreCase(id))))
      .map(f => f.id)
  }

  def createCrossrefCollectedFrom(): KeyValue = {

    val cf = new KeyValue
    cf.setValue("Crossref");
    cf.setKey(ModelConstants.CROSSREF_ID)
    cf

  }

  def createUnpayWallCollectedFrom(): KeyValue = {

    val cf = new KeyValue
    cf.setValue("UnpayWall")
    cf.setKey(s"10|openaire____:${DHPUtils.md5("UnpayWall".toLowerCase)}")
    cf

  }

  def generateDataInfo(): DataInfo = {
    generateDataInfo("0.91")
  }

  def generateDataInfo(trust: String): DataInfo = {
    val di = new DataInfo
    di.setDeletedbyinference(false)
    di.setInferred(false)
    di.setInvisible(false)
    di.setTrust(trust)
    di.setProvenanceaction(
      OafMapperUtils.qualifier(
        ModelConstants.SYSIMPORT_ACTIONSET,
        ModelConstants.SYSIMPORT_ACTIONSET,
        ModelConstants.DNET_PROVENANCE_ACTIONS,
        ModelConstants.DNET_PROVENANCE_ACTIONS
      )
    )
    di
  }

  def getOpenAccessQualifier(): AccessRight = {

    OafMapperUtils.accessRight(
      ModelConstants.ACCESS_RIGHT_OPEN,
      "Open Access",
      ModelConstants.DNET_ACCESS_MODES,
      ModelConstants.DNET_ACCESS_MODES
    )
  }

  def getRestrictedQualifier(): AccessRight = {
    OafMapperUtils.accessRight(
      "RESTRICTED",
      "Restricted",
      ModelConstants.DNET_ACCESS_MODES,
      ModelConstants.DNET_ACCESS_MODES
    )
  }

  def getUnknownQualifier(): AccessRight = {
    OafMapperUtils.accessRight(
      ModelConstants.UNKNOWN,
      ModelConstants.NOT_AVAILABLE,
      ModelConstants.DNET_ACCESS_MODES,
      ModelConstants.DNET_ACCESS_MODES
    )
  }

  def getEmbargoedAccessQualifier(): AccessRight = {
    OafMapperUtils.accessRight(
      "EMBARGO",
      "Embargo",
      ModelConstants.DNET_ACCESS_MODES,
      ModelConstants.DNET_ACCESS_MODES
    )
  }

  def getClosedAccessQualifier(): AccessRight = {
    OafMapperUtils.accessRight(
      "CLOSED",
      "Closed Access",
      ModelConstants.DNET_ACCESS_MODES,
      ModelConstants.DNET_ACCESS_MODES
    )
  }

  def decideAccessRight(lic: Field[String], date: String): AccessRight = {
    if (lic == null) {
      //Default value Unknown
      return getUnknownQualifier()
    }
    val license: String = lic.getValue
    //CC licenses
    if (
      license.startsWith("cc") ||
      license.startsWith("http://creativecommons.org/licenses") ||
      license.startsWith("https://creativecommons.org/licenses") ||

      //ACS Publications Author choice licenses (considered OPEN also by Unpaywall)
      license.equals("http://pubs.acs.org/page/policy/authorchoice_ccby_termsofuse.html") ||
      license.equals("http://pubs.acs.org/page/policy/authorchoice_termsofuse.html") ||
      license.equals("http://pubs.acs.org/page/policy/authorchoice_ccbyncnd_termsofuse.html") ||

      //APA (considered OPEN also by Unpaywall)
      license.equals("http://www.apa.org/pubs/journals/resources/open-access.aspx")
    ) {

      val oaq: AccessRight = getOpenAccessQualifier()
      oaq.setOpenAccessRoute(OpenAccessRoute.hybrid)
      return oaq
    }

    //OUP (BUT ONLY AFTER 12 MONTHS FROM THE PUBLICATION DATE, OTHERWISE THEY ARE EMBARGOED)
    if (
      license.equals(
        "https://academic.oup.com/journals/pages/open_access/funder_policies/chorus/standard_publication_model"
      )
    ) {
      val now = java.time.LocalDate.now

      try {
        val pub_date = LocalDate.parse(date, DateTimeFormatter.ofPattern("yyyy-MM-dd"))
        if (((now.toEpochDay - pub_date.toEpochDay) / 365.0) > 1) {
          val oaq: AccessRight = getOpenAccessQualifier()
          oaq.setOpenAccessRoute(OpenAccessRoute.hybrid)
          return oaq
        } else {
          return getEmbargoedAccessQualifier()
        }
      } catch {
        case e: Exception => {
          try {
            val pub_date =
              LocalDate.parse(date, DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"))
            if (((now.toEpochDay - pub_date.toEpochDay) / 365.0) > 1) {
              val oaq: AccessRight = getOpenAccessQualifier()
              oaq.setOpenAccessRoute(OpenAccessRoute.hybrid)
              return oaq
            } else {
              return getEmbargoedAccessQualifier()
            }
          } catch {
            case ex: Exception => return getClosedAccessQualifier()
          }
        }

      }

    }

    getClosedAccessQualifier()

  }

  def isValidAuthorName(fullName: String): Boolean = {
    if (fullName == null || fullName.isEmpty)
      return false
    if (invalidName.contains(fullName.toLowerCase.trim))
      return false
    true
  }

  def filterResult(publication: Result): Boolean = {

    //Case empty publication
    if (publication == null)
      return false
    if (publication.getId == null || publication.getId.isEmpty)
      return false

    //Case publication with no title
    if (publication.getTitle == null || publication.getTitle.size == 0)
      return false

    val s = publication.getTitle.asScala.count(p =>
      p.getValue != null
      && p.getValue.nonEmpty && !p.getValue.equalsIgnoreCase("[NO TITLE AVAILABLE]")
    )

    if (s == 0)
      return false

    // fixes #4360 (test publisher)
    val publisher =
      if (publication.getPublisher != null) publication.getPublisher.getValue else null

    if (
      publisher != null && (publisher.equalsIgnoreCase("Test accounts") || publisher
        .equalsIgnoreCase("CrossRef Test Account"))
    ) {
      return false;
    }

    //RELAXED this constraint
    //Publication with no Author
    if (publication.getAuthor == null || publication.getAuthor.size() == 0)
      return true

    //filter invalid author
    val authors = publication.getAuthor.asScala.map(s => {
      if (s.getFullname.nonEmpty) {
        s.getFullname
      } else
        s"${s.getName} ${s.getSurname}"
    })

    val c = authors.count(isValidAuthorName)
    if (c == 0)
      return false

    // fixes #4368
    if (
      authors.count(s => s.equalsIgnoreCase("Addie Jackson")) > 0 && "Elsevier BV".equalsIgnoreCase(
        publication.getPublisher.getValue
      )
    )
      return false

    true
  }

  def get_unpaywall_color(input: String): Option[OpenAccessRoute] = {
    if (input == null || input.equalsIgnoreCase("close"))
      return None
    if (input.equalsIgnoreCase("green"))
      return Some(OpenAccessRoute.green)
    if (input.equalsIgnoreCase("bronze"))
      return Some(OpenAccessRoute.bronze)
    if (input.equalsIgnoreCase("hybrid"))
      return Some(OpenAccessRoute.hybrid)
    else
      return Some(OpenAccessRoute.gold)

  }

  def get_color(input: String): Option[OpenAccessRoute] = {
    if (input == null || input.equalsIgnoreCase("closed"))
      return None
    if (input.equalsIgnoreCase("green"))
      return Some(OpenAccessRoute.green)
    if (input.equalsIgnoreCase("bronze"))
      return Some(OpenAccessRoute.bronze)
    if (input.equalsIgnoreCase("hybrid"))
      return Some(OpenAccessRoute.hybrid)
    else
      return Some(OpenAccessRoute.gold)

  }

  def mappingResult(result: Result, json: JValue, instanceType: Qualifier, originalType: String): Result = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats

    //MAPPING Crossref DOI into PID
    val doi: String = DoiCleaningRule.normalizeDoi((json \ "DOI").extract[String])
    result.setPid(
      List(
        structuredProperty(
          doi,
          qualifier(
            PidType.doi.toString,
            PidType.doi.toString,
            ModelConstants.DNET_PID_TYPES,
            ModelConstants.DNET_PID_TYPES
          ),
          null
        )
      ).asJava
    )

    //MAPPING Crossref DOI into OriginalId
    //and Other Original Identifier of dataset like clinical-trial-number
    val clinicalTrialNumbers = for (JString(ctr) <- json \ "clinical-trial-number") yield ctr
    val alternativeIds = for (JString(ids) <- json \ "alternative-id") yield ids
    val tmp = clinicalTrialNumbers ::: alternativeIds ::: List(doi)

    val originalIds = new util.ArrayList(tmp.filter(id => id != null).asJava)
    result.setOriginalId(originalIds)

    // Add DataInfo
    result.setDataInfo(generateDataInfo())

    result.setLastupdatetimestamp((json \ "indexed" \ "timestamp").extract[Long])
    result.setDateofcollection((json \ "indexed" \ "date-time").extract[String])

    result.setCollectedfrom(List(createCrossrefCollectedFrom()).asJava)

    // Publisher ( Name of work's publisher mapped into  Result/Publisher)
    val publisher = (json \ "publisher").extractOrElse[String](null)
    if (publisher != null && publisher.nonEmpty)
      result.setPublisher(field(publisher, null))

    // TITLE
    val mainTitles =
      for { JString(title) <- json \ "title" if title.nonEmpty } yield {
        structuredProperty(title, ModelConstants.MAIN_TITLE_QUALIFIER, null)
      }
    val originalTitles = for {
      JString(title) <- json \ "original-title" if title.nonEmpty
    } yield structuredProperty(title, ModelConstants.ALTERNATIVE_TITLE_QUALIFIER, null)
    val shortTitles = for {
      JString(title) <- json \ "short-title" if title.nonEmpty
    } yield structuredProperty(title, ModelConstants.ALTERNATIVE_TITLE_QUALIFIER, null)
    val subtitles =
      for { JString(title) <- json \ "subtitle" if title.nonEmpty } yield structuredProperty(
        title,
        ModelConstants.SUBTITLE_QUALIFIER,
        null
      )
    result.setTitle((mainTitles ::: originalTitles ::: shortTitles ::: subtitles).asJava)

    // DESCRIPTION
    val descriptionList =
      for { JString(description) <- json \ "abstract" } yield field[String](description, null)
    result.setDescription(descriptionList.asJava)

    // Source
    val sourceList = for {
      JString(source) <- json \ "source" if source != null && source.nonEmpty
    } yield field(source, null)
    result.setSource(sourceList.asJava)

    //RELEVANT DATE Mapping
    val createdDate = generateDate(
      (json \ "created" \ "date-time").extract[String],
      (json \ "created" \ "date-parts").extract[List[List[Int]]],
      "created",
      ModelConstants.DNET_DATACITE_DATE
    )
    val postedDate = generateDate(
      (json \ "posted" \ "date-time").extractOrElse[String](null),
      (json \ "posted" \ "date-parts").extract[List[List[Int]]],
      "available",
      ModelConstants.DNET_DATACITE_DATE
    )
    val acceptedDate = generateDate(
      (json \ "accepted" \ "date-time").extractOrElse[String](null),
      (json \ "accepted" \ "date-parts").extract[List[List[Int]]],
      "accepted",
      ModelConstants.DNET_DATACITE_DATE
    )
    val publishedPrintDate = generateDate(
      (json \ "published-print" \ "date-time").extractOrElse[String](null),
      (json \ "published-print" \ "date-parts").extract[List[List[Int]]],
      "published-print",
      ModelConstants.DNET_DATACITE_DATE
    )
    val publishedOnlineDate = generateDate(
      (json \ "published-online" \ "date-time").extractOrElse[String](null),
      (json \ "published-online" \ "date-parts").extract[List[List[Int]]],
      "published-online",
      ModelConstants.DNET_DATACITE_DATE
    )

    val issuedDate = extractDate(
      (json \ "issued" \ "date-time").extractOrElse[String](null),
      (json \ "issued" \ "date-parts").extract[List[List[Int]]]
    )
    if (StringUtils.isNotBlank(issuedDate)) {
      result.setDateofacceptance(field(issuedDate, null))
    } else {
      result.setDateofacceptance(field(createdDate.getValue, null))
    }
    result.setRelevantdate(
      List(createdDate, postedDate, acceptedDate, publishedOnlineDate, publishedPrintDate)
        .filter(p => p != null)
        .asJava
    )

    //Mapping Subject
    val subjectList: List[String] = (json \ "subject").extractOrElse[List[String]](List())

    if (subjectList.nonEmpty) {
      result.setSubject(
        subjectList.map(s => subject(s, ModelConstants.SUBTITLE_QUALIFIER, null)).asJava
      )
    }

    //Mapping Author
    val authorList: List[mappingAuthor] =
      (json \ "author").extract[List[mappingAuthor]].filter(a => a.family.isDefined)

    val sorted_list = authorList.sortWith((a: mappingAuthor, b: mappingAuthor) =>
      a.sequence.isDefined && a.sequence.get.equalsIgnoreCase("first")
    )

    result.setAuthor(sorted_list.zipWithIndex.map { case (a, index) =>
      generateAuhtor(a.given.orNull, a.family.get, a.ORCID.orNull, index)
    }.asJava)

    // Mapping instance
    val instance = new Instance()
    val license = for {
      JObject(license)                                    <- json \ "license"
      JField("URL", JString(lic))                         <- license
      JField("content-version", JString(content_version)) <- license
    } yield (field[String](lic, null), content_version)
    val l = license.filter(d => StringUtils.isNotBlank(d._1.getValue))
    if (l.nonEmpty) {
      if (l exists (d => d._2.equals("vor"))) {
        for (d <- l) {
          if (d._2.equals("vor")) {
            instance.setLicense(d._1)
          }
        }
      } else {
        instance.setLicense(l.head._1)
      }
    }

    // Ticket #6281 added pid to Instance
    instance.setPid(result.getPid)

    val has_review = json \ "relation" \ "has-review" \ "id"

    if (has_review != JNothing) {
      instance.setRefereed(
        OafMapperUtils.qualifier(
          "0001",
          "peerReviewed",
          ModelConstants.DNET_REVIEW_LEVELS,
          ModelConstants.DNET_REVIEW_LEVELS
        )
      )
    }

    instance.setAccessright(
      decideAccessRight(instance.getLicense, result.getDateofacceptance.getValue)
    )
    instance.setInstancetype(instanceType)

    //ADD ORIGINAL TYPE to the mapping
    val itm = new InstanceTypeMapping
    itm.setOriginalType(originalType)
    itm.setVocabularyName(ModelConstants.OPENAIRE_COAR_RESOURCE_TYPES_3_1)
    instance.setInstanceTypeMapping(List(itm).asJava)

    instance.setCollectedfrom(createCrossrefCollectedFrom())
    if (StringUtils.isNotBlank(issuedDate)) {
      instance.setDateofacceptance(field(issuedDate, null))
    } else {
      instance.setDateofacceptance(field(createdDate.getValue, null))
    }
    val s: List[String] = List("https://doi.org/" + doi)
    //    val links: List[String] = ((for {JString(url) <- json \ "link" \ "URL"} yield url) ::: List(s)).filter(p => p != null && p.toLowerCase().contains(doi.toLowerCase())).distinct
    //    if (links.nonEmpty) {
    //      instance.setUrl(links.asJava)
    //    }
    if (s.nonEmpty) {
      instance.setUrl(s.asJava)
    }

    result.setInstance(List(instance).asJava)

    //IMPORTANT
    //The old method result.setId(generateIdentifier(result, doi))
    //is replaced using IdentifierFactory, but the old identifier
    //is preserved among the originalId(s)
    val oldId = generateIdentifier(result, doi)
    result.setId(oldId)

    val newId = IdentifierFactory.createDOIBoostIdentifier(result)
    if (!oldId.equalsIgnoreCase(newId)) {
      result.getOriginalId.add(oldId)
    }
    result.setId(newId)

    if (result.getId == null)
      null
    else
      result
  }

  def generateIdentifier(oaf: Result, doi: String): String = {
    val id = DHPUtils.md5(doi.toLowerCase)
    s"50|doiboost____|$id"
  }

  def generateAuhtor(given: String, family: String, orcid: String, index: Int): Author = {
    val a = new Author
    a.setName(given)
    a.setSurname(family)
    a.setFullname(s"$given $family")
    a.setRank(index + 1)
    if (StringUtils.isNotBlank(orcid))
      a.setPid(
        List(
          structuredProperty(
            orcid,
            qualifier(
              ModelConstants.ORCID_PENDING,
              ModelConstants.ORCID_PENDING,
              ModelConstants.DNET_PID_TYPES,
              ModelConstants.DNET_PID_TYPES
            ),
            generateDataInfo()
          )
        ).asJava
      )

    a
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
    * @param vocabularies
    * @return
    */
  def getTypeQualifier(
    resourceType: String,
    vocabularies: VocabularyGroup
  ): (Qualifier, Qualifier, String) = {
    if (resourceType != null && resourceType.nonEmpty) {
      val typeQualifier =
        vocabularies.getSynonymAsQualifier(ModelConstants.DNET_PUBLICATION_RESOURCE, resourceType)
      if (typeQualifier != null)
        return (
          typeQualifier,
          vocabularies.getSynonymAsQualifier(
            ModelConstants.DNET_RESULT_TYPOLOGIES,
            typeQualifier.getClassid
          ),
          resourceType
        )
    }
    null
  }

  def extract_doi(input: String): CrossrefDT = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json: json4s.JValue = parse(input)
    CrossrefDT(doi = (json \ "DOI").extract[String].toLowerCase, json = input, 0)
  }

  def convert(input: CrossrefDT, uw: UnpayWall, vocabularies: VocabularyGroup): List[CrossrefResult] = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json: json4s.JValue = parse(input.json)

    var resultList: List[CrossrefResult] = List()

    val objectType = (json \ "type").extractOrElse[String](null)
    if (objectType == null)
      return resultList
    val typology = getTypeQualifier(objectType, vocabularies)

    if (typology == null)
      return List()

    val result = generateItemFromType(typology._2)
    if (result == null)
      return List()

    mappingResult(result, json, typology._1, typology._3)

    if (result == null || result.getId == null)
      return List()

    val funderList: List[mappingFunder] =
      (json \ "funder").extractOrElse[List[mappingFunder]](List())

    if (funderList.nonEmpty) {
      resultList = resultList ::: mappingFunderToRelations(
        funderList,
        result.getId,
        createCrossrefCollectedFrom(),
        result.getDataInfo,
        result.getLastupdatetimestamp
      ).map(s => CrossrefResult(s.getClass.getSimpleName, mapper.writeValueAsString(s)))
    }

    result match {
      case publication: Publication => convertPublication(publication, json, typology._1)
      case dataset: Dataset         => convertDataset(dataset)
    }

    val doisReference: List[String] = for {
      JObject(reference_json)          <- json \ "reference"
      JField("DOI", JString(doi_json)) <- reference_json
    } yield doi_json

    if (doisReference != null && doisReference.nonEmpty) {
      val citation_relations: List[Relation] = generateCitationRelations(doisReference, result)
      resultList = resultList ::: citation_relations.map(s =>
        CrossrefResult(s.getClass.getSimpleName, mapper.writeValueAsString(s))
      )
    }

    if (uw != null) {
      result.getCollectedfrom.add(createUnpayWallCollectedFrom())
      val i: Instance = new Instance()
      i.setCollectedfrom(createUnpayWallCollectedFrom())
      if (uw.best_oa_location != null) {

        i.setUrl(List(uw.best_oa_location.url).asJava)
        if (uw.best_oa_location.license.isDefined) {
          i.setLicense(field[String](uw.best_oa_location.license.get, null))
        }

        val colour = get_unpaywall_color(uw.oa_status)
        if (colour.isDefined) {
          val a = new AccessRight
          a.setClassid(ModelConstants.ACCESS_RIGHT_OPEN)
          a.setClassname(ModelConstants.ACCESS_RIGHT_OPEN)
          a.setSchemeid(ModelConstants.DNET_ACCESS_MODES)
          a.setSchemename(ModelConstants.DNET_ACCESS_MODES)
          a.setOpenAccessRoute(colour.get)
          i.setAccessright(a)
        }
        i.setPid(result.getPid)
        result.getInstance().add(i)
      }
    }
    if (!filterResult(result))
      List()
    else
      resultList ::: List(result).map(s => CrossrefResult(s.getClass.getSimpleName, mapper.writeValueAsString(s)))

  }

  private def createCiteRelation(source: Result, targetPid: String, targetPidType: String): List[Relation] = {

    val targetId = IdentifierFactory.idFromPid("50", targetPidType, targetPid, true)

    val from = new Relation
    from.setSource(source.getId)
    from.setTarget(targetId)
    from.setRelType(ModelConstants.RESULT_RESULT)
    from.setRelClass(ModelConstants.CITES)
    from.setSubRelType(ModelConstants.CITATION)
    from.setCollectedfrom(source.getCollectedfrom)
    from.setDataInfo(source.getDataInfo)
    from.setLastupdatetimestamp(source.getLastupdatetimestamp)

    List(from)
  }

  def generateCitationRelations(dois: List[String], result: Result): List[Relation] = {
    dois.flatMap(d => createCiteRelation(result, d, "doi"))
  }

  def mappingFunderToRelations(
    funders: List[mappingFunder],
    sourceId: String,
    cf: KeyValue,
    di: DataInfo,
    ts: Long
  ): List[Relation] = {

    val queue = new mutable.Queue[Relation]

    def snsfRule(award: String): String = {
      val tmp1 = StringUtils.substringAfter(award, "_")
      val tmp2 = StringUtils.substringBefore(tmp1, "/")
      logger.debug(s"From $award to $tmp2")
      tmp2

    }

    def extractECAward(award: String): String = {
      val awardECRegex: Regex = "[0-9]{4,9}".r
      if (awardECRegex.findAllIn(award).hasNext)
        return awardECRegex.findAllIn(award).max
      null
    }

    def generateRelation(sourceId: String, targetId: String, relClass: String): Relation = {

      val r = new Relation
      r.setSource(sourceId)
      r.setTarget(targetId)
      r.setRelType(ModelConstants.RESULT_PROJECT)
      r.setRelClass(relClass)
      r.setSubRelType(ModelConstants.OUTCOME)
      r.setCollectedfrom(List(cf).asJava)
      r.setDataInfo(di)
      r.setLastupdatetimestamp(ts)
      r

    }

    def generateSimpleRelationFromAward(
      funder: mappingFunder,
      nsPrefix: String,
      extractField: String => String
    ): Unit = {
      if (funder.award.isDefined && funder.award.get.nonEmpty)
        funder.award.get
          .map(extractField)
          .filter(a => a != null && a.nonEmpty)
          .foreach(award => {
            val targetId = getProjectId(nsPrefix, DHPUtils.md5(award))
            queue += generateRelation(sourceId, targetId, ModelConstants.IS_PRODUCED_BY)
            queue += generateRelation(targetId, sourceId, ModelConstants.PRODUCES)
          })
    }

    def getProjectId(nsPrefix: String, targetId: String): String = {
      s"40|$nsPrefix::$targetId"
    }

    if (funders != null)
      funders.foreach(funder => {
        if (funder.DOI.isDefined && funder.DOI.get.nonEmpty) {

          if (getIrishId(funder.DOI.get).isDefined) {
            val nsPrefix = getIrishId(funder.DOI.get).get.padTo(12, '_')
            val targetId = getProjectId(nsPrefix, "1e5e62235d094afd01cd56e65112fc63")
            queue += generateRelation(sourceId, targetId, ModelConstants.IS_PRODUCED_BY)
            queue += generateRelation(targetId, sourceId, ModelConstants.PRODUCES)
          }

          funder.DOI.get match {
            case "10.13039/100010663" | "10.13039/100010661" | "10.13039/501100007601" | "10.13039/501100000780" |
                "10.13039/100010665" =>
              generateSimpleRelationFromAward(funder, "corda__h2020", extractECAward)
            case "10.13039/100011199" | "10.13039/100004431" | "10.13039/501100004963" | "10.13039/501100000780" =>
              generateSimpleRelationFromAward(funder, "corda_______", extractECAward)
            case "10.13039/501100000781" =>
              generateSimpleRelationFromAward(funder, "corda_______", extractECAward)
              generateSimpleRelationFromAward(funder, "corda__h2020", extractECAward)
              generateSimpleRelationFromAward(funder, "corda_____he", extractECAward)
            case "10.13039/100000001"    => generateSimpleRelationFromAward(funder, "nsf_________", a => a)
            case "10.13039/501100001665" => generateSimpleRelationFromAward(funder, "anr_________", a => a)
            case "10.13039/501100002341" => generateSimpleRelationFromAward(funder, "aka_________", a => a)
            case "10.13039/501100001602" =>
              generateSimpleRelationFromAward(funder, "sfi_________", a => a.replace("SFI", ""))
            case "10.13039/501100000923" => generateSimpleRelationFromAward(funder, "arc_________", a => a)
            case "10.13039/501100000038" =>
              val targetId = getProjectId("nserc_______", "1e5e62235d094afd01cd56e65112fc63")
              queue += generateRelation(sourceId, targetId, ModelConstants.IS_PRODUCED_BY)
              queue += generateRelation(targetId, sourceId, ModelConstants.PRODUCES)
            case "10.13039/501100000155" =>
              val targetId = getProjectId("sshrc_______", "1e5e62235d094afd01cd56e65112fc63")
              queue += generateRelation(sourceId, targetId, ModelConstants.IS_PRODUCED_BY)
              queue += generateRelation(targetId, sourceId, ModelConstants.PRODUCES)
            case "10.13039/501100000024" =>
              val targetId = getProjectId("cihr________", "1e5e62235d094afd01cd56e65112fc63")
              queue += generateRelation(sourceId, targetId, ModelConstants.IS_PRODUCED_BY)
              queue += generateRelation(targetId, sourceId, ModelConstants.PRODUCES)

            case "10.13039/100020031" =>
              val targetId = getProjectId("tara________", "1e5e62235d094afd01cd56e65112fc63")
              queue += generateRelation(sourceId, targetId, ModelConstants.IS_PRODUCED_BY)
              queue += generateRelation(targetId, sourceId, ModelConstants.PRODUCES)

            case "10.13039/501100005416" => generateSimpleRelationFromAward(funder, "rcn_________", a => a)
            case "10.13039/501100002848" => generateSimpleRelationFromAward(funder, "conicytf____", a => a)
            case "10.13039/501100003448" => generateSimpleRelationFromAward(funder, "gsrt________", extractECAward)
            case "10.13039/501100010198" => generateSimpleRelationFromAward(funder, "sgov________", a => a)
            case "10.13039/501100004564" => generateSimpleRelationFromAward(funder, "mestd_______", extractECAward)
            case "10.13039/501100003407" =>
              generateSimpleRelationFromAward(funder, "miur________", a => a)
              val targetId = getProjectId("miur________", "1e5e62235d094afd01cd56e65112fc63")
              queue += generateRelation(sourceId, targetId, ModelConstants.IS_PRODUCED_BY)
              queue += generateRelation(targetId, sourceId, ModelConstants.PRODUCES)
            case "10.13039/501100006588" | "10.13039/501100004488" =>
              generateSimpleRelationFromAward(
                funder,
                "irb_hr______",
                a => a.replaceAll("Project No.", "").replaceAll("HRZZ-", "")
              )
            case "10.13039/501100006769" => generateSimpleRelationFromAward(funder, "rsf_________", a => a)
            case "10.13039/501100001711" => generateSimpleRelationFromAward(funder, "snsf________", snsfRule)
            case "10.13039/501100004410" => generateSimpleRelationFromAward(funder, "tubitakf____", a => a)
            case "10.13039/100004440" =>
              generateSimpleRelationFromAward(funder, "wt__________", a => a)
              val targetId = getProjectId("wt__________", "1e5e62235d094afd01cd56e65112fc63")
              queue += generateRelation(sourceId, targetId, ModelConstants.IS_PRODUCED_BY)
              queue += generateRelation(targetId, sourceId, ModelConstants.PRODUCES)
            //ASAP
            case "10.13039/100018231" => generateSimpleRelationFromAward(funder, "asap________", a => a)
            //CHIST-ERA
            case "10.13039/501100001942" =>
              val targetId = getProjectId("chistera____", "1e5e62235d094afd01cd56e65112fc63")
              queue += generateRelation(sourceId, targetId, ModelConstants.IS_PRODUCED_BY)
              queue += generateRelation(targetId, sourceId, ModelConstants.PRODUCES)
            //HE
            case "10.13039/100018693" | "10.13039/100018694" | "10.13039/100019188" | "10.13039/100019180" |
                "10.13039/100018695" | "10.13039/100019185" | "10.13039/100019186" | "10.13039/100019187" =>
              generateSimpleRelationFromAward(funder, "corda_____he", extractECAward)
            //FCT
            case "10.13039/501100001871" =>
              generateSimpleRelationFromAward(funder, "fct_________", a => a)
            //NHMRC
            case "10.13039/501100000925" =>
              generateSimpleRelationFromAward(funder, "nhmrc_______", a => a)
            //NIH
            case "10.13039/100000002" =>
              generateSimpleRelationFromAward(funder, "nih_________", a => a)
            //NWO
            case "10.13039/501100003246" =>
              generateSimpleRelationFromAward(funder, "nwo_________", a => a)
            //UKRI
            case "10.13039/100014013" | "10.13039/501100000267" | "10.13039/501100000268" | "10.13039/501100000269" |
                "10.13039/501100000266" | "10.13039/501100006041" | "10.13039/501100000265" | "10.13039/501100000270" |
                "10.13039/501100013589" | "10.13039/501100000271" =>
              generateSimpleRelationFromAward(funder, "ukri________", a => a)

            case _ => logger.debug("no match for " + funder.DOI.get)

          }

        } else {
          funder.name match {
            case "European Union’s Horizon 2020 research and innovation program" =>
              generateSimpleRelationFromAward(funder, "corda__h2020", extractECAward)
            case "European Union's" =>
              generateSimpleRelationFromAward(funder, "corda__h2020", extractECAward)
              generateSimpleRelationFromAward(funder, "corda_______", extractECAward)
              generateSimpleRelationFromAward(funder, "corda_____he", extractECAward)
            case "The French National Research Agency (ANR)" | "The French National Research Agency" =>
              generateSimpleRelationFromAward(funder, "anr_________", a => a)
            case "CONICYT, Programa de Formación de Capital Humano Avanzado" =>
              generateSimpleRelationFromAward(funder, "conicytf____", a => a)
            case "Wellcome Trust Masters Fellowship" =>
              generateSimpleRelationFromAward(funder, "wt__________", a => a)
              val targetId = getProjectId("wt__________", "1e5e62235d094afd01cd56e65112fc63")
              queue += generateRelation(sourceId, targetId, ModelConstants.IS_PRODUCED_BY)
              queue += generateRelation(targetId, sourceId, ModelConstants.PRODUCES)
            case _ => logger.debug("no match for " + funder.name)

          }
        }

      })
    queue.toList
  }

  def convertDataset(dataset: Dataset): Unit = {
    // TODO check if there are other info to map into the Dataset
  }

  def convertPublication(publication: Publication, json: JValue, cobjCategory: Qualifier): Unit = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    val containerTitles = for { JString(ct) <- json \ "container-title" } yield ct

    //Mapping book
    if (cobjCategory.getClassname.toLowerCase.contains("book")) {
      val ISBN = for { JString(isbn) <- json \ "ISBN" } yield isbn
      if (ISBN.nonEmpty && containerTitles.nonEmpty) {
        val source = s"${containerTitles.head} ISBN: ${ISBN.head}"
        if (publication.getSource != null) {
          val l: List[Field[String]] = publication.getSource.asScala.toList
          val ll: List[Field[String]] = l ::: List(field(source, null))
          publication.setSource(ll.asJava)
        } else
          publication.setSource(List(field(source, null)).asJava)
      }
    } else {
      // Mapping Journal

      val issnInfos = for {
        JArray(issn_types)           <- json \ "issn-type"
        JObject(issn_type)           <- issn_types
        JField("type", JString(tp))  <- issn_type
        JField("value", JString(vl)) <- issn_type
      } yield Tuple2(tp, vl)

      val volume = (json \ "volume").extractOrElse[String](null)
      if (containerTitles.nonEmpty) {
        val journal = new Journal
        journal.setName(containerTitles.head)
        if (issnInfos.nonEmpty) {

          issnInfos.foreach(tp => {
            tp._1 match {
              case "electronic" => journal.setIssnOnline(tp._2)
              case "print"      => journal.setIssnPrinted(tp._2)
            }
          })
        }
        journal.setVol(volume)
        val page = (json \ "page").extractOrElse[String](null)
        if (page != null) {
          val pp = page.split("-")
          if (pp.nonEmpty)
            journal.setSp(pp.head)
          if (pp.size > 1)
            journal.setEp(pp(1))
        }
        publication.setJournal(journal)
      }
    }
  }

  def extractDate(dt: String, datePart: List[List[Int]]): String = {
    if (StringUtils.isNotBlank(dt))
      return GraphCleaningFunctions.cleanDate(dt)
    if (datePart != null && datePart.size == 1) {
      val res = datePart.head
      if (res.size == 3) {
        val dp = f"${res.head}-${res(1)}%02d-${res(2)}%02d"
        if (dp.length == 10) {
          return GraphCleaningFunctions.cleanDate(dp)
        }
      } else if (res.size == 2) {
        val dp = f"${res.head}-${res(1)}%02d-01"
        return GraphCleaningFunctions.cleanDate(dp)
      } else if (res.size == 1) {
        return GraphCleaningFunctions.cleanDate(s"${res.head}-01-01")
      }
    }
    null

  }

  def generateDate(
    dt: String,
    datePart: List[List[Int]],
    classId: String,
    schemeId: String
  ): StructuredProperty = {
    val dp = extractDate(dt, datePart)
    if (StringUtils.isNotBlank(dp))
      return structuredProperty(dp, qualifier(classId, classId, schemeId, schemeId), null)
    null
  }

  def generateItemFromType(objectType: Qualifier): Result = {
    if (objectType.getClassid.equalsIgnoreCase("publication")) {
      val item = new Publication
      item.setResourcetype(objectType)
      return item
    } else if (objectType.getClassid.equalsIgnoreCase("dataset")) {
      val item = new Dataset
      item.setResourcetype(objectType)
      return item
    } else if (objectType.getClassid.equalsIgnoreCase("software")) {
      val item = new Software
      item.setResourcetype(objectType)
      return item
    } else if (objectType.getClassid.equalsIgnoreCase("OtherResearchProduct")) {
      val item = new OtherResearchProduct
      item.setResourcetype(objectType)
      return item
    }
    null
  }

}
