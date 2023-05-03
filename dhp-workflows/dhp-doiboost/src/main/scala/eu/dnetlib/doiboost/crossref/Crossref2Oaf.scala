package eu.dnetlib.doiboost.crossref

import com.google.common.collect.Lists
import eu.dnetlib.dhp.schema.common.ModelConstants
import eu.dnetlib.dhp.schema.oaf._
import eu.dnetlib.dhp.schema.oaf.utils.{GraphCleaningFunctions, IdentifierFactory, OafMapperUtils, PidType}
import eu.dnetlib.dhp.utils.DHPUtils
import eu.dnetlib.doiboost.DoiBoostMappingUtil
import eu.dnetlib.doiboost.DoiBoostMappingUtil._
import org.apache.commons.lang.StringUtils
import org.json4s
import org.json4s.DefaultFormats
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods._
import org.slf4j.{Logger, LoggerFactory}

import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.matching.Regex

case class CrossrefDT(doi: String, json: String, timestamp: Long) {}

case class mappingAffiliation(name: String) {}

case class mappingAuthor(
  given: Option[String],
  family: String,
  sequence: Option[String],
  ORCID: Option[String],
  affiliation: Option[mappingAffiliation]
) {}

case class mappingFunder(name: String, DOI: Option[String], award: Option[List[String]]) {}

case object Crossref2Oaf {
  val logger: Logger = LoggerFactory.getLogger(Crossref2Oaf.getClass)

  val mappingCrossrefType = Map(
    "book-section"        -> "publication",
    "book"                -> "publication",
    "book-chapter"        -> "publication",
    "book-part"           -> "publication",
    "book-series"         -> "publication",
    "book-set"            -> "publication",
    "book-track"          -> "publication",
    "edited-book"         -> "publication",
    "reference-book"      -> "publication",
    "monograph"           -> "publication",
    "journal-article"     -> "publication",
    "dissertation"        -> "publication",
    "other"               -> "publication",
    "peer-review"         -> "publication",
    "proceedings"         -> "publication",
    "proceedings-article" -> "publication",
    "reference-entry"     -> "publication",
    "report"              -> "publication",
    "report-series"       -> "publication",
    "standard"            -> "publication",
    "standard-series"     -> "publication",
    "posted-content"      -> "publication",
    "dataset"             -> "dataset"
  )

  val mappingCrossrefSubType = Map(
    "book-section"        -> "0013 Part of book or chapter of book",
    "book"                -> "0002 Book",
    "book-chapter"        -> "0013 Part of book or chapter of book",
    "book-part"           -> "0013 Part of book or chapter of book",
    "book-series"         -> "0002 Book",
    "book-set"            -> "0002 Book",
    "book-track"          -> "0002 Book",
    "edited-book"         -> "0002 Book",
    "reference-book"      -> "0002 Book",
    "monograph"           -> "0002 Book",
    "journal-article"     -> "0001 Article",
    "dissertation"        -> "0044 Thesis",
    "other"               -> "0038 Other literature type",
    "peer-review"         -> "0015 Review",
    "proceedings"         -> "0004 Conference object",
    "proceedings-article" -> "0004 Conference object",
    "reference-entry"     -> "0013 Part of book or chapter of book",
    "report"              -> "0017 Report",
    "report-series"       -> "0017 Report",
    "standard"            -> "0038 Other literature type",
    "standard-series"     -> "0038 Other literature type",
    "dataset"             -> "0021 Dataset",
    "preprint"            -> "0016 Preprint",
    "report"              -> "0017 Report"
  )

  def mappingResult(result: Result, json: JValue, cobjCategory: String): Result = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats

    //MAPPING Crossref DOI into PID
    val doi: String = DoiBoostMappingUtil.normalizeDoi((json \ "DOI").extract[String])
    result.setPid(List(createSP(doi, "doi", ModelConstants.DNET_PID_TYPES)).asJava)

    //MAPPING Crossref DOI into OriginalId
    //and Other Original Identifier of dataset like clinical-trial-number
    val clinicalTrialNumbers = for (JString(ctr) <- json \ "clinical-trial-number") yield ctr
    val alternativeIds = for (JString(ids) <- json \ "alternative-id") yield ids
    val tmp = clinicalTrialNumbers ::: alternativeIds ::: List(doi)

    val originalIds = new util.ArrayList(tmp.filter(id => id != null).asJava)
    result.setOriginalId(originalIds)

    // Add DataInfo
    result.setDataInfo(entityDataInfo)

    result.setLastupdatetimestamp((json \ "indexed" \ "timestamp").extract[Long])
    result.setDateofcollection((json \ "indexed" \ "date-time").extract[String])

    result.setCollectedfrom(List(collectedFrom).asJava)

    // Publisher ( Name of work's publisher mapped into  Result/Publisher)
    val publisher = (json \ "publisher").extractOrElse[String](null)
    if (publisher != null && publisher.nonEmpty)
      result.setPublisher(OafMapperUtils.publisher(publisher))

    // TITLE
    val mainTitles =
      for { JString(title) <- json \ "title" if title.nonEmpty } yield createSP(
        title,
        "main title",
        ModelConstants.DNET_DATACITE_TITLE
      )
    val originalTitles = for {
      JString(title) <- json \ "original-title" if title.nonEmpty
    } yield createSP(title, "alternative title", ModelConstants.DNET_DATACITE_TITLE)
    val shortTitles = for {
      JString(title) <- json \ "short-title" if title.nonEmpty
    } yield createSP(title, "alternative title", ModelConstants.DNET_DATACITE_TITLE)
    val subtitles =
      for { JString(title) <- json \ "subtitle" if title.nonEmpty } yield createSP(
        title,
        "subtitle",
        ModelConstants.DNET_DATACITE_TITLE
      )
    result.setTitle((mainTitles ::: originalTitles ::: shortTitles ::: subtitles).asJava)

    // DESCRIPTION
    val descriptionList =
      for { JString(description) <- json \ "abstract" } yield description
    result.setDescription(descriptionList.asJava)

    // Source
    val sourceList = for {
      JString(source) <- json \ "source" if source != null && source.nonEmpty
    } yield source
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
      result.setDateofacceptance(issuedDate)
    } else {
      result.setDateofacceptance(createdDate.getValue)
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
        subjectList.map(s => createSubject(s, "keyword", ModelConstants.DNET_SUBJECT_TYPOLOGIES)).asJava
      )
    }

    //Mapping Author
    val authorList: List[mappingAuthor] =
      (json \ "author").extractOrElse[List[mappingAuthor]](List())

    val sorted_list = authorList.sortWith((a: mappingAuthor, b: mappingAuthor) =>
      a.sequence.isDefined && a.sequence.get.equalsIgnoreCase("first")
    )

    result.setAuthor(sorted_list.zipWithIndex.map { case (a, index) =>
      generateAuhtor(a.given.orNull, a.family, a.ORCID.orNull, index)
    }.asJava)

    // Mapping instance
    val instance = new Instance()
    val license = for {
      JObject(license)                                    <- json \ "license"
      JField("URL", JString(lic))                         <- license
      JField("content-version", JString(content_version)) <- license
    } yield (OafMapperUtils.license(lic), content_version)
    val l = license.filter(d => StringUtils.isNotBlank(d._1.getUrl))
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
          ModelConstants.DNET_REVIEW_LEVELS
        )
      )
    }

    instance.setAccessright(
      decideAccessRight(instance.getLicense.getUrl, result.getDateofacceptance)
    )
    instance.setInstancetype(
      OafMapperUtils.qualifier(
        cobjCategory.substring(0, 4),
        cobjCategory.substring(5),
        ModelConstants.DNET_PUBLICATION_RESOURCE
      )
    )
    result.setResourcetype(
      OafMapperUtils.qualifier(
        cobjCategory.substring(0, 4),
        cobjCategory.substring(5),
        ModelConstants.DNET_PUBLICATION_RESOURCE
      )
    )

    instance.setCollectedfrom(collectedFrom)
    if (StringUtils.isNotBlank(issuedDate)) {
      instance.setDateofacceptance(issuedDate)
    } else {
      instance.setDateofacceptance(createdDate.getValue)
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

  def generateAuhtor(given: String, family: String, orcid: String, index: Int): Author = {
    val a = new Author
    a.setName(given)
    a.setSurname(family)
    a.setFullname(s"$given $family")
    a.setRank(index + 1)
    if (StringUtils.isNotBlank(orcid))
      a.setPid(
        List(
          OafMapperUtils.authorPid(
            orcid,
            OafMapperUtils.qualifier(ModelConstants.ORCID_PENDING, ModelConstants.ORCID_PENDING, ModelConstants.DNET_PID_TYPES),
            dataInfo
          )
        ).asJava
      )

    a
  }

  def convert(input: String): List[Oaf] = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json: json4s.JValue = parse(input)

    var resultList: List[Oaf] = List()

    val objectType = (json \ "type").extractOrElse[String](null)
    val objectSubType = (json \ "subtype").extractOrElse[String](null)
    if (objectType == null)
      return resultList

    val result = generateItemFromType(objectType, objectSubType)
    if (result == null)
      return List()
    val cOBJCategory = mappingCrossrefSubType.getOrElse(
      objectType,
      mappingCrossrefSubType.getOrElse(objectSubType, "0038 Other literature type")
    )
    mappingResult(result, json, cOBJCategory)
    if (result == null || result.getId == null)
      return List()

    val funderList: List[mappingFunder] =
      (json \ "funder").extractOrElse[List[mappingFunder]](List())

    if (funderList.nonEmpty) {
      resultList = resultList ::: mappingFunderToRelations(
        funderList,
        result.getId
      )
    }

    result match {
      case publication: Publication => convertPublication(publication, json, cOBJCategory)
      case dataset: Dataset         => convertDataset(dataset)
    }

    val doisReference: List[String] = for {
      JObject(reference_json)          <- json \ "reference"
      JField("DOI", JString(doi_json)) <- reference_json
    } yield doi_json

    if (doisReference != null && doisReference.nonEmpty) {
      val citation_relations: List[Relation] = generateCitationRelations(doisReference, result)
      resultList = resultList ::: citation_relations
    }
    resultList = resultList ::: List(result)
    resultList
  }

  private def createCiteRelation(sourceId: String, targetPid: String, targetPidType: String): List[Relation] = {

    val targetId = IdentifierFactory.idFromPid("50", targetPidType, targetPid, true)

    val rel = new Relation
    rel.setSource(sourceId)
    rel.setTarget(targetId)
    rel.setRelType(Relation.RELTYPE.resultResult)
    rel.setRelClass(Relation.RELCLASS.Cites)
    rel.setSubRelType(Relation.SUBRELTYPE.citation)
    rel.setProvenance(Lists.newArrayList(OafMapperUtils.getProvenance(collectedFrom, dataInfo)))

    List(rel)
  }

  def generateCitationRelations(dois: List[String], result: Result): List[Relation] = {
    dois.flatMap(doi => createCiteRelation(result.getId, doi, PidType.doi.toString))
  }

  def mappingFunderToRelations(
    funders: List[mappingFunder],
    sourceId: String
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

    def generateRelation(sourceId: String, targetId: String, relClass: Relation.RELCLASS): Relation = {

      val r = new Relation
      r.setSource(sourceId)
      r.setTarget(targetId)
      r.setRelType(Relation.RELTYPE.resultProject)
      r.setRelClass(relClass)
      r.setSubRelType(Relation.SUBRELTYPE.outcome)

      r.setProvenance(Lists.newArrayList(OafMapperUtils.getProvenance(collectedFrom, dataInfo)))
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
            queue += generateRelation(sourceId, targetId, Relation.RELCLASS.isProducedBy)
            queue += generateRelation(targetId, sourceId, Relation.RELCLASS.produces)
          })
    }

    def getProjectId(nsPrefix: String, targetId: String): String = {
      s"40|$nsPrefix::$targetId"
    }

    if (funders != null)
      funders.foreach(funder => {
        if (funder.DOI.isDefined && funder.DOI.get.nonEmpty) {
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
              queue += generateRelation(sourceId, targetId, Relation.RELCLASS.isProducedBy)
              queue += generateRelation(targetId, sourceId, Relation.RELCLASS.produces)
            case "10.13039/501100000155" =>
              val targetId = getProjectId("sshrc_______", "1e5e62235d094afd01cd56e65112fc63")
              queue += generateRelation(sourceId, targetId, Relation.RELCLASS.isProducedBy)
              queue += generateRelation(targetId, sourceId, Relation.RELCLASS.produces)
            case "10.13039/501100000024" =>
              val targetId = getProjectId("cihr________", "1e5e62235d094afd01cd56e65112fc63")
              queue += generateRelation(sourceId, targetId, Relation.RELCLASS.isProducedBy)
              queue += generateRelation(targetId, sourceId, Relation.RELCLASS.produces)

            case "10.13039/100020031" =>
              val targetId = getProjectId("tara________", "1e5e62235d094afd01cd56e65112fc63")
              queue += generateRelation(sourceId, targetId, Relation.RELCLASS.isProducedBy)
              queue += generateRelation(targetId, sourceId, Relation.RELCLASS.produces)

            case "10.13039/501100005416" => generateSimpleRelationFromAward(funder, "rcn_________", a => a)
            case "10.13039/501100002848" => generateSimpleRelationFromAward(funder, "conicytf____", a => a)
            case "10.13039/501100003448" => generateSimpleRelationFromAward(funder, "gsrt________", extractECAward)
            case "10.13039/501100010198" => generateSimpleRelationFromAward(funder, "sgov________", a => a)
            case "10.13039/501100004564" => generateSimpleRelationFromAward(funder, "mestd_______", extractECAward)
            case "10.13039/501100003407" =>
              generateSimpleRelationFromAward(funder, "miur________", a => a)
              val targetId = getProjectId("miur________", "1e5e62235d094afd01cd56e65112fc63")
              queue += generateRelation(sourceId, targetId, Relation.RELCLASS.isProducedBy)
              queue += generateRelation(targetId, sourceId, Relation.RELCLASS.produces)
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
              queue += generateRelation(sourceId, targetId, Relation.RELCLASS.isProducedBy)
              queue += generateRelation(targetId, sourceId, Relation.RELCLASS.produces)
            //ASAP
            case "10.13039/100018231" => generateSimpleRelationFromAward(funder, "asap________", a => a)
            //CHIST-ERA
            case "10.13039/501100001942" =>
              val targetId = getProjectId("chistera____", "1e5e62235d094afd01cd56e65112fc63")
              queue += generateRelation(sourceId, targetId, Relation.RELCLASS.isProducedBy)
              queue += generateRelation(targetId, sourceId, Relation.RELCLASS.produces)
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
              queue += generateRelation(sourceId, targetId, Relation.RELCLASS.isProducedBy)
              queue += generateRelation(targetId, sourceId, Relation.RELCLASS.produces)
            case _ => logger.debug("no match for " + funder.name)

          }
        }

      })
    queue.toList
  }

  def convertDataset(dataset: Dataset): Unit = {
    // TODO check if there are other info to map into the Dataset
  }

  def convertPublication(publication: Publication, json: JValue, cobjCategory: String): Unit = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    val containerTitles = for { JString(ct) <- json \ "container-title" } yield ct

    //Mapping book
    if (cobjCategory.toLowerCase.contains("book")) {
      val ISBN = for { JString(isbn) <- json \ "ISBN" } yield isbn
      if (ISBN.nonEmpty && containerTitles.nonEmpty) {
        val source = s"${containerTitles.head} ISBN: ${ISBN.head}"
        if (publication.getSource != null) {
          val l: List[String] = publication.getSource.asScala.toList
          val ll: List[String] = l ::: List(source)
          publication.setSource(ll.asJava)
        } else
          publication.setSource(List(source).asJava)
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
      return createSP(dp, classId, schemeId)
    null
  }

  def generateItemFromType(objectType: String, objectSubType: String): Result = {
    if (mappingCrossrefType.contains(objectType)) {
      if (mappingCrossrefType(objectType).equalsIgnoreCase("publication"))
        return new Publication()
      if (mappingCrossrefType(objectType).equalsIgnoreCase("dataset"))
        return new Dataset()
    }
    null
  }

}
