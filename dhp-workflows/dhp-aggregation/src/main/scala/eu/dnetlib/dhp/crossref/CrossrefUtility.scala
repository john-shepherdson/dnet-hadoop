package eu.dnetlib.dhp.crossref

import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup
import eu.dnetlib.dhp.schema.common.ModelConstants
import eu.dnetlib.dhp.schema.common.ModelConstants.OPEN_ACCESS_RIGHT
import eu.dnetlib.dhp.schema.oaf._
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils._
import eu.dnetlib.dhp.schema.oaf.utils._
import org.apache.commons.lang.StringUtils
import org.json4s
import org.json4s.DefaultFormats
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods.parse
import org.slf4j.{Logger, LoggerFactory}

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex

case class CrossrefDT(doi: String, json: String, timestamp: Long) {}

case class CrossrefAuthor(givenName: String, familyName: String, ORCID: String, sequence: String, rank: Int) {}

case class mappingFunder(name: String, DOI: Option[String], award: Option[List[String]]) {}

object CrossrefUtility {
  val CROSSREF_COLLECTED_FROM = keyValue(ModelConstants.CROSSREF_ID, ModelConstants.CROSSREF_NAME)

  val logger: Logger = LoggerFactory.getLogger(getClass)

  def convert(input: String, vocabularies: VocabularyGroup): List[Oaf] = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json: json4s.JValue = parse(input)

    var resultList: List[Oaf] = List()

    val objectType = (json \ "type").extractOrElse[String](null)
    if (objectType == null)
      return resultList

    val resultWithType = generateItemFromType(objectType, vocabularies)
    if (resultWithType == null)
      return List()

    val result = resultWithType._1
    val cOBJCategory = resultWithType._2
    val className = resultWithType._3
    mappingResult(result, json, cOBJCategory, className)
    if (result == null || result.getId == null)
      return List()

    val funderList: List[mappingFunder] =
      (json \ "funder").extractOrElse[List[mappingFunder]](List())

    if (funderList.nonEmpty) {
      resultList = resultList ::: mappingFunderToRelations(funderList, result)
    }
    resultList = resultList ::: List(result)
    resultList
  }

  private def createRelation(sourceId: String, targetId: String, relClass: Relation.RELCLASS): Relation = {
    val r = new Relation

    //TODO further inspect
    r.setSource(sourceId)
    r.setTarget(targetId)
    r.setRelType(Relation.RELTYPE.resultProject)
    r.setRelClass(relClass)
    r.setSubRelType(Relation.SUBRELTYPE.outcome)
    r.setProvenance(List(OafMapperUtils.getProvenance(CROSSREF_COLLECTED_FROM, null)).asJava)
    r
  }

  private def generateSimpleRelationFromAward(
    funder: mappingFunder,
    nsPrefix: String,
    extractField: String => String,
    source: Result
  ): List[Relation] = {
    if (funder.award.isDefined && funder.award.get.nonEmpty)
      funder.award.get
        .map(extractField)
        .filter(a => a != null && a.nonEmpty)
        .map(award => {
          val targetId = IdentifierFactory.createOpenaireId("project", s"$nsPrefix::$award", true)
          createRelation(targetId, source.getId, Relation.RELCLASS.produces)
        })
    else List()
  }

  private def extractECAward(award: String): String = {
    val awardECRegex: Regex = "[0-9]{4,9}".r
    if (awardECRegex.findAllIn(award).hasNext)
      return awardECRegex.findAllIn(award).max
    null
  }

  private def snsfRule(award: String): String = {
    val tmp1 = StringUtils.substringAfter(award, "_")
    val tmp2 = StringUtils.substringBefore(tmp1, "/")
    tmp2

  }

  private def mappingFunderToRelations(funders: List[mappingFunder], result: Result): List[Relation] = {
    var relList: List[Relation] = List()

    if (funders != null)
      funders.foreach(funder => {
        if (funder.DOI.isDefined && funder.DOI.get.nonEmpty) {
          funder.DOI.get match {
            case "10.13039/100010663" | "10.13039/100010661" | "10.13039/501100007601" | "10.13039/501100000780" |
                "10.13039/100010665" =>
              relList = relList ::: generateSimpleRelationFromAward(funder, "corda__h2020", extractECAward, result)
            case "10.13039/100011199" | "10.13039/100004431" | "10.13039/501100004963" | "10.13039/501100000780" =>
              relList = relList ::: generateSimpleRelationFromAward(funder, "corda_______", extractECAward, result)
            case "10.13039/501100000781" =>
              relList = relList ::: generateSimpleRelationFromAward(funder, "corda_______", extractECAward, result)
              relList = relList ::: generateSimpleRelationFromAward(funder, "corda__h2020", extractECAward, result)
            case "10.13039/100000001" =>
              relList = relList ::: generateSimpleRelationFromAward(funder, "nsf_________", a => a, result)
            case "10.13039/501100001665" =>
              relList = relList ::: generateSimpleRelationFromAward(funder, "anr_________", a => a, result)
            case "10.13039/501100002341" =>
              relList = relList ::: generateSimpleRelationFromAward(funder, "aka_________", a => a, result)
            case "10.13039/501100001602" =>
              relList =
                relList ::: generateSimpleRelationFromAward(funder, "sfi_________", a => a.replace("SFI", ""), result)
            case "10.13039/501100000923" =>
              relList = relList ::: generateSimpleRelationFromAward(funder, "arc_________", a => a, result)
            case "10.13039/501100000038" =>
              val targetId =
                IdentifierFactory.createOpenaireId("project", "nserc_______::1e5e62235d094afd01cd56e65112fc63", false)
              relList = relList ::: List(createRelation(targetId, result.getId, Relation.RELCLASS.produces))
            case "10.13039/501100000155" =>
              val targetId =
                IdentifierFactory.createOpenaireId("project", "sshrc_______::1e5e62235d094afd01cd56e65112fc63", false)
              relList = relList ::: List(createRelation(targetId, result.getId, Relation.RELCLASS.produces))
            case "10.13039/501100000024" =>
              val targetId =
                IdentifierFactory.createOpenaireId("project", "cihr________::1e5e62235d094afd01cd56e65112fc63", false)
              relList = relList ::: List(createRelation(targetId, result.getId, Relation.RELCLASS.produces))
            case "10.13039/501100002848" =>
              relList = relList ::: generateSimpleRelationFromAward(funder, "conicytf____", a => a, result)
            case "10.13039/501100003448" =>
              relList = relList ::: generateSimpleRelationFromAward(funder, "gsrt________", extractECAward, result)
            case "10.13039/501100010198" =>
              relList = relList ::: generateSimpleRelationFromAward(funder, "sgov________", a => a, result)
            case "10.13039/501100004564" =>
              relList = relList ::: generateSimpleRelationFromAward(funder, "mestd_______", extractECAward, result)
            case "10.13039/501100003407" =>
              relList = relList ::: generateSimpleRelationFromAward(funder, "miur________", a => a, result)
              val targetId =
                IdentifierFactory.createOpenaireId("project", "miur________::1e5e62235d094afd01cd56e65112fc63", false)
              relList = relList ::: List(createRelation(targetId, result.getId, Relation.RELCLASS.produces))
            case "10.13039/501100006588" | "10.13039/501100004488" =>
              relList = relList ::: generateSimpleRelationFromAward(
                funder,
                "irb_hr______",
                a => a.replaceAll("Project No.", "").replaceAll("HRZZ-", ""),
                result
              )
            case "10.13039/501100006769" =>
              relList = relList ::: generateSimpleRelationFromAward(funder, "rsf_________", a => a, result)
            case "10.13039/501100001711" =>
              relList = relList ::: generateSimpleRelationFromAward(funder, "snsf________", snsfRule, result)
            case "10.13039/501100004410" =>
              relList = relList ::: generateSimpleRelationFromAward(funder, "tubitakf____", a => a, result)
            case "10.13039/100004440" =>
              relList = relList ::: generateSimpleRelationFromAward(funder, "wt__________", a => a, result)
              val targetId =
                IdentifierFactory.createOpenaireId("project", "wt__________::1e5e62235d094afd01cd56e65112fc63", false)
              relList = relList ::: List(createRelation(targetId, result.getId, Relation.RELCLASS.produces))
            case _ => logger.debug("no match for " + funder.DOI.get)

          }

        } else {
          funder.name match {
            case "European Union’s Horizon 2020 research and innovation program" =>
              relList = relList ::: generateSimpleRelationFromAward(funder, "corda__h2020", extractECAward, result)
            case "European Union's" =>
              relList = relList ::: generateSimpleRelationFromAward(funder, "corda__h2020", extractECAward, result)
              relList = relList ::: generateSimpleRelationFromAward(funder, "corda_______", extractECAward, result)
            case "The French National Research Agency (ANR)" | "The French National Research Agency" =>
              relList = relList ::: generateSimpleRelationFromAward(funder, "anr_________", a => a, result)
            case "CONICYT, Programa de Formación de Capital Humano Avanzado" =>
              relList = relList ::: generateSimpleRelationFromAward(funder, "conicytf____", extractECAward, result)
            case "Wellcome Trust Masters Fellowship" =>
              relList = relList ::: generateSimpleRelationFromAward(funder, "wt__________", a => a, result)
              val targetId =
                IdentifierFactory.createOpenaireId("project", "wt__________::1e5e62235d094afd01cd56e65112fc63", false)
              relList = relList ::: List(createRelation(targetId, result.getId, Relation.RELCLASS.produces))
            case _ => logger.debug("no match for " + funder.name)

          }
        }

      })
    relList

  }

  private def mappingResult(result: Result, json: JValue, cobjCategory: String, className: String): Result = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats

    //MAPPING Crossref DOI into PID
    val doi: String = CleaningFunctions.normalizePidValue(ModelConstants.DOI, (json \ "DOI").extract[String])

    result.setPid(
      List(
        structuredProperty(doi, PidType.doi.toString, PidType.doi.toString, ModelConstants.DNET_PID_TYPES)
      ).asJava
    )

    //MAPPING Crossref DOI into OriginalId
    //and Other Original Identifier of dataset like clinical-trial-number
    val clinicalTrialNumbers: List[String] = for (JString(ctr) <- json \ "clinical-trial-number") yield ctr
    val alternativeIds: List[String] = for (JString(ids) <- json \ "alternative-id") yield ids
    val tmp = clinicalTrialNumbers ::: alternativeIds ::: List(doi)

    result.setOriginalId(tmp.filter(id => id != null).asJava)

    // Add DataInfo
    result.setDataInfo(dataInfo(false, false, 0.9f, null, false, ModelConstants.REPOSITORY_PROVENANCE_ACTIONS))

    result.setLastupdatetimestamp((json \ "indexed" \ "timestamp").extract[Long])
    result.setDateofcollection((json \ "indexed" \ "date-time").extract[String])

    result.setCollectedfrom(List(CROSSREF_COLLECTED_FROM).asJava)

    // Publisher ( Name of work's publisher mapped into  Result/Publisher)
    val publisher = (json \ "publisher").extractOrElse[String](null)
    if (publisher != null && publisher.nonEmpty)
      result.setPublisher(new Publisher(publisher))

    // TITLE
    val mainTitles =
      for { JString(title) <- json \ "title" if title.nonEmpty } yield structuredProperty(
        title,
        ModelConstants.MAIN_TITLE_QUALIFIER
      )
    val originalTitles = for {
      JString(title) <- json \ "original-title" if title.nonEmpty
    } yield structuredProperty(title, ModelConstants.ALTERNATIVE_TITLE_QUALIFIER)
    val shortTitles = for {
      JString(title) <- json \ "short-title" if title.nonEmpty
    } yield structuredProperty(title, ModelConstants.ALTERNATIVE_TITLE_QUALIFIER)
    val subtitles =
      for { JString(title) <- json \ "subtitle" if title.nonEmpty } yield structuredProperty(
        title,
        ModelConstants.SUBTITLE_QUALIFIER
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
        subjectList
          .map(s =>
            OafMapperUtils.subject(
              s,
              OafMapperUtils.qualifier(
                ModelConstants.DNET_SUBJECT_KEYWORD,
                ModelConstants.DNET_SUBJECT_KEYWORD,
                ModelConstants.DNET_SUBJECT_TYPOLOGIES
              ),
              null
            )
          )
          .asJava
      )
    }

    //Mapping Author
    val authorList: List[CrossrefAuthor] =
      for {
        JObject(author)                       <- json \ "author"
        JField("ORCID", JString(orcid))       <- author
        JField("given", JString(givenName))   <- author
        JField("family", JString(familyName)) <- author
        JField("sequence", JString(sequence)) <- author
      } yield CrossrefAuthor(
        givenName = givenName,
        familyName = familyName,
        ORCID = orcid,
        sequence = sequence,
        rank = 0
      )

    result.setAuthor(
      authorList
        .sortWith((a, b) => {
          if (a.sequence.equalsIgnoreCase("first"))
            true
          else if (b.sequence.equalsIgnoreCase("first"))
            false
          else a.familyName < b.familyName
        })
        .zipWithIndex
        .map(k => k._1.copy(rank = k._2))
        .map(k => generateAuthor(k))
        .asJava
    )

    // Mapping instance
    val instance = new Instance()
    val license = for {
      JObject(license)                                    <- json \ "license"
      JField("URL", JString(lic))                         <- license
      JField("content-version", JString(content_version)) <- license
    } yield (new License(lic), content_version)
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

    if (instance.getLicense != null)
      instance.setAccessright(
        decideAccessRight(instance.getLicense.getUrl, result.getDateofacceptance)
      )
    instance.setInstancetype(
      OafMapperUtils.qualifier(
        cobjCategory,
        className,
        ModelConstants.DNET_PUBLICATION_RESOURCE
      )
    )
    result.setResourcetype(
      OafMapperUtils.qualifier(
        cobjCategory,
        className,
        ModelConstants.DNET_PUBLICATION_RESOURCE
      )
    )

    instance.setCollectedfrom(CROSSREF_COLLECTED_FROM)
    if (StringUtils.isNotBlank(issuedDate)) {
      instance.setDateofacceptance(issuedDate)
    } else {
      instance.setDateofacceptance(createdDate.getValue)
    }
    val s: List[String] = List("https://doi.org/" + doi)
    if (s.nonEmpty) {
      instance.setUrl(s.asJava)
    }
    val containerTitles = for { JString(ct) <- json \ "container-title" } yield ct
    //Mapping book
    if (className.toLowerCase.contains("book")) {
      val ISBN = for { JString(isbn) <- json \ "ISBN" } yield isbn
      if (ISBN.nonEmpty && containerTitles.nonEmpty) {
        val source = s"${containerTitles.head} ISBN: ${ISBN.head}"
        if (result.getSource != null) {
          val l: List[String] = result.getSource.asScala.toList ::: List(source)
          result.setSource(l.asJava)
        } else
          result.setSource(List(source).asJava)
      }
    } else {
      // Mapping Journal
      val issnInfos = for {
        JObject(issn_type)           <- json \ "issn-type"
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
        result.setJournal(journal)
      }
    }

    result.setInstance(List(instance).asJava)
    result.setId("ID")
    result.setId(IdentifierFactory.createIdentifier(result, true))
    if (result.getId == null || "ID".equalsIgnoreCase(result.getId))
      null
    else
      result
  }

  def decideAccessRight(license: String, date: String): AccessRight = {
    if (license == null || license.isEmpty) {
      //Default value Unknown
      return ModelConstants.UNKNOWN_ACCESS_RIGHT();
    }
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

      val oaq: AccessRight = ModelConstants.OPEN_ACCESS_RIGHT()
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
          val oaq: AccessRight = ModelConstants.OPEN_ACCESS_RIGHT()
          oaq.setOpenAccessRoute(OpenAccessRoute.hybrid)
          return oaq
        } else {
          return ModelConstants.EMBARGOED_ACCESS_RIGHT()
        }
      } catch {
        case _: Exception => {
          try {
            val pub_date =
              LocalDate.parse(date, DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"))
            if (((now.toEpochDay - pub_date.toEpochDay) / 365.0) > 1) {
              val oaq: AccessRight = OPEN_ACCESS_RIGHT()
              oaq.setOpenAccessRoute(OpenAccessRoute.hybrid)
              return oaq
            } else {
              return ModelConstants.EMBARGOED_ACCESS_RIGHT()
            }
          } catch {
            case _: Exception => return ModelConstants.CLOSED_ACCESS_RIGHT()
          }
        }

      }

    }

    ModelConstants.CLOSED_ACCESS_RIGHT()
  }

  private def extractDate(dt: String, datePart: List[List[Int]]): String = {
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

  private def generateDate(
    dt: String,
    datePart: List[List[Int]],
    classId: String,
    schemeId: String
  ): StructuredProperty = {
    val dp = extractDate(dt, datePart)
    if (StringUtils.isNotBlank(dp))
      structuredProperty(dp, classId, classId, schemeId)
    else
      null
  }

  private def generateItemFromType(objectType: String, vocabularies: VocabularyGroup): (Result, String, String) = {
    val term = vocabularies.getSynonymAsQualifier(ModelConstants.DNET_PUBLICATION_RESOURCE, objectType)
    if (term != null) {
      val resourceType =
        vocabularies.getSynonymAsQualifier(ModelConstants.DNET_RESULT_TYPOLOGIES, term.getClassid).getClassname

      resourceType match {
        case "publication"          => (new Publication, resourceType, term.getClassname)
        case "dataset"              => (new Dataset, resourceType, term.getClassname)
        case "software"             => (new Software, resourceType, term.getClassname)
        case "otherresearchproduct" => (new OtherResearchProduct, resourceType, term.getClassname)
      }
    } else
      null
  }

  private def generateAuthor(ca: CrossrefAuthor): Author = {
    val a = new Author
    a.setName(ca.givenName)
    a.setSurname(ca.familyName)
    a.setFullname(s"${ca.familyName}, ${ca.givenName}")
    a.setRank(ca.rank + 1)
    if (StringUtils.isNotBlank(ca.ORCID))
      a.setPid(
        List(
          OafMapperUtils.authorPid(
            ca.ORCID,
            OafMapperUtils.qualifier(
              ModelConstants.ORCID_PENDING,
              ModelConstants.ORCID_PENDING,
              ModelConstants.DNET_PID_TYPES
            ),
            null
          )
        ).asJava
      )
    a
  }

}
