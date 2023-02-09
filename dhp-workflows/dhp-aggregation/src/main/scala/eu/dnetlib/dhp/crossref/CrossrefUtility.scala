package eu.dnetlib.dhp.crossref

import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup
import eu.dnetlib.dhp.schema.common.ModelConstants
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils._
import eu.dnetlib.dhp.schema.oaf.utils.{GraphCleaningFunctions, IdentifierFactory, OafMapperUtils, PidType}
import eu.dnetlib.dhp.schema.oaf._
import org.apache.commons.lang.StringUtils
import org.json4s
import org.json4s.DefaultFormats
import org.json4s.JsonAST.{JField, JObject, JString, JValue}
import org.json4s.jackson.JsonMethods.parse

import scala.collection.JavaConverters._


case class CrossrefDT(doi: String, json: String, timestamp: Long) {}
object CrossrefUtility {
  val DOI_PREFIX_REGEX = "(^10\\.|\\/10.)"
  val DOI_PREFIX = "10."
  val CROSSREF_COLLECTED_FROM = keyValue(ModelConstants.CROSSREF_ID, ModelConstants.CROSSREF_NAME)

  def normalizeDoi(input: String): String = {
    if (input == null)
      return null
    val replaced = input
      .replaceAll("(?:\\n|\\r|\\t|\\s)", "")
      .toLowerCase
      .replaceFirst(DOI_PREFIX_REGEX, DOI_PREFIX)
    if (replaced == null || replaced.trim.isEmpty)
      return null
    if (replaced.indexOf("10.") < 0)
      return null
    val ret = replaced.substring(replaced.indexOf("10."))
    if (!ret.startsWith(DOI_PREFIX))
      return null
    ret
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

  private def generateDate(
                    dt: String,
                    datePart: List[List[Int]],
                    classId: String,
                    schemeId: String
                  ): StructuredProperty = {
    val dp = extractDate(dt, datePart)
    if (StringUtils.isNotBlank(dp))
      structuredProperty(dp, classId, classId,schemeId)
    else
      null
  }


  private def generateItemFromType(objectType: String, vocabularies:VocabularyGroup): (Result, String) = {
    val term = vocabularies.getSynonymAsQualifier(ModelConstants.DNET_PUBLICATION_RESOURCE, objectType)
    if (term != null) {
      val resourceType = vocabularies.getSynonymAsQualifier(ModelConstants.DNET_RESULT_TYPOLOGIES, term.getClassid).getClassname

      resourceType match {
        case "publication" =>(new Publication, resourceType)
        case "dataset" =>(new Dataset, resourceType)
        case "software" => (new Software, resourceType)
        case "otherresearchproduct" =>(new OtherResearchProduct, resourceType)
      }
    } else
      null
  }


  def convert(input: String, vocabularies:VocabularyGroup): List[Oaf] = {
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
    mappingResult(result, json, cOBJCategory)
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
      )
    }

    result match {
      case publication: Publication => convertPublication(publication, json, cOBJCategory)
      case dataset: Dataset => convertDataset(dataset)
    }

    resultList = resultList ::: List(result)
    resultList
  }


  def mappingResult(result: Result, json: JValue, cobjCategory: String): Result = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats

    //MAPPING Crossref DOI into PID
    val doi: String = normalizeDoi((json \ "DOI").extract[String])

    result.setPid(
      List(
        structuredProperty(doi, PidType.doi.toString, PidType.doi.toString,   ModelConstants.DNET_PID_TYPES)
      ).asJava)

    //MAPPING Crossref DOI into OriginalId
    //and Other Original Identifier of dataset like clinical-trial-number
    val clinicalTrialNumbers: List[String] = for (JString(ctr) <- json \ "clinical-trial-number") yield ctr
    val alternativeIds: List[String] = for (JString(ids) <- json \ "alternative-id") yield ids
    val tmp = clinicalTrialNumbers ::: alternativeIds ::: List(doi)


    result.setOriginalId(tmp.filter(id => id != null).asJava)

    // Add DataInfo
    result.setDataInfo(dataInfo(false, false,0.9F,null, false,ModelConstants.REPOSITORY_PROVENANCE_ACTIONS))

    result.setLastupdatetimestamp((json \ "indexed" \ "timestamp").extract[Long])
    result.setDateofcollection((json \ "indexed" \ "date-time").extract[String])

    result.setCollectedfrom(List(CROSSREF_COLLECTED_FROM).asJava)

    // Publisher ( Name of work's publisher mapped into  Result/Publisher)
    val publisher = (json \ "publisher").extractOrElse[String](null)
    if (publisher != null && publisher.nonEmpty)
      result.setPublisher(new Publisher(publisher))

    // TITLE
    val mainTitles =
      for {JString(title) <- json \ "title" if title.nonEmpty}
        yield
          structuredProperty(title, ModelConstants.MAIN_TITLE_QUALIFIER)
    val originalTitles = for {
      JString(title) <- json \ "original-title" if title.nonEmpty
    } yield structuredProperty(title, ModelConstants.ALTERNATIVE_TITLE_QUALIFIER)
    val shortTitles = for {
      JString(title) <- json \ "short-title" if title.nonEmpty
    }  yield structuredProperty(title, ModelConstants.ALTERNATIVE_TITLE_QUALIFIER)
    val subtitles =
      for {JString(title) <- json \ "subtitle" if title.nonEmpty}
        yield structuredProperty(title, ModelConstants.SUBTITLE_QUALIFIER)
    result.setTitle((mainTitles ::: originalTitles ::: shortTitles ::: subtitles).asJava)

    // DESCRIPTION
    val descriptionList =
      for {JString(description) <- json \ "abstract"} yield description
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
        subjectList.map(s =>  createSubject(s, "keyword", ModelConstants.DNET_SUBJECT_TYPOLOGIES)).asJava
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
      JObject(license) <- json \ "license"
      JField("URL", JString(lic)) <- license
      JField("content-version", JString(content_version)) <- license
    } yield (asField(lic), content_version)
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
    instance.setInstancetype(
      OafMapperUtils.qualifier(
        cobjCategory.substring(0, 4),
        cobjCategory.substring(5),
        ModelConstants.DNET_PUBLICATION_RESOURCE,
        ModelConstants.DNET_PUBLICATION_RESOURCE
      )
    )
    result.setResourcetype(
      OafMapperUtils.qualifier(
        cobjCategory.substring(0, 4),
        cobjCategory.substring(5),
        ModelConstants.DNET_PUBLICATION_RESOURCE,
        ModelConstants.DNET_PUBLICATION_RESOURCE
      )
    )

    instance.setCollectedfrom(createCrossrefCollectedFrom())
    if (StringUtils.isNotBlank(issuedDate)) {
      instance.setDateofacceptance(asField(issuedDate))
    } else {
      instance.setDateofacceptance(asField(createdDate.getValue))
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

}
