package eu.dnetlib.doiboost.crossref

import eu.dnetlib.dhp.schema.oaf._
import eu.dnetlib.dhp.utils.DHPUtils
import org.apache.commons.lang.StringUtils
import org.json4s
import org.json4s.DefaultFormats
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods._
import org.slf4j.Logger
import scala.collection.JavaConverters._

case class mappingAffiliation(name: String)
case class mappingAuthor(given: Option[String], family: String, ORCID: Option[String], affiliation: Option[mappingAffiliation]) {}


case object Crossref2Oaf {

  //STATIC STRING
  val MAG = "MAG"
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

  val mappingCrossrefType = Map(
    "book-section" -> "publication",
    "book" -> "publication",
    "book-chapter" -> "publication",
    "book-part" -> "publication",
    "book-series" -> "publication",
    "book-set" -> "publication",
    "book-track" -> "publication",
    "edited-book" -> "publication",
    "reference-book" -> "publication",
    "monograph" -> "publication",
    "journal-article" -> "publication",
    "dissertation" -> "publication",
    "other" -> "publication",
    "peer-review" -> "publication",
    "proceedings" -> "publication",
    "proceedings-article" -> "publication",
    "reference-entry" -> "publication",
    "report" -> "publication",
    "report-series" -> "publication",
    "standard" -> "publication",
    "standard-series" -> "publication",
    "posted-content" -> "publication",
    "dataset" -> "dataset"
  )


  val mappingCrossrefSubType = Map(
    "book-section" -> "0013 Part of book or chapter of book",
    "book" -> "0002 Book",
    "book-chapter" -> "0013 Part of book or chapter of book",
    "book-part" -> "0013 Part of book or chapter of book",
    "book-series" -> "0002 Book",
    "book-set" -> "0002 Book",
    "book-track" -> "0002 Book",
    "edited-book" -> "0002 Book",
    "reference-book" -> "0002 Book",
    "monograph" -> "0002 Book",
    "journal-article" -> "0001 Article",
    "dissertation" -> "0006 Doctoral thesis",
    "other" -> "0038 Other literature type",
    "peer-review" -> "0015 Review",
    "proceedings" -> "0004 Conference object",
    "proceedings-article" -> "0004 Conference object",
    "reference-entry" -> "0013 Part of book or chapter of book",
    "report" -> "0017 Report",
    "report-series" -> "0017 Report",
    "standard" -> "0038 Other literature type",
    "standard-series" -> "0038 Other literature type",
    "dataset" -> "0021 Dataset",
    "preprint" -> "0016 Preprint",
    "report" -> "0017 Report"
  )


  def mappingResult(result: Result, json: JValue, cobjCategory: String): Result = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats

    //MAPPING Crossref DOI into PID
    val doi: String = (json \ "DOI").extract[String]
    result.setPid(List(createSP(doi, "doi", PID_TYPES)).asJava)

    //MAPPING Crossref DOI into OriginalId
    //and Other Original Identifier of dataset like clinical-trial-number
    val clinicalTrialNumbers = for (JString(ctr) <- json \ "clinical-trial-number") yield ctr
    val alternativeIds = for (JString(ids) <- json \ "alternative-id") yield ids
    val tmp = clinicalTrialNumbers ::: alternativeIds ::: List(doi)

    result.setOriginalId(tmp.filter(id => id != null).asJava)

    //Set identifier as {50|60} | doiboost____::md5(DOI)
    result.setId(generateIdentifier(result, doi))

    // Add DataInfo
    result.setDataInfo(generateDataInfo())

    result.setLastupdatetimestamp((json \ "indexed" \ "timestamp").extract[Long])
    result.setDateofcollection((json \ "indexed" \ "date-time").extract[String])

    result.setCollectedfrom(List(createCollectedFrom()).asJava)

    // Publisher ( Name of work's publisher mapped into  Result/Publisher)
    val publisher = (json \ "publisher").extractOrElse[String](null)
    result.setPublisher(asField(publisher))

    // TITLE
    val mainTitles = for {JString(title) <- json \ "title"} yield createSP(title, "main title", "dnet:dataCite_title")
    val originalTitles = for {JString(title) <- json \ "original-title"} yield createSP(title, "alternative title", "dnet:dataCite_title")
    val shortTitles = for {JString(title) <- json \ "short-title"} yield createSP(title, "alternative title", "dnet:dataCite_title")
    val subtitles = for {JString(title) <- json \ "subtitle"} yield createSP(title, "subtitle", "dnet:dataCite_title")
    result.setTitle((mainTitles ::: originalTitles ::: shortTitles ::: subtitles).asJava)

    // DESCRIPTION
    val descriptionList = for {JString(description) <- json \ "abstract"} yield asField(description)
    result.setDescription(descriptionList.asJava)
    // Source
    val sourceList = for {JString(source) <- json \ "source"} yield asField(source)
    result.setSource(sourceList.asJava)

    //RELEVANT DATE Mapping
    val createdDate = generateDate((json \ "created" \ "date-time").extract[String], (json \ "created" \ "date-parts").extract[List[List[Int]]], "created", "dnet:dataCite_date")
    val postedDate = generateDate((json \ "posted" \ "date-time").extractOrElse[String](null), (json \ "posted" \ "date-parts").extract[List[List[Int]]], "available", "dnet:dataCite_date")
    val acceptedDate = generateDate((json \ "accepted" \ "date-time").extractOrElse[String](null), (json \ "accepted" \ "date-parts").extract[List[List[Int]]], "accepted", "dnet:dataCite_date")
    val publishedPrintDate = generateDate((json \ "published-print" \ "date-time").extractOrElse[String](null), (json \ "published-print" \ "date-parts").extract[List[List[Int]]], "published-print", "dnet:dataCite_date")
    val publishedOnlineDate = generateDate((json \ "published-online" \ "date-time").extractOrElse[String](null), (json \ "published-online" \ "date-parts").extract[List[List[Int]]], "published-online", "dnet:dataCite_date")

    val issuedDate = extractDate((json \ "issued" \ "date-time").extractOrElse[String](null), (json \ "issued" \ "date-parts").extract[List[List[Int]]])
    if (StringUtils.isNotBlank(issuedDate)) {
      result.setDateofacceptance(asField(issuedDate))
    }
    result.setRelevantdate(List(createdDate, postedDate, acceptedDate, publishedOnlineDate, publishedPrintDate).filter(p => p != null).asJava)


    //Mapping AUthor

    val authorList: List[mappingAuthor] = (json \ "author").extractOrElse[List[mappingAuthor]](List())

    result.setAuthor(authorList.map(a => generateAuhtor(a.given.orNull, a.family, a.ORCID.orNull)).asJava)

    // Mapping instance

    val instance = new Instance()
    val license = for {
      JString(lic) <- json \ "license" \ "URL"
    } yield asField(lic)
    val l = license.filter(d => StringUtils.isNotBlank(d.getValue))
    if (l.nonEmpty)
      instance.setLicense(l.head)

    instance.setAccessright(createQualifier("Restricted", "dnet:access_modes"))

    result.setInstance(List(instance).asJava)
    instance.setInstancetype(createQualifier(cobjCategory.substring(0, 4), cobjCategory.substring(5), "dnet:publication_resource", "dnet:publication_resource"))

    instance.setCollectedfrom(createCollectedFrom())
    if (StringUtils.isNotBlank(issuedDate)) {
      instance.setDateofacceptance(asField(issuedDate))
    }
    val s: String = (json \ "URL").extract[String]
    val links: List[String] = ((for {JString(url) <- json \ "link" \ "URL"} yield url) ::: List(s)).filter(p => p != null).distinct
    if (links.nonEmpty)
      instance.setUrl(links.asJava)
    result
  }


  def generateAuhtor(given: String, family: String, orcid: String): Author = {
    val a = new Author
    a.setName(given)
    a.setSurname(family)
    a.setFullname(s"${given} ${family}")
    if (StringUtils.isNotBlank(orcid))
      a.setPid(List(createSP(orcid, ORCID, PID_TYPES)).asJava)
    a
  }

  def convert(input: String, logger: Logger): Result = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json: json4s.JValue = parse(input)


    val objectType = (json \ "type").extractOrElse[String](null)
    val objectSubType = (json \ "subtype").extractOrElse[String](null)
    if (objectType == null)
      return null


    val result = generateItemFromType(objectType, objectSubType)
    if (result == null)
      return result
    val cOBJCategory = mappingCrossrefSubType.getOrElse(objectType, mappingCrossrefSubType.getOrElse(objectSubType, "0038 Other literature type"));
//    logger.debug(mappingCrossrefType(objectType))
//    logger.debug(cOBJCategory)

    mappingResult(result, json, cOBJCategory)


    result match {
      case publication: Publication => convertPublication(publication, json, cOBJCategory)
      case dataset: Dataset => convertDataset(dataset)
    }


    result
  }

  def convertDataset(dataset: Dataset): Unit = {
    //TODO probably we need to add relation and other stuff here
  }


  def convertPublication(publication: Publication, json: JValue, cobjCategory: String): Unit = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    val containerTitles = for {JString(ct) <- json \ "container-title"} yield ct


    //Mapping book
    if (cobjCategory.toLowerCase.contains("book")) {
      val ISBN = for {JString(isbn) <- json \ "ISBN"} yield isbn
      if (ISBN.nonEmpty && containerTitles.nonEmpty) {
        val source = s"${containerTitles.head} ISBN: ${ISBN.head}"
        if (publication.getSource != null) {
          val l: List[Field[String]] = publication.getSource.asScala.toList
          val ll: List[Field[String]] = l ::: List(asField(source))
          publication.setSource(ll.asJava)
        }
        else
          publication.setSource(List(asField(source)).asJava)
      }
    } else {
      // Mapping Journal

      val issnInfos = for {JArray(issn_types) <- json \ "issn-type"
                           JObject(issn_type) <- issn_types
                           JField("type", JString(tp)) <- issn_type
                           JField("value", JString(vl)) <- issn_type
                           } yield Tuple2(tp, vl)

      val volume = (json \ "volume").extractOrElse[String] (null)
      if (containerTitles.nonEmpty) {
        val journal = new Journal
        journal.setName(containerTitles.head)
        if (issnInfos.nonEmpty) {

          issnInfos.foreach(tp => {
            tp._1 match {
              case "electronic" => journal.setIssnOnline(tp._2)
              case "print" => journal.setIssnPrinted(tp._2)
            }
          })

        }
        journal.setVol(volume)

        val page = (json \ "page").extractOrElse[String] (null)
        if(page!= null ) {
          val pp = page.split("-")
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
      return dt
    if (datePart != null && datePart.size == 1) {
      val res = datePart.head
      if (res.size == 3) {
        val dp = f"${res.head}-${res(1)}%02d-${res(2)}%02d"
        if (dp.length == 10) {
          return dp
        }
      }
    }
    null

  }

  def generateDate(dt: String, datePart: List[List[Int]], classId: String, schemeId: String): StructuredProperty = {

    val dp = extractDate(dt, datePart)
    if (StringUtils.isNotBlank(dp))
      return createSP(dp, classId, schemeId)
    null
  }


  def generateIdentifier(oaf: Result, doi: String): String = {
    val id = DHPUtils.md5(doi.toLowerCase)
    if (oaf.isInstanceOf[Dataset])
      return s"60|${doiBoostNSPREFIX}${SEPARATOR}${id}"
    s"50|${doiBoostNSPREFIX}${SEPARATOR}${id}"
  }

  def asField[T](value: T): Field[T] = {
    val tmp = new Field[T]
    tmp.setValue(value)
    tmp


  }


  def generateDataInfo(): DataInfo = {
    val di = new DataInfo
    di.setDeletedbyinference(false)
    di.setInferred(false)
    di.setInvisible(false)
    di.setTrust("0.9")
    di.setProvenanceaction(createQualifier("sysimport:actionset", "dnet:provenanceActions"))
    di
  }


  def createSP(value: String, classId: String, schemeId: String): StructuredProperty = {
    val sp = new StructuredProperty
    sp.setQualifier(createQualifier(classId, schemeId))
    sp.setValue(value)
    sp

  }

  def createCollectedFrom(): KeyValue = {

    val cf = new KeyValue
    cf.setValue(CROSSREF)
    cf.setKey("10|" + OPENAIRE_PREFIX + SEPARATOR + DHPUtils.md5("crossref"))
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
