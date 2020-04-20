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

class Crossref2Oaf {

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
    logger.debug(mappingCrossrefType(objectType))
    logger.debug(cOBJCategory)

    //MAPPING Crossref DOI into PID
    val doi: String = (json \ "DOI").extract[String]

    result.setPid(List(createSP(doi, "doi", PID_TYPES)).asJava)
    //MAPPING Crossref DOI into OriginalId
    result.setOriginalId(List(doi).asJava)
    //Set identifier as {50|60} | doiboost____::md5(DOI)
    result.setId(generateIdentifier(result, doi))

    // Add DataInfo
    result.setDataInfo(generateDataInfo())

    result.setLastupdatetimestamp((json \ "indexed" \ "timestamp").extract[Long])
    result.setDateofcollection((json \ "indexed" \ "date-time").extract[String])

    result.setCollectedfrom(List(createCollectedFrom()).asJava)

    // Publisher ( Name of work's publisher mapped into  Result/Publisher)
    val publisher = (json \ "publisher").extract[String]
    result.setPublisher(asField(publisher))

    // TITLE
    val mainTitles = for {JString(title) <- json \ "title"} yield createSP(title, "main title", "dnet:dataCite_title")
    val originalTitles = for {JString(title) <- json \ "original-title"} yield createSP(title, "alternative title", "dnet:dataCite_title")
    val shortTitles = for {JString(title) <- json \ "short-title"} yield createSP(title, "alternative title", "dnet:dataCite_title")
    result.setTitle((mainTitles ::: originalTitles ::: shortTitles).asJava)

    // DESCRIPTION
    val descriptionList = for {JString(description) <- json \ "abstract"} yield asField(description)
    result.setDescription(descriptionList.asJava)
    // Source
    val sourceList = for {JString(source) <- json \ "source"} yield asField(source)

    result.setSource(sourceList.asJava)


    //RELEVANT DATE Mapping
    val createdDate =generateDate((json \ "created" \"date-time").extract[String],(json \ "created"\"date-parts").extract[List[List[Int]]],"created", "dnet:dataCite_date" )
    val postedDate =generateDate((json \ "posted" \"date-time").extractOrElse[String](null),(json \ "posted"\"date-parts").extract[List[List[Int]]],"available", "dnet:dataCite_date" )
    val acceptedDate =generateDate((json \ "accepted" \"date-time").extractOrElse[String](null),(json \ "accepted"\"date-parts").extract[List[List[Int]]],"accepted", "dnet:dataCite_date" )
    val publishedPrintDate =generateDate((json \ "published-print" \"date-time").extractOrElse[String](null),(json \ "published-print"\"date-parts").extract[List[List[Int]]],"published-print", "dnet:dataCite_date" )
    val publishedOnlineDate =generateDate((json \ "published-online" \"date-time").extractOrElse[String](null),(json \ "published-online"\"date-parts").extract[List[List[Int]]],"published-online", "dnet:dataCite_date" )

    result.setRelevantdate(List(createdDate ,postedDate, acceptedDate,publishedOnlineDate, publishedPrintDate).asJava)
    result
  }


  def generateDate(dt: String, datePart: List[List[Int]], classId: String, schemeId: String): StructuredProperty = {
    if (StringUtils.isNotBlank(dt))
      return createSP(dt, classId, schemeId)


    if (datePart != null && datePart.size == 1) {
      val res = datePart.head
      if (res.size == 3) {
        val dp = f"${res.head}-${res(1)}%02d-${res(2)}%02d"
        println(dp)
        if (dp.length == 10) {
          return createSP(dp, classId, schemeId)
        }
      }
    }
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

    def createQualifier(cls: String, sch: String): Qualifier = {
      val q = new Qualifier
      q.setClassid(cls)
      q.setClassname(cls)
      q.setSchemeid(sch)
      q.setSchemename(sch)
      q
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
