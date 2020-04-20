package eu.dnetlib.doiboost.crossref

import eu.dnetlib.dhp.schema.oaf._
import org.json4s
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._
import org.slf4j.Logger

import scala.collection.JavaConverters._
class Crossref2Oaf {

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
      "posted-content"-> "publication",
      "dataset" -> "dataset"
  )


  val mappingCrossrefSubType = Map(
    "book-section" ->                 "0013 Part of book or chapter of book",
    "book" ->                         "0002 Book",
    "book-chapter" ->                 "0013 Part of book or chapter of book",
    "book-part" ->                    "0013 Part of book or chapter of book",
    "book-series" ->                  "0002 Book",
    "book-set" ->                     "0002 Book",
    "book-track" ->                   "0002 Book",
    "edited-book" ->                  "0002 Book",
    "reference-book" ->               "0002 Book",
    "monograph" ->                    "0002 Book",
    "journal-article" ->              "0001 Article",
    "dissertation" ->                 "0006 Doctoral thesis",
    "other" ->                        "0038 Other literature type",
    "peer-review" ->                  "0015 Review",
    "proceedings" ->                  "0004 Conference object",
    "proceedings-article" ->          "0004 Conference object",
    "reference-entry" ->              "0013 Part of book or chapter of book",
    "report" ->                       "0017 Report",
    "report-series" ->                "0017 Report",
    "standard" ->                     "0038 Other literature type",
    "standard-series" ->              "0038 Other literature type",
    "dataset"->                       "0021 Dataset",
    "preprint"->                      "0016 Preprint",
    "report"->                        "0017 Report"
  )

  def convert(input: String, logger:Logger): Result = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json: json4s.JValue = parse(input)
    val objectType = (json \ "type").extractOrElse[String](null)
    val objectSubType = (json \ "subtype").extractOrElse[String](null)
    if (objectType == null)
      return null
    val result = generateItemFromType(objectType, objectSubType)
    if (result == null)
      return result
    val cOBJCategory = mappingCrossrefSubType.getOrElse(objectType,mappingCrossrefSubType.getOrElse(objectSubType,"0038 Other literature type"));

    logger.info(mappingCrossrefType(objectType))
    logger.info(cOBJCategory)
    val doi:String = (json \ "DOI").extract[String]
    val pid = new StructuredProperty()
    pid.setValue(doi)
    pid.setQualifier(new Qualifier)
    result.setPid(List(createSP(doi,"doi", PID_TYPES)).asJava)

    logger.info(doi)

    result
  }


  def createSP(value:String, classId:String, schemeId:String ):StructuredProperty = {
    val sp = new StructuredProperty
    val q = new Qualifier
    q.setClassid(classId)
    q.setClassname(classId)
    q.setSchemeid(schemeId)
    q.setSchemename(schemeId  )
    sp.setValue(value)
    sp.setQualifier(q)
    sp

  }


  def generateItemFromType (objectType:String, objectSubType:String):Result = {
    if (mappingCrossrefType.contains(objectType)){
      if (mappingCrossrefType(objectType).equalsIgnoreCase("publication"))
        return  new Publication()
      if (mappingCrossrefType(objectType).equalsIgnoreCase("dataset"))
        return  new Dataset()
    }
    null
  }

}
