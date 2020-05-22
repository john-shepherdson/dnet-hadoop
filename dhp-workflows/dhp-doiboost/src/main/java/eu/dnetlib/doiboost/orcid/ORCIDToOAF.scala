package eu.dnetlib.doiboost.orcid

import java.io.IOException

import eu.dnetlib.dhp.schema.oaf.{Author, Publication}
import eu.dnetlib.doiboost.DoiBoostMappingUtil
import eu.dnetlib.doiboost.DoiBoostMappingUtil.{ORCID, PID_TYPES, createSP, generateDataInfo, generateIdentifier}
import eu.dnetlib.doiboost.crossref.Crossref2Oaf
import org.apache.commons.lang.StringUtils
import org.codehaus.jackson.map.ObjectMapper
import org.json4s
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._


case class ORCIDItem(oid:String,name:String,surname:String,creditName:String,errorCode:String){}
object ORCIDToOAF {
  val logger: Logger = LoggerFactory.getLogger(Crossref2Oaf.getClass)
  val mapper = new ObjectMapper

  def isJsonValid(inputStr: String): Boolean = {
    import java.io.IOException
    try {
      mapper.readTree(inputStr)
      true
    } catch {
      case e: IOException =>
        false
    }
  }

  def extractValueFromInputString(input: String): (String, String) = {
    val i = input.indexOf('[')
    if (i <5) {
      return null
    }
    val orcidList = input.substring(i, input.length - 1)
    val doi = input.substring(1, i - 1)
    if (isJsonValid(orcidList)) {
      (doi, orcidList)
    } else null
  }


  def convertTOOAF(input:String) :Publication = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats

    val item:(String, String) = extractValueFromInputString(input)

    if (item== null) {
      return  null
    }

    val json_str = item._2
    lazy val json: json4s.JValue = parse(json_str)

    val doi = item._1

    val pub:Publication = new Publication
    pub.setPid(List(createSP(doi, "doi", PID_TYPES)).asJava)
    pub.setDataInfo(generateDataInfo())
    pub.setId(generateIdentifier(pub, doi.toLowerCase))




    try{
      val authorList:List[ORCIDItem] = json.extract[List[ORCIDItem]]


      pub.setAuthor(authorList.map(a=> {
        generateAuhtor(a.name, a.surname, a.creditName, a.oid)
      }).asJava)


      pub.setCollectedfrom(List(DoiBoostMappingUtil.createORIDCollectedFrom()).asJava)
      pub
    } catch {
      case e: Throwable =>
        logger.info(s"ERROR ON GENERATE Publication from $input")
        null
    }

  }

  def generateAuhtor(given: String, family: String, fullName:String, orcid: String): Author = {
    val a = new Author
    a.setName(given)
    a.setSurname(family)
    if (fullName!= null && fullName.nonEmpty)
      a.setFullname(fullName)
    else
      a.setFullname(s"$given $family")
    if (StringUtils.isNotBlank(orcid))
      a.setPid(List(createSP(orcid, ORCID, PID_TYPES)).asJava)

    a
  }


}
