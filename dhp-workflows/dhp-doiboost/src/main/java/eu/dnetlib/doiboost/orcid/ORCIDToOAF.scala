package eu.dnetlib.doiboost.orcid

import eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory
import eu.dnetlib.dhp.schema.oaf.{Author, Publication}
import eu.dnetlib.doiboost.DoiBoostMappingUtil
import eu.dnetlib.doiboost.DoiBoostMappingUtil.{ORCID, PID_TYPES, createSP, generateDataInfo, generateIdentifier}
import org.apache.commons.lang.StringUtils
import org.codehaus.jackson.map.ObjectMapper
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._


case class ORCIDItem(oid:String,name:String,surname:String,creditName:String,errorCode:String){}



case class ORCIDElement(doi:String, authors:List[ORCIDItem]) {}
object ORCIDToOAF {
  val logger: Logger = LoggerFactory.getLogger(ORCIDToOAF.getClass)
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


  def convertTOOAF(input:ORCIDElement) :Publication = {
    val doi = input.doi
    val pub:Publication = new Publication
    pub.setPid(List(createSP(doi, "doi", PID_TYPES)).asJava)
    pub.setDataInfo(generateDataInfo())

    //IMPORTANT
    //The old method pub.setId(IdentifierFactory.createIdentifier(pub))
    //will be replaced using IdentifierFactory
    pub.setId(generateIdentifier(pub, doi.toLowerCase))
    pub.setId(IdentifierFactory.createIdentifier(pub))


    try{
      pub.setAuthor(input.authors.map(a=> {
        generateAuthor(a.name, a.surname, a.creditName, a.oid)
      }).asJava)
      pub.setCollectedfrom(List(DoiBoostMappingUtil.createORIDCollectedFrom()).asJava)
      pub.setDataInfo(DoiBoostMappingUtil.generateDataInfo())
      pub
    } catch {
      case e: Throwable =>
        logger.info(s"ERROR ON GENERATE Publication from $input")
        null
    }
  }

  def generateAuthor(given: String, family: String, fullName:String, orcid: String): Author = {
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
