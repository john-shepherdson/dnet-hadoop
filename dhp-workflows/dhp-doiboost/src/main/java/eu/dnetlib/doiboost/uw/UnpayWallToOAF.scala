package eu.dnetlib.doiboost.uw

import eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory
import eu.dnetlib.dhp.schema.oaf.{Instance, Publication}
import org.json4s
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import eu.dnetlib.doiboost.DoiBoostMappingUtil._



case class OALocation(evidence:Option[String], host_type:Option[String], is_best:Option[Boolean], license: Option[String], pmh_id:Option[String], updated:Option[String],
                      url:Option[String], url_for_landing_page:Option[String], url_for_pdf:Option[String], version:Option[String]) {}




object UnpayWallToOAF {
  val logger: Logger = LoggerFactory.getLogger(getClass)

  def convertToOAF(input:String):Publication = {
    val pub = new Publication

    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json: json4s.JValue = parse(input)

    val doi = (json \"doi").extract[String]


    val is_oa = (json\ "is_oa").extract[Boolean]

    val oaLocation:OALocation = (json \ "best_oa_location").extractOrElse[OALocation](null)
    pub.setPid(List(createSP(doi, "doi", PID_TYPES)).asJava)

    //IMPORTANT
    //The old method pub.setId(IdentifierFactory.createIdentifier(pub))
    //will be replaced using IdentifierFactory
    pub.setId(generateIdentifier(pub, doi.toLowerCase))
    pub.setId(IdentifierFactory.createIdentifier(pub))


    pub.setCollectedfrom(List(createUnpayWallCollectedFrom()).asJava)
    pub.setDataInfo(generateDataInfo())

    if (!is_oa)
      return null

    if(oaLocation== null || oaLocation.url.isEmpty)
      return  null
    val i :Instance= new Instance()

    i.setCollectedfrom(createUnpayWallCollectedFrom())
    i.setAccessright(getOpenAccessQualifier())
    i.setUrl(List(oaLocation.url.get).asJava)

    if (oaLocation.license.isDefined)
      i.setLicense(asField(oaLocation.license.get))
    pub.setInstance(List(i).asJava)

    pub

  }




}
