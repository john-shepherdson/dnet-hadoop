package eu.dnetlib.doiboost

import eu.dnetlib.dhp.schema.oaf.{DataInfo, Dataset, Field, KeyValue, Qualifier, Result, StructuredProperty}
import eu.dnetlib.dhp.utils.DHPUtils

object DoiBoostMappingUtil {

  //STATIC STRING
  val MAG = "microsoft"
  val MAG_NAME= "Microsoft Academic Graph"
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



  def generateDataInfo(): DataInfo = {
    generateDataInfo("0.9")
  }

  def generateDataInfo(trust:String): DataInfo = {
    val di = new DataInfo
    di.setDeletedbyinference(false)
    di.setInferred(false)
    di.setInvisible(false)
    di.setTrust(trust)
    di.setProvenanceaction(createQualifier("sysimport:actionset", "dnet:provenanceActions"))
    di
  }


  def createSP(value: String, classId: String, schemeId: String): StructuredProperty = {
    val sp = new StructuredProperty
    sp.setQualifier(createQualifier(classId, schemeId))
    sp.setValue(value)
    sp

  }

  def createSP(value: String, classId: String, schemeId: String, dataInfo: DataInfo): StructuredProperty = {
    val sp = new StructuredProperty
    sp.setQualifier(createQualifier(classId, schemeId))
    sp.setValue(value)
    sp.setDataInfo(dataInfo)
    sp

  }

  def createCrossrefCollectedFrom(): KeyValue = {

    val cf = new KeyValue
    cf.setValue(CROSSREF)
    cf.setKey("10|" + OPENAIRE_PREFIX + SEPARATOR + DHPUtils.md5(CROSSREF.toLowerCase))
    cf

  }


  def createUnpayWallCollectedFrom(): KeyValue = {

    val cf = new KeyValue
    cf.setValue(UNPAYWALL)
    cf.setKey("10|" + OPENAIRE_PREFIX + SEPARATOR + DHPUtils.md5(UNPAYWALL.toLowerCase))
    cf

  }

  def createORIDCollectedFrom(): KeyValue = {

    val cf = new KeyValue
    cf.setValue(ORCID)
    cf.setKey("10|" + OPENAIRE_PREFIX + SEPARATOR + DHPUtils.md5(ORCID.toLowerCase))
    cf

  }



  def generateIdentifier(oaf: Result, doi: String): String = {
    val id = DHPUtils.md5(doi.toLowerCase)
    if (oaf.isInstanceOf[Dataset])
      return s"60|${doiBoostNSPREFIX}${SEPARATOR}${id}"
    s"50|${doiBoostNSPREFIX}${SEPARATOR}${id}"
  }





  def createMAGCollectedFrom(): KeyValue = {

    val cf = new KeyValue
    cf.setValue(MAG)
    cf.setKey("10|" + OPENAIRE_PREFIX + SEPARATOR + DHPUtils.md5(MAG))
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


  def asField[T](value: T): Field[T] = {
    val tmp = new Field[T]
    tmp.setValue(value)
    tmp


  }



}
