package eu.dnetlib.dhp.schema.sx

import eu.dnetlib.dhp.schema.common.ModelConstants
import eu.dnetlib.dhp.schema.oaf._

object OafUtils {

  def generateKeyValue(key: String, value: String): KeyValue = {
    val kv: KeyValue = new KeyValue()
    kv.setKey(key)
    kv.setValue(value)
    kv
  }

  def generateDataInfo(trust: Float = 0.9f, invisible: Boolean = false): DataInfo = {
    val di = new DataInfo
    di.setInferred(false)
    di.setTrust(trust)
    di.setProvenanceaction(createQualifier(ModelConstants.SYSIMPORT_ACTIONSET, ModelConstants.DNET_PROVENANCE_ACTIONS))
    di
  }

  def createQualifier(cls: String, sch: String): Qualifier = {
    createQualifier(cls, cls, sch)
  }

  def createQualifier(classId: String, className: String, schemeId: String): Qualifier = {
    val q: Qualifier = new Qualifier
    q.setClassid(classId)
    q.setClassname(className)
    q.setSchemeid(schemeId)
    q
  }

  def createAccessRight(classId: String, className: String, schemeId: String): AccessRight = {
    val accessRight: AccessRight = new AccessRight
    accessRight.setClassid(classId)
    accessRight.setClassname(className)
    accessRight.setSchemeid(schemeId)
    accessRight
  }

  def createSP(value: String, classId: String,className:String, schemeId: String): StructuredProperty = {
    val sp = new StructuredProperty
    sp.setQualifier(createQualifier(classId,className, schemeId))
    sp.setValue(value)
    sp

  }

  def createSP(value: String, classId: String, schemeId: String): StructuredProperty = {
    val sp = new StructuredProperty
    sp.setQualifier(createQualifier(classId, schemeId))
    sp.setValue(value)
    sp

  }

}
