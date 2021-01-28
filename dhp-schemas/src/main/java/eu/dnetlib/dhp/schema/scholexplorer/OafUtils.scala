package eu.dnetlib.dhp.schema.scholexplorer

import eu.dnetlib.dhp.schema.oaf.{DataInfo, Field, KeyValue, Qualifier, StructuredProperty}

object OafUtils {



  def generateKeyValue(key: String, value: String): KeyValue = {
    val kv: KeyValue = new KeyValue()
    kv.setKey(key)
    kv.setValue(value)
    kv.setDataInfo(generateDataInfo("0.9"))
    kv
  }


  def generateDataInfo(trust: String = "0.9", invisible: Boolean = false): DataInfo = {
    val di = new DataInfo
    di.setDeletedbyinference(false)
    di.setInferred(false)
    di.setInvisible(invisible)
    di.setTrust(trust)
    di.setProvenanceaction(createQualifier("sysimport:actionset", "dnet:provenanceActions"))
    di
  }

  def createQualifier(cls: String, sch: String): Qualifier = {
    createQualifier(cls, cls, sch, sch)
  }


  def createQualifier(classId: String, className: String, schemeId: String, schemeName: String): Qualifier = {
    val q: Qualifier = new Qualifier
    q.setClassid(classId)
    q.setClassname(className)
    q.setSchemeid(schemeId)
    q.setSchemename(schemeName)
    q
  }


  def asField[T](value: T): Field[T] = {
    val tmp = new Field[T]
    tmp.setValue(value)
    tmp


  }

  def createSP(value: String, classId: String,className:String, schemeId: String, schemeName:String): StructuredProperty = {
    val sp = new StructuredProperty
    sp.setQualifier(createQualifier(classId,className, schemeId, schemeName))
    sp.setValue(value)
    sp

  }



  def createSP(value: String, classId: String,className:String, schemeId: String, schemeName:String, dataInfo: DataInfo): StructuredProperty = {
    val sp = new StructuredProperty
    sp.setQualifier(createQualifier(classId,className, schemeId, schemeName))
    sp.setValue(value)
    sp.setDataInfo(dataInfo)
    sp

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


}
