package eu.dnetlib.doiboost.orcid

import eu.dnetlib.dhp.schema.oaf.Publication
import eu.dnetlib.doiboost.crossref.Crossref2Oaf
import org.codehaus.jackson.map.{ObjectMapper, SerializationConfig}
import org.junit.jupiter.api.Test
import org.slf4j.{Logger, LoggerFactory}
import org.junit.jupiter.api.Assertions._

import scala.io.Source

class MappingORCIDToOAFTest {
  val logger: Logger = LoggerFactory.getLogger(Crossref2Oaf.getClass)
  val mapper = new ObjectMapper()

  @Test
  def testExtractData():Unit ={
    val json = Source.fromInputStream(getClass.getResourceAsStream("dataOutput")).mkString
    assertNotNull(json)
    assertFalse(json.isEmpty)
    json.lines.foreach(s => {
      assertNotNull(ORCIDToOAF.extractValueFromInputString(s))
    })
  }


  @Test
  def testConvertOrcidToOAF():Unit ={
    val json = Source.fromInputStream(getClass.getResourceAsStream("dataOutput")).mkString
    mapper.getSerializationConfig.enable(SerializationConfig.Feature.INDENT_OUTPUT)

    assertNotNull(json)
    assertFalse(json.isEmpty)
//    json.lines.foreach(s => {
//
//    })


    val p :Publication = ORCIDToOAF.convertTOOAF(json.lines.next())


    logger.info(mapper.writeValueAsString(p))
  }








}
