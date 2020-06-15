package eu.dnetlib.doiboost.orcid

import org.codehaus.jackson.map.ObjectMapper
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.slf4j.{Logger, LoggerFactory}

import scala.io.Source

class MappingORCIDToOAFTest {
  val logger: Logger = LoggerFactory.getLogger(ORCIDToOAF.getClass)
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





}
