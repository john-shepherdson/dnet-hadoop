package eu.dnetlib.dhp.sx.ebi

import com.fasterxml.jackson.databind.ObjectMapper
import eu.dnetlib.dhp.sx.ebi.model.PMParser
import org.junit.jupiter.api.Test

import scala.io.Source
import scala.xml.pull.XMLEventReader

class TestEBI {



  @Test
  def testEBIData() = {
    val inputXML = Source.fromInputStream(getClass.getResourceAsStream("pubmed.xml")).mkString
    val xml = new XMLEventReader(Source.fromBytes(inputXML.getBytes()))

    val mapper = new ObjectMapper()

    new PMParser(xml).foreach(s =>println(mapper.writeValueAsString(s)))




  }

}
