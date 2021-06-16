package eu.dnetlib.dhp.sx.ebi

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}
import eu.dnetlib.dhp.schema.oaf.{Oaf, Publication, Result}
import eu.dnetlib.dhp.sx.ebi.model.{PMArticle, PMParser, PubMedToOaf}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.{BeforeEach, Test}
import org.mockito.junit.jupiter.MockitoExtension

import scala.collection.JavaConverters._
import scala.io.Source
import scala.xml.pull.XMLEventReader


@ExtendWith(Array(classOf[MockitoExtension]))
class TestEBI extends AbstractVocabularyTest{


  @BeforeEach
  def setUp() :Unit = {

    super.setUpVocabulary()
  }


  @Test
  def testEBIData() = {

    val mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT)
    val inputXML = Source.fromInputStream(getClass.getResourceAsStream("pubmed.xml")).mkString
    val xml = new XMLEventReader(Source.fromBytes(inputXML.getBytes()))
    new PMParser(xml).foreach(s =>println(mapper.writeValueAsString(s)))
  }


  @Test
  def testPubmedToOaf(): Unit = {
    assertNotNull(vocabularies)
    assertTrue(vocabularies.vocabularyExists("dnet:publication_resource"))
    val mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT)

    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,false)
    val records:String =Source.fromInputStream(getClass.getResourceAsStream("pubmed_dump")).mkString
    val r:List[Oaf] = records.lines.toList.map(s=>mapper.readValue(s, classOf[PMArticle])).map(a => PubMedToOaf.convert(a, vocabularies))
    assertEquals(10, r.size)
    assertTrue(r.map(p => p.asInstanceOf[Result]).flatMap(p => p.getInstance().asScala.map(i => i.getInstancetype.getClassid)).exists(p => "0037".equalsIgnoreCase(p)))
    println(mapper.writeValueAsString(r.head))







  }

}
