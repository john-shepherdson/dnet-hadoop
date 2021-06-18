package eu.dnetlib.dhp.sx.bio.pubmed

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}
import eu.dnetlib.dhp.schema.oaf.{Oaf, Result}
import eu.dnetlib.dhp.sx.bio.PDBToOAF
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.{BeforeEach, Test}
import org.mockito.junit.jupiter.MockitoExtension

import scala.collection.JavaConverters._
import scala.io.Source
import scala.xml.pull.XMLEventReader


@ExtendWith(Array(classOf[MockitoExtension]))
class BioScholixTest extends AbstractVocabularyTest{


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


  @Test
  def testPDBToOAF():Unit = {

    assertNotNull(vocabularies)
    assertTrue(vocabularies.vocabularyExists("dnet:publication_resource"))

//    val mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT)
//    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,false)
    val records:String =Source.fromInputStream(getClass.getResourceAsStream("/eu/dnetlib/dhp/sx/bio/pdb_dump")).mkString
    records.lines.foreach(s => assertTrue(s.nonEmpty))

    val result:List[Oaf]=  records.lines.toList.flatMap(o => PDBToOAF.convert(o))

    assertTrue(result.nonEmpty)
    result.foreach(r => assertNotNull(r))


  }

}
