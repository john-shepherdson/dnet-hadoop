package eu.dnetlib.dhp.sx.graph.scholix

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}
import eu.dnetlib.dhp.schema.oaf.{Oaf, Relation, Result}
import eu.dnetlib.dhp.sx.graph.bio.BioDBToOAF
import eu.dnetlib.dhp.sx.graph.bio.BioDBToOAF.ScholixResolved
import eu.dnetlib.dhp.sx.graph.bio.pubmed.AbstractVocabularyTest
import org.json4s
import org.json4s.DefaultFormats
import org.json4s.JsonAST.{JField, JObject, JString}
import org.json4s.jackson.JsonMethods.parse
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.{BeforeEach, Test}
import org.mockito.junit.jupiter.MockitoExtension

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.util.zip.GZIPInputStream
import scala.collection.JavaConverters._
import scala.io.Source
import scala.xml.pull.XMLEventReader

@ExtendWith(Array(classOf[MockitoExtension]))
class ScholixGraphTest extends AbstractVocabularyTest{


  val mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,false)

  @BeforeEach
  def setUp() :Unit = {

    super.setUpVocabulary()
  }


  @Test
  def testOAFToSummary():Unit= {
    val inputRelations = Source.fromInputStream(getClass.getResourceAsStream("/eu/dnetlib/dhp/sx/graph/oaf_to_summary")).mkString
    val items = inputRelations.lines.toList
    assertNotNull(items)
    items.foreach(i =>assertTrue(i.nonEmpty))
    val result = items.map(r => mapper.readValue(r, classOf[Result])).map(i => ScholixUtils.resultToSummary(i))

    assertNotNull(result)

    assertEquals(result.size, items.size)
    val d = result.find(s => s.getLocalIdentifier.asScala.exists(i => i.getUrl == null || i.getUrl.isEmpty))
    assertFalse(d.isDefined)

    println(mapper.writeValueAsString(result.head))

  }






  @Test
  def testScholixRelationshipsClean() = {
    val inputRelations = Source.fromInputStream(getClass.getResourceAsStream("/eu/dnetlib/dhp/sx/graph/relation_transform.json")).mkString
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats

    lazy val json: json4s.JValue = parse(inputRelations)
    val l:List[String] =json.extract[List[String]]
    assertNotNull(l)
    assertTrue(l.nonEmpty)


    val relVocbaulary =ScholixUtils.relations

    l.foreach(r =>  assertTrue(relVocbaulary.contains(r.toLowerCase)))






  }




}
