package eu.dnetlib.dhp.sx.graph.scholix

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}
import eu.dnetlib.dhp.oa.graph.resolution.SparkResolveRelation
import eu.dnetlib.dhp.schema.oaf.{Relation, Result}
import eu.dnetlib.dhp.schema.sx.scholix.{Scholix, ScholixResource}
import eu.dnetlib.dhp.schema.sx.summary.ScholixSummary
import eu.dnetlib.dhp.sx.graph.bio.pubmed.AbstractVocabularyTest
import org.json4s
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.{BeforeEach, Test}
import org.mockito.junit.jupiter.MockitoExtension

import scala.collection.JavaConverters._
import scala.io.Source

@ExtendWith(Array(classOf[MockitoExtension]))
class ScholixGraphTest extends AbstractVocabularyTest {

  val mapper: ObjectMapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  @BeforeEach
  def setUp(): Unit = {

    super.setUpVocabulary()
  }

  @Test
  def testExtractPids(): Unit = {

    val input = Source
      .fromInputStream(getClass.getResourceAsStream("/eu/dnetlib/dhp/sx/graph/scholix/result.json"))
      .mkString
    val res = SparkResolveRelation.extractPidsFromRecord(input)
    assertNotNull(res)

    assertEquals(1, res._2.size)

  }

  @Test
  def testOAFToSummary(): Unit = {
    val inputRelations = Source
      .fromInputStream(getClass.getResourceAsStream("/eu/dnetlib/dhp/sx/graph/oaf_to_summary"))
      .mkString
    val items = inputRelations.linesWithSeparators.map(l => l.stripLineEnd).toList
    assertNotNull(items)
    items.foreach(i => assertTrue(i.nonEmpty))
    val result =
      items.map(r => mapper.readValue(r, classOf[Result])).map(i => ScholixUtils.resultToSummary(i))

    assertNotNull(result)

    assertEquals(result.size, items.size)
    val d = result.find(s => s.getLocalIdentifier.asScala.exists(i => i.getUrl == null || i.getUrl.isEmpty))
    assertFalse(d.isDefined)
    println(mapper.writeValueAsString(result.head))

  }

  @Test
  def testScholixMergeOnSource(): Unit = {
    val inputRelations = Source
      .fromInputStream(
        getClass.getResourceAsStream("/eu/dnetlib/dhp/sx/graph/merge_result_scholix")
      )
      .mkString
    val result: List[(Relation, ScholixResource)] = inputRelations.linesWithSeparators
      .map(l => l.stripLineEnd)
      .sliding(2, 2)
      .map(s => (s.head, s(1)))
      .map(p =>
        (
          mapper.readValue(p._1, classOf[Relation]),
          ScholixUtils.generateScholixResourceFromSummary(mapper.readValue(p._2, classOf[ScholixSummary]))
        )
      )
      .toList
    assertNotNull(result)
    assertTrue(result.nonEmpty)
    result.foreach(r => assertEquals(r._1.getSource, r._2.getDnetIdentifier))
    val scholix: List[Scholix] = result.map(r => ScholixUtils.scholixFromSource(r._1, r._2))
    println(mapper.writeValueAsString(scholix.head))
  }

  @Test
  def testScholixRelationshipsClean(): Unit = {
    val inputRelations = Source
      .fromInputStream(
        getClass.getResourceAsStream("/eu/dnetlib/dhp/sx/graph/relation_transform.json")
      )
      .mkString
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats

    lazy val json: json4s.JValue = parse(inputRelations)
    val l: List[String] = json.extract[List[String]]
    assertNotNull(l)
    assertTrue(l.nonEmpty)
    val relVocbaulary = ScholixUtils.relations
    l.foreach(r => assertTrue(relVocbaulary.contains(r.toLowerCase)))

  }

}
