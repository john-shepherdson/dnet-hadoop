package eu.dnetlib.dhp.collection.crossref

import com.fasterxml.jackson.databind.ObjectMapper
import eu.dnetlib.dhp.aggregation.AbstractVocabularyTest
import eu.dnetlib.dhp.collection.crossref.Crossref2Oaf.TransformationType
import eu.dnetlib.dhp.schema.oaf.Publication
import org.apache.commons.io.IOUtils
import org.junit.jupiter.api.{Assertions, BeforeEach, Test}
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.junit.jupiter.MockitoExtension
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters.asScalaBufferConverter

@ExtendWith(Array(classOf[MockitoExtension]))
class CrossrefMappingTest extends AbstractVocabularyTest {

  val logger: Logger = LoggerFactory.getLogger(Crossref2Oaf.getClass)
  val mapper = new ObjectMapper()

  @BeforeEach
  def setUp(): Unit = {
    super.setUpVocabulary()
  }

  @Test
  def mappingRecord(): Unit = {
    val input =
      IOUtils.toString(getClass.getResourceAsStream("/eu/dnetlib/dhp/collection/crossref/issn_pub.json"), "utf-8")

    Crossref2Oaf
      .convert(input, vocabularies, TransformationType.All)
      .foreach(record => {
        Assertions.assertNotNull(record)
      })

  }

  @Test
  def mappingAffiliation(): Unit = {
    val input =
      IOUtils.toString(
        getClass.getResourceAsStream("/eu/dnetlib/dhp/collection/crossref/affiliationTest.json"),
        "utf-8"
      )
    val data = Crossref2Oaf.convert(input, vocabularies, TransformationType.OnlyResult)
    data.foreach(record => {
      Assertions.assertNotNull(record)
      Assertions.assertTrue(record.isInstanceOf[Publication])
      val publication = record.asInstanceOf[Publication]
      publication.getAuthor.asScala.foreach(author => {
        Assertions.assertNotNull(author.getRawAffiliationString)
        Assertions.assertTrue(author.getRawAffiliationString.size() > 0)

      })
    })
    println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(data.head))
  }
}
