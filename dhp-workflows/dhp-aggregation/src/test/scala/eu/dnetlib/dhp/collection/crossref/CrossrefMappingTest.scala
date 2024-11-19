package eu.dnetlib.dhp.collection.crossref

import com.fasterxml.jackson.databind.ObjectMapper
import eu.dnetlib.dhp.aggregation.AbstractVocabularyTest
import eu.dnetlib.dhp.collection.crossref.Crossref2Oaf.TransformationType
import org.apache.commons.io.IOUtils
import org.junit.jupiter.api.{BeforeEach, Test}
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.junit.jupiter.MockitoExtension
import org.slf4j.{Logger, LoggerFactory}

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

    Crossref2Oaf.convert(input, vocabularies, TransformationType.All).foreach(record => {
      println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(record))
    })

  }


  @Test
  def mappingAffiliation(): Unit = {
    val input =
      IOUtils.toString(getClass.getResourceAsStream("/eu/dnetlib/dhp/collection/crossref/affiliationTest.json"), "utf-8")
    val data = Crossref2Oaf.convert(input, vocabularies, TransformationType.OnlyResult)
    println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(data.head))
  }
}
