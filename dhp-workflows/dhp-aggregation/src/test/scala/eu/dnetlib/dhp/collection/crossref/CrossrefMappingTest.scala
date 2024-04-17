package eu.dnetlib.dhp.collection.crossref

import com.fasterxml.jackson.databind.ObjectMapper
import eu.dnetlib.dhp.aggregation.AbstractVocabularyTest
import org.junit.jupiter.api.BeforeEach
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

}
