package eu.dnetlib.dhp.actionmanager.datacite


import eu.dnetlib.dhp.aggregation.AbstractVocabularyTest
import eu.dnetlib.dhp.schema.oaf.Oaf
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.{BeforeEach, Test}
import org.mockito.junit.jupiter.MockitoExtension
import org.codehaus.jackson.map.ObjectMapper
import scala.io.Source

@ExtendWith(Array(classOf[MockitoExtension]))
class DataciteToOAFTest extends  AbstractVocabularyTest{


  @BeforeEach
  def setUp() :Unit = {

    super.setUpVocabulary()
  }

  @Test
  def testMapping() :Unit = {
    val record =Source.fromInputStream(getClass.getResourceAsStream("record.json")).mkString



    val mapper = new ObjectMapper()
    val res:List[Oaf] =DataciteToOAFTransformation.generateOAF(record, 0L,0L, vocabularies )
    println (mapper.defaultPrettyPrintingWriter().writeValueAsString(res.head))


  }

}