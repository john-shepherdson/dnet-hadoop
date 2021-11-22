package eu.dnetlib.dhp.datacite


import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import eu.dnetlib.dhp.aggregation.AbstractVocabularyTest
import eu.dnetlib.dhp.schema.oaf.Oaf
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.{BeforeEach, Test}
import org.mockito.junit.jupiter.MockitoExtension

import java.text.SimpleDateFormat
import java.util.Locale
import scala.io.Source

@ExtendWith(Array(classOf[MockitoExtension]))
class DataciteToOAFTest extends  AbstractVocabularyTest{


  @BeforeEach
  def setUp() :Unit = {

    super.setUpVocabulary()
  }


  @Test
  def testDateMapping:Unit = {
    val inputDate = "2021-07-14T11:52:54+0000"
    val ISO8601FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ", Locale.US)
    val dt = ISO8601FORMAT.parse(inputDate)
    println(dt.getTime)


  }


  @Test
  def testMapping() :Unit = {
    val record =Source.fromInputStream(getClass.getResourceAsStream("/eu/dnetlib/dhp/actionmanager/datacite/record.json")).mkString



    val mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT)
    val res:List[Oaf] =DataciteToOAFTransformation.generateOAF(record, 0L,0L, vocabularies, true )

    res.foreach(r => {
      println (mapper.writeValueAsString(r))
      println("----------------------------")

    })



  }

}