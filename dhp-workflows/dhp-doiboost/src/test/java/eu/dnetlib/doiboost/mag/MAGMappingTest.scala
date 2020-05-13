package eu.dnetlib.doiboost.mag

import org.codehaus.jackson.map.ObjectMapper
import org.junit.jupiter.api.Test
import org.slf4j.{Logger, LoggerFactory}
import org.junit.jupiter.api.Assertions._
import scala.io.Source


class MAGMappingTest {

  val logger: Logger = LoggerFactory.getLogger(getClass)
  val mapper = new ObjectMapper()


  //@Test
  def testMAGCSV(): Unit = {
    SparkPreProcessMAG.main("-m local[*] -s /data/doiboost/mag/datasets -t /data/doiboost/mag/datasets/preprocess".split(" "))
  }


  @Test
  def buildInvertedIndexTest() :Unit = {
    val json_input = Source.fromInputStream(getClass.getResourceAsStream("invertedIndex.json")).mkString
    val description = ConversionUtil.convertInvertedIndexString(json_input)
    assertNotNull(description)
    assertTrue(description.nonEmpty)

    logger.debug(description)

  }


}
