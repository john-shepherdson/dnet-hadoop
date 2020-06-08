package eu.dnetlib.doiboost.uw

import org.codehaus.jackson.map.{ObjectMapper, SerializationConfig}
import org.junit.jupiter.api.Test

import scala.io.Source
import org.junit.jupiter.api.Assertions._
import org.slf4j.{Logger, LoggerFactory}

class UnpayWallMappingTest {

  val logger: Logger = LoggerFactory.getLogger(getClass)
  val mapper = new ObjectMapper()


  @Test
  def testMappingToOAF():Unit ={

    val Ilist = Source.fromInputStream(getClass.getResourceAsStream("input.json")).mkString


    for (line <-Ilist.lines) {


      val p = UnpayWallToOAF.convertToOAF(line)

      if(p!= null) {
        assertTrue(p.getPid.size()==1)
        logger.info(p.getId)
      }
      assertNotNull(line)
      assertTrue(line.nonEmpty)
    }
    mapper.getSerializationConfig.enable(SerializationConfig.Feature.INDENT_OUTPUT)


     val l = Ilist.lines.next()

    logger.info(mapper.writeValueAsString(UnpayWallToOAF.convertToOAF(l)))







  }

}
