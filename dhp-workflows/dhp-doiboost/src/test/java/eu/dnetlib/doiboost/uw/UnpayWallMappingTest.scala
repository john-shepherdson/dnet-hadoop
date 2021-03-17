package eu.dnetlib.doiboost.uw


import com.fasterxml.jackson.databind.ObjectMapper
import eu.dnetlib.dhp.schema.oaf.OpenAccessRoute
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
        logger.info("ID :",p.getId)
      }
      assertNotNull(line)
      assertTrue(line.nonEmpty)
    }



     val l = Ilist.lines.next()

    val item = UnpayWallToOAF.convertToOAF(l)

    assertEquals(item.getInstance().get(0).getAccessright.getOpenAccessRoute, OpenAccessRoute.bronze)
    logger.info(mapper.writeValueAsString(item))
  }

}
