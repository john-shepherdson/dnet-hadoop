package eu.dnetlib.dhp.doiboost


import eu.dnetlib.doiboost.DoiBoostMappingUtil
import org.junit.jupiter.api.Test

class DoiBoostHostedByMapTest {

  @Test
  def testLoadMap(): Unit = {
    println(DoiBoostMappingUtil.retrieveHostedByMap().keys.size)


  }

}
