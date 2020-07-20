package eu.dnetlib.dhp.sx.ebi

import org.junit.jupiter.api.Test

class TestEBI {



  @Test
  def testEBIData() = {
    SparkAddLinkUpdates.main("-mt local[*] -w /home/sandro/Downloads".split(" "))






  }

}
