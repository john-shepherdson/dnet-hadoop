package eu.dnetlib.dhp.sx.graph

import org.junit.jupiter.api.Test

import java.text.SimpleDateFormat

class RetrieveDataciteDeltaTest {

  @Test
  def testParsingDate(): Unit = {

    val inputDate = "2021-12-02T11:17:36+0000"

    val t = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").parse(inputDate).getTime

    println(t)

  }

}
