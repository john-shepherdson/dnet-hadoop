package eu.dnetlib.dhp.sx.pangaea

import eu.dnetlib.dhp.sx.graph.pangaea.PangaeaUtils
import org.junit.jupiter.api.Test

import java.util.TimeZone
import java.text.SimpleDateFormat
import java.util.Date
import scala.io.Source
class PangaeaTransformTest {



  @Test
  def test_dateStamp() :Unit ={



    val  d = new Date()

    val s:String =  s"${new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")format d}Z"
    println(s)


    val xml = Source.fromInputStream(getClass.getResourceAsStream("input.xml")).mkString
    println(PangaeaUtils.parseXml(xml))
  }

}
