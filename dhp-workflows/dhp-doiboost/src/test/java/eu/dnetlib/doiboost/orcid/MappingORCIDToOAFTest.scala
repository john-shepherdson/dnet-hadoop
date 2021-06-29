package eu.dnetlib.doiboost.orcid

import com.fasterxml.jackson.databind.ObjectMapper
import eu.dnetlib.dhp.schema.oaf.Publication
import eu.dnetlib.doiboost.orcid.SparkConvertORCIDToOAF.getClass
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.slf4j.{Logger, LoggerFactory}
import java.io._
import scala.collection.JavaConversions._
import scala.io.Source

class MappingORCIDToOAFTest {
  val logger: Logger = LoggerFactory.getLogger(ORCIDToOAF.getClass)
  val mapper = new ObjectMapper()

  @Test
  def testExtractData():Unit ={
    val json = Source.fromInputStream(getClass.getResourceAsStream("dataOutput")).mkString
    assertNotNull(json)
    assertFalse(json.isEmpty)
    json.lines.foreach(s => {
      assertNotNull(ORCIDToOAF.extractValueFromInputString(s))
    })
  }

//  @Test
//  def testOAFConvert():Unit ={
//
//    val spark: SparkSession =
//      SparkSession
//        .builder()
//        .appName(getClass.getSimpleName)
//        .master("local[*]").getOrCreate()
//
//
//    SparkConvertORCIDToOAF.run( spark,"/Users/sandro/Downloads/orcid", "/Users/sandro/Downloads/orcid_oaf")
//    implicit val mapEncoderPubs: Encoder[Publication] = Encoders.kryo[Publication]
//
//    val df = spark.read.load("/Users/sandro/Downloads/orcid_oaf").as[Publication]
//    println(df.first.getId)
//    println(mapper.writeValueAsString(df.first()))
//
//
//
//
//  }

  @Test
  def testExtractDat1():Unit ={



    val aList: List[OrcidAuthor] = List(OrcidAuthor("0000-0002-4335-5309", Some("Lucrecia"), Some("Curto"), null, null, null ),
    OrcidAuthor("0000-0001-7501-3330", Some("Emilio"), Some("Malchiodi"), null, null, null ), OrcidAuthor("0000-0002-5490-9186", Some("Sofia"), Some("Noli Truant"), null, null, null ))

    val orcid:ORCIDItem = ORCIDItem("10.1042/BCJ20160876", aList)

    val oaf = ORCIDToOAF.convertTOOAF(orcid)
    assert(oaf.getPid.size() == 1)
    oaf.getPid.toList.foreach(pid => pid.getQualifier.getClassid.equals("doi"))
    oaf.getPid.toList.foreach(pid => pid.getValue.equals("10.1042/BCJ20160876".toLowerCase()))
      println(mapper.writeValueAsString(ORCIDToOAF.convertTOOAF(orcid)))


  }







}
