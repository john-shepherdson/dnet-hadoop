package eu.dnetlib.doiboost.orcid

import eu.dnetlib.dhp.schema.oaf.Publication
import eu.dnetlib.doiboost.orcid.SparkConvertORCIDToOAF.getClass
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.codehaus.jackson.map.ObjectMapper
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.slf4j.{Logger, LoggerFactory}

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







}
