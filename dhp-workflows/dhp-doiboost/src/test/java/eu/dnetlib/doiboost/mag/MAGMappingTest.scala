package eu.dnetlib.doiboost.mag

import eu.dnetlib.dhp.schema.oaf.Publication
import org.apache.htrace.fasterxml.jackson.databind.SerializationFeature
import org.apache.spark.SparkConf
import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SaveMode, SparkSession}
import org.codehaus.jackson.map.{ObjectMapper, SerializationConfig}
import org.junit.jupiter.api.Test
import org.slf4j.{Logger, LoggerFactory}
import org.junit.jupiter.api.Assertions._
import org.apache.spark.sql.functions._

import scala.collection.JavaConverters._
import scala.io.Source
import scala.reflect.ClassTag
import scala.util.matching.Regex



class MAGMappingTest {

  val logger: Logger = LoggerFactory.getLogger(getClass)
  val mapper = new ObjectMapper()


  @Test
  def testMAGCSV(): Unit = {
    // SparkPreProcessMAG.main("-m local[*] -s /data/doiboost/mag/datasets -t /data/doiboost/mag/datasets/preprocess".split(" "))

    val sparkConf: SparkConf = new SparkConf

    val spark: SparkSession = SparkSession.builder()
      .config(sparkConf)
      .appName(getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._


    implicit val mapEncoderPubs: Encoder[Publication] = org.apache.spark.sql.Encoders.kryo[Publication]
    implicit val longBarEncoder = Encoders.tuple(Encoders.STRING, mapEncoderPubs)

    val sourcePath = "/data/doiboost/mag/input"

    mapper.getSerializationConfig.enable(SerializationConfig.Feature.INDENT_OUTPUT)


   val magOAF = spark.read.load("$sourcePath/merge_step_4").as[Publication]

   println(magOAF.first().getOriginalId)


   magOAF.map(k => (ConversionUtil.extractMagIdentifier(k.getOriginalId.asScala),k)).as[(String,Publication)].show()


    println((ConversionUtil.extractMagIdentifier(magOAF.first().getOriginalId.asScala)))

    val magIDRegex: Regex = "^[0-9]+$".r


    println(magIDRegex.findFirstMatchIn("suca").isDefined)

  }


  @Test
  def buildInvertedIndexTest(): Unit = {
    val json_input = Source.fromInputStream(getClass.getResourceAsStream("invertedIndex.json")).mkString
    val description = ConversionUtil.convertInvertedIndexString(json_input)
    assertNotNull(description)
    assertTrue(description.nonEmpty)

    logger.debug(description)

  }


}


