package eu.dnetlib.doiboost.mag

import java.sql.Timestamp

import eu.dnetlib.dhp.schema.oaf.Publication
import org.apache.htrace.fasterxml.jackson.databind.SerializationFeature
import org.apache.spark.{SparkConf, SparkContext}
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
  def testSplitter():Unit = {
    val s = "sports.team"


    if (s.contains(".")) {
      println(s.split("\\.")head)
    }

  }



  @Test
  def testDate() :Unit = {

    val p:Timestamp = Timestamp.valueOf("2011-10-02 00:00:00")

    println(p.toString.substring(0,10))

  }



  @Test
  def buildInvertedIndexTest(): Unit = {
    val json_input = Source.fromInputStream(getClass.getResourceAsStream("invertedIndex.json")).mkString
    val description = ConversionUtil.convertInvertedIndexString(json_input)
    assertNotNull(description)
    assertTrue(description.nonEmpty)

    logger.debug(description)

  }
  @Test
  def normalizeDoiTest():Unit = {

    import org.json4s.jackson.Serialization.write
    import org.json4s.DefaultFormats

    implicit val formats = DefaultFormats


    val conf = new SparkConf().setAppName("test").setMaster("local[*]").set("spark.driver.host", "localhost")

    val spark: SparkSession =
      SparkSession
        .builder()
        .appName(getClass.getSimpleName)
        .config(conf)
        .getOrCreate()
    val path = getClass.getResource("magPapers.json").getPath

    import org.apache.spark.sql.Encoders
    val schema = Encoders.product[MagPapers].schema

    import spark.implicits._
    val magPapers :Dataset[MagPapers] = spark.read.option("multiline",true).schema(schema).json(path).as[MagPapers]
    val ret :Dataset[MagPapers] = SparkProcessMAG.getDistinctResults(magPapers)
    assertTrue(ret.count == 10)
    ret.take(10).foreach(mp => assertTrue(mp.Doi.equals(mp.Doi.toLowerCase())))

    spark.close()
  }

  @Test
  def normalizeDoiTest2():Unit = {

    import org.json4s.jackson.Serialization.write
    import org.json4s.DefaultFormats

    implicit val formats = DefaultFormats


    val conf = new SparkConf().setAppName("test").setMaster("local[*]").set("spark.driver.host", "localhost")

        val spark: SparkSession =
          SparkSession
            .builder()
            .appName(getClass.getSimpleName)
            .config(conf)
            .getOrCreate()
    val path = getClass.getResource("duplicatedMagPapers.json").getPath

    import org.apache.spark.sql.Encoders
    val schema = Encoders.product[MagPapers].schema

    import spark.implicits._
    val magPapers :Dataset[MagPapers] = spark.read.option("multiline",true).schema(schema).json(path).as[MagPapers]
    val ret :Dataset[MagPapers] = SparkProcessMAG.getDistinctResults(magPapers)
    assertTrue(ret.count == 8)
    ret.take(8).foreach(mp => assertTrue(mp.Doi.equals(mp.Doi.toLowerCase())))
    spark.close()
    //ret.take(8).foreach(mp => println(write(mp)))
  }


}


