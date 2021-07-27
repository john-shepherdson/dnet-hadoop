package eu.dnetlib.dhp.oa.graph.hostedbymap

import java.sql.Timestamp

import eu.dnetlib.dhp.oa.graph.hostebymap.SparkPrepareHostedByMapData
import eu.dnetlib.dhp.oa.graph.hostebymap.SparkPrepareHostedByMapData.HostedByInfo
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import org.codehaus.jackson.map.ObjectMapper
import org.json4s.DefaultFormats
import org.junit.jupiter.api.Assertions.{assertNotNull, assertTrue}
import org.junit.jupiter.api.Test
import org.slf4j.{Logger, LoggerFactory}

import scala.io.Source

class TestPreprocess {

  val logger: Logger = LoggerFactory.getLogger(getClass)
  val mapper = new ObjectMapper()



  @Test
  def readDatasource():Unit = {


    import org.apache.spark.sql.Encoders
    implicit val formats = DefaultFormats
    import org.json4s.jackson.Serialization.write

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.set("spark.driver.host", "localhost")
    val spark: SparkSession =
      SparkSession
        .builder()
        .appName(getClass.getSimpleName)
        .config(conf)
        .getOrCreate()
    val path = getClass.getResource("datasource.json").getPath


    val schema = Encoders.product[HostedByInfo]

    spark.read.textFile(path).foreach(r => println(mapper.writeValueAsString(r)))

//    SparkPrepareHostedByMapData.readOADataset(path, spark)
//      .foreach(r => println(write(r)))


    spark.close()
  }





}
