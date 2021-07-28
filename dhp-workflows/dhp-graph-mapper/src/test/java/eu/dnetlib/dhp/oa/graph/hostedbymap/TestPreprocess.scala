package eu.dnetlib.dhp.oa.graph.hostedbymap

import java.sql.Timestamp

import com.fasterxml.jackson.databind.ObjectMapper
import eu.dnetlib.dhp.oa.graph.hostebymap.{Constants, HostedByInfo, SparkPrepareHostedByMapData}
import eu.dnetlib.dhp.schema.oaf.Datasource
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import org.json4s.DefaultFormats
import org.junit.jupiter.api.Assertions.{assertNotNull, assertTrue}
import org.junit.jupiter.api.Test
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer
import scala.io.Source

class TestPreprocess extends java.io.Serializable{

  implicit val mapEncoderDats: Encoder[Datasource] = Encoders.kryo[Datasource]
  implicit val schema = Encoders.product[HostedByInfo]



  @Test
  def readDatasource():Unit = {


    import org.apache.spark.sql.Encoders
    implicit val formats = DefaultFormats

    val logger: Logger = LoggerFactory.getLogger(getClass)
    val mapper = new ObjectMapper()



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


    println(SparkPrepareHostedByMapData.oaHostedByDataset(spark, path).count)



    spark.close()
  }


  @Test
  def readGold():Unit = {

    implicit val formats = DefaultFormats

    val logger: Logger = LoggerFactory.getLogger(getClass)
    val mapper = new ObjectMapper()



    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.set("spark.driver.host", "localhost")
    val spark: SparkSession =
      SparkSession
        .builder()
        .appName(getClass.getSimpleName)
        .config(conf)
        .getOrCreate()
    val path = getClass.getResource("unibi_transformed.json").getPath


    println(SparkPrepareHostedByMapData.goldHostedByDataset(spark, path).count)



    spark.close()
  }

  @Test
  def readDoaj():Unit = {

    implicit val formats = DefaultFormats

    val logger: Logger = LoggerFactory.getLogger(getClass)
    val mapper = new ObjectMapper()



    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.set("spark.driver.host", "localhost")
    val spark: SparkSession =
      SparkSession
        .builder()
        .appName(getClass.getSimpleName)
        .config(conf)
        .getOrCreate()
    val path = getClass.getResource("doaj_transformed.json").getPath


    println(SparkPrepareHostedByMapData.doajHostedByDataset(spark, path).count)



    spark.close()
  }




}
