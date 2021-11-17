package eu.dnetlib.doiboost

import eu.dnetlib.dhp.schema.oaf.{Publication, Dataset => OafDataset}
import eu.dnetlib.doiboost.{DoiBoostMappingUtil, HostedByItemType}
import eu.dnetlib.doiboost.SparkGenerateDoiBoost.getClass
import eu.dnetlib.doiboost.mag.ConversionUtil
import eu.dnetlib.doiboost.orcid.ORCIDElement
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SaveMode, SparkSession}
import org.codehaus.jackson.map.{ObjectMapper, SerializationConfig}
import org.junit.jupiter.api.Test

import scala.io.Source

class DoiBoostHostedByMapTest {



//  @Test
//  def testMerge():Unit = {
//    val conf: SparkConf = new SparkConf()
//    val spark: SparkSession =
//      SparkSession
//        .builder()
//        .config(conf)
//        .appName(getClass.getSimpleName)
//        .master("local[*]").getOrCreate()
//
//
//
//    implicit val mapEncoderPub: Encoder[Publication] = Encoders.kryo[Publication]
//    implicit val mapEncoderDataset: Encoder[OafDataset] = Encoders.kryo[OafDataset]
//    implicit val tupleForJoinEncoder: Encoder[(String, Publication)] = Encoders.tuple(Encoders.STRING, mapEncoderPub)
//
//
//    import spark.implicits._
//    val dataset:RDD[String]= spark.sparkContext.textFile("/home/sandro/Downloads/hbMap.gz")
//
//
//    val hbMap:Dataset[(String, HostedByItemType)] =spark.createDataset(dataset.map(DoiBoostMappingUtil.toHostedByItem))
//
//
//    hbMap.show()
//
//
//
//
//
//
//
//
//
//
//  }


  @Test
  def idDSGeneration():Unit = {
    val s ="doajarticles::0066-782X"



    println(DoiBoostMappingUtil.generateDSId(s))


  }


}
