package eu.dnetlib.dhp.doiboost

import eu.dnetlib.dhp.schema.oaf.{Publication, Dataset => OafDataset}
import eu.dnetlib.doiboost.DoiBoostMappingUtil
import eu.dnetlib.doiboost.SparkGenerateDoiBoost.getClass
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.codehaus.jackson.map.{ObjectMapper, SerializationConfig}
import org.junit.jupiter.api.Test

class DoiBoostHostedByMapTest {

  @Test
  def testLoadMap(): Unit = {
    println(DoiBoostMappingUtil.retrieveHostedByMap().keys.size)


  }


  @Test
  def testFilter():Unit = {
    val conf: SparkConf = new SparkConf()
    val spark: SparkSession =
      SparkSession
        .builder()
        .config(conf)
        .appName(getClass.getSimpleName)
        .master("local[*]").getOrCreate()


    implicit val mapEncoderPub: Encoder[Publication] = Encoders.kryo[Publication]
    implicit val mapEncoderDataset: Encoder[OafDataset] = Encoders.kryo[OafDataset]
    implicit val tupleForJoinEncoder: Encoder[(String, Publication)] = Encoders.tuple(Encoders.STRING, mapEncoderPub)



    val pub =spark.read.load("/data/doiboost/doiboostPublicationFiltered").as[Publication]

    val mapper = new ObjectMapper()

    val map = DoiBoostMappingUtil.retrieveHostedByMap()

   println(pub.map(p => DoiBoostMappingUtil.fixPublication(p, map)).count())


  }


  @Test
  def idDSGeneration():Unit = {
    val s ="doajarticles::0066-782X"



    println(DoiBoostMappingUtil.generateDSId(s))


  }


}
