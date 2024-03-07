package eu.dnetlib.dhp.oa.graph.hostedbymap

import com.fasterxml.jackson.databind.ObjectMapper
import eu.dnetlib.dhp.oa.graph.hostedbymap.SparkPrepareHostedByInfoToApply.{joinResHBM, prepareResultInfo, toEntityInfo}
import eu.dnetlib.dhp.oa.graph.hostedbymap.model.EntityInfo
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import org.json4s.DefaultFormats
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

class TestPrepare extends java.io.Serializable {

  def getString(input: HostedByItemType): String = {

    import org.json4s.jackson.Serialization.write
    implicit val formats = DefaultFormats

    write(input)
  }

  @Test
  def testHostedByMaptoEntityInfo(): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.set("spark.driver.host", "localhost")
    val spark: SparkSession =
      SparkSession
        .builder()
        .appName(getClass.getSimpleName)
        .config(conf)
        .getOrCreate()
    val hbm = getClass.getResource("hostedbymap.json").getPath

    import spark.implicits._

    val mapper: ObjectMapper = new ObjectMapper()

    implicit val mapEncoderDSInfo: Encoder[EntityInfo] = Encoders.bean(classOf[EntityInfo])

    val ds: Dataset[EntityInfo] =
      spark.createDataset(spark.sparkContext.textFile(hbm)).map(toEntityInfo)

    ds.foreach(e => println(mapper.writeValueAsString(e)))

    assertEquals(20, ds.count)
    spark.close()
  }

  @Test
  def testPublicationtoEntityInfo(): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.set("spark.driver.host", "localhost")
    val spark: SparkSession =
      SparkSession
        .builder()
        .appName(getClass.getSimpleName)
        .config(conf)
        .getOrCreate()
    val path = getClass.getResource("publication.json").getPath

    val mapper: ObjectMapper = new ObjectMapper()

    implicit val mapEncoderDSInfo: Encoder[EntityInfo] = Encoders.bean(classOf[EntityInfo])

    val ds: Dataset[EntityInfo] = prepareResultInfo(spark, path)

    ds.foreach(e => println(mapper.writeValueAsString(e)))

    assertEquals(2, ds.count)

    assertEquals(
      "50|4dc99724cf04::ed1ba83e1add6ce292433729acd8b0d9",
      ds.filter(ei => ei.getJournalId.equals("1728-5852")).first().getId
    )
    assertEquals(
      "50|4dc99724cf04::ed1ba83e1add6ce292433729acd8b0d9",
      ds.filter(ei => ei.getJournalId.equals("0001-396X")).first().getId
    )

    spark.close()
  }

  @Test
  def testJoinResHBM(): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.set("spark.driver.host", "localhost")
    val spark: SparkSession =
      SparkSession
        .builder()
        .appName(getClass.getSimpleName)
        .config(conf)
        .getOrCreate()
    val pub = getClass.getResource("iteminfofrompublication").getPath
    val hbm = getClass.getResource("iteminfofromhostedbymap.json").getPath

    val mapper: ObjectMapper = new ObjectMapper()

    implicit val mapEncoderDSInfo: Encoder[EntityInfo] = Encoders.bean(classOf[EntityInfo])

    val pub_ds: Dataset[EntityInfo] =
      spark.read.textFile(pub).map(p => mapper.readValue(p, classOf[EntityInfo]))
    val hbm_ds: Dataset[EntityInfo] =
      spark.read.textFile(hbm).map(p => mapper.readValue(p, classOf[EntityInfo]))

    val ds: Dataset[EntityInfo] = joinResHBM(pub_ds, hbm_ds)

    assertEquals(1, ds.count)

    val ei: EntityInfo = ds.first()

    assertEquals("50|4dc99724cf04::ed1ba83e1add6ce292433729acd8b0d9", ei.getId)
    assertEquals("10|issn___print::e4b6d6d978f67520f6f37679a98c5735", ei.getHostedById)
    assertEquals("0001-396X", ei.getJournalId)
    assertEquals("Academic Therapy", ei.getName)
    assertTrue(!ei.getOpenAccess)

    spark.close()
  }

  @Test
  def testJoinResHBM2(): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.set("spark.driver.host", "localhost")
    val spark: SparkSession =
      SparkSession
        .builder()
        .appName(getClass.getSimpleName)
        .config(conf)
        .getOrCreate()
    val pub = getClass.getResource("iteminfofrompublication2").getPath
    val hbm = getClass.getResource("iteminfofromhostedbymap2.json").getPath

    val mapper: ObjectMapper = new ObjectMapper()

    implicit val mapEncoderDSInfo: Encoder[EntityInfo] = Encoders.bean(classOf[EntityInfo])

    val pub_ds: Dataset[EntityInfo] =
      spark.read.textFile(pub).map(p => mapper.readValue(p, classOf[EntityInfo]))
    val hbm_ds: Dataset[EntityInfo] =
      spark.read.textFile(hbm).map(p => mapper.readValue(p, classOf[EntityInfo]))

    val ds: Dataset[EntityInfo] = joinResHBM(pub_ds, hbm_ds)

    assertEquals(1, ds.count)

    val ei: EntityInfo = ds.first()

    assertEquals("50|4dc99724cf04::ed1ba83e1add6ce292433729acd8b0d9", ei.getId)
    assertEquals("10|issn___print::e4b6d6d978f67520f6f37679a98c5735", ei.getHostedById)
    assertEquals("Academic Therapy", ei.getName)
    assertTrue(ei.getOpenAccess)

    ds.foreach(e => println(mapper.writeValueAsString(e)))

    spark.close()
  }

}
