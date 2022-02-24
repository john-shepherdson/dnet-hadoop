package eu.dnetlib.dhp.oa.graph.hostedbymap

import com.fasterxml.jackson.databind.ObjectMapper
import eu.dnetlib.dhp.oa.graph.hostedbymap.model.EntityInfo
import eu.dnetlib.dhp.schema.common.ModelConstants
import eu.dnetlib.dhp.schema.oaf.{Datasource, OpenAccessRoute, Publication}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

class TestApply extends java.io.Serializable {

  @Test
  def testApplyOnResult(): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.set("spark.driver.host", "localhost")
    val spark: SparkSession =
      SparkSession
        .builder()
        .appName(getClass.getSimpleName)
        .config(conf)
        .getOrCreate()
    val pub = getClass.getResource("publication.json").getPath
    val hbm = getClass.getResource("preparedInfo.json").getPath

    val mapper: ObjectMapper = new ObjectMapper()

    implicit val mapEncoderDSInfo: Encoder[EntityInfo] = Encoders.bean(classOf[EntityInfo])
    implicit val mapEncoderPubInfo: Encoder[Publication] = Encoders.bean(classOf[Publication])

    val pub_ds: Dataset[Publication] =
      spark.read.textFile(pub).map(p => mapper.readValue(p, classOf[Publication]))
    val hbm_ds: Dataset[EntityInfo] =
      spark.read.textFile(hbm).map(p => mapper.readValue(p, classOf[EntityInfo]))

    assertEquals(13, pub_ds.count())

    val ds: Dataset[Publication] = SparkApplyHostedByMapToResult.applyHBtoPubs(hbm_ds, pub_ds)

    assertEquals(13, ds.count)

    val temp: Dataset[(Publication, Publication)] =
      pub_ds.joinWith(ds, pub_ds.col("id").equalTo(ds.col("id")), "left")
    assertEquals(13, temp.count())
    temp.foreach(t2 => {
      val pb: Publication = t2._1
      val pa: Publication = t2._2
      assertEquals(1, pa.getInstance().size())
      assertEquals(1, pb.getInstance().size())
      assertTrue(t2._1.getId.equals(t2._2.getId))
      if (pb.getId.equals("50|4dc99724cf04::ed1ba83e1add6ce292433729acd8b0d9")) {
        assertTrue(
          pa.getInstance()
            .get(0)
            .getHostedby
            .getKey
            .equals("10|issn___print::e4b6d6d978f67520f6f37679a98c5735")
        )
        assertTrue(pa.getInstance().get(0).getHostedby.getValue.equals("Academic Therapy"))
        assertTrue(pa.getInstance().get(0).getAccessright.getClassid.equals("OPEN"))
        assertTrue(pa.getInstance().get(0).getAccessright.getClassname.equals("Open Access"))
        assertTrue(
          pa.getInstance().get(0).getAccessright.getOpenAccessRoute.equals(OpenAccessRoute.gold)
        )
        assertTrue(pa.getBestaccessright.getClassid.equals("OPEN"))
        assertTrue(pa.getBestaccessright.getClassname.equals("Open Access"))

        assertTrue(
          pb.getInstance()
            .get(0)
            .getHostedby
            .getKey
            .equals("10|openaire____::0b74b6a356bbf23c245f9ae9a748745c")
        )
        assertTrue(
          pb.getInstance()
            .get(0)
            .getHostedby
            .getValue
            .equals("Revistas de investigación Universidad Nacional Mayor de San Marcos")
        )
        assertTrue(pb.getInstance().get(0).getAccessright.getClassname.equals("not available"))
        assertTrue(pb.getInstance().get(0).getAccessright.getClassid.equals("UNKNOWN"))
        assertTrue(pb.getInstance().get(0).getAccessright.getOpenAccessRoute == null)
        assertTrue(pb.getBestaccessright.getClassid.equals("UNKNOWN"))
        assertTrue(pb.getBestaccessright.getClassname.equals("not available"))

      } else {
        assertTrue(
          pa.getInstance()
            .get(0)
            .getHostedby
            .getKey
            .equals(pb.getInstance().get(0).getHostedby.getKey)
        )
        assertTrue(
          pa.getInstance()
            .get(0)
            .getHostedby
            .getValue
            .equals(pb.getInstance().get(0).getHostedby.getValue)
        )
        assertTrue(
          pa.getInstance()
            .get(0)
            .getAccessright
            .getClassid
            .equals(pb.getInstance().get(0).getAccessright.getClassid)
        )
        assertTrue(
          pa.getInstance()
            .get(0)
            .getAccessright
            .getClassname
            .equals(pb.getInstance().get(0).getAccessright.getClassname)
        )
        assertTrue(
          pa.getInstance().get(0).getAccessright.getOpenAccessRoute == pb
            .getInstance()
            .get(0)
            .getAccessright
            .getOpenAccessRoute
        )

      }
    })

    spark.close()
  }

  @Test
  def testApplyOnDatasource(): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.set("spark.driver.host", "localhost")
    val spark: SparkSession =
      SparkSession
        .builder()
        .appName(getClass.getSimpleName)
        .config(conf)
        .getOrCreate()
    val dats = getClass.getResource("datasource.json").getPath
    val hbm = getClass.getResource("preparedInfo2.json").getPath

    val mapper: ObjectMapper = new ObjectMapper()

    implicit val mapEncoderDSInfo: Encoder[EntityInfo] = Encoders.bean(classOf[EntityInfo])
    implicit val mapEncoderPubInfo: Encoder[Datasource] = Encoders.bean(classOf[Datasource])

    val dats_ds: Dataset[Datasource] =
      spark.read.textFile(dats).map(p => mapper.readValue(p, classOf[Datasource]))
    val hbm_ds: Dataset[EntityInfo] = Aggregators.datasourceToSingleId(
      spark.read.textFile(hbm).map(p => mapper.readValue(p, classOf[EntityInfo]))
    )

    assertEquals(10, dats_ds.count())

    val ds: Dataset[Datasource] = SparkApplyHostedByMapToDatasource.applyHBtoDats(hbm_ds, dats_ds)

    assertEquals(10, ds.count)

    val temp: Dataset[(Datasource, Datasource)] =
      dats_ds.joinWith(ds, dats_ds.col("id").equalTo(ds.col("id")), "left")
    assertEquals(10, temp.count())
    temp.foreach(t2 => {
      val pb: Datasource = t2._1
      val pa: Datasource = t2._2
      assertTrue(t2._1.getId.equals(t2._2.getId))
      if (pb.getId.equals("10|doajarticles::0ab37b7620eb9a73ac95d3ca4320c97d")) {
        assertTrue(pa.getOpenairecompatibility().getClassid.equals("hostedBy"))
        assertTrue(
          pa.getOpenairecompatibility()
            .getClassname
            .equals("collected from a compatible aggregator")
        )

        assertTrue(pb.getOpenairecompatibility().getClassid.equals(ModelConstants.UNKNOWN))

      } else {
        assertTrue(
          pa.getOpenairecompatibility().getClassid.equals(pb.getOpenairecompatibility.getClassid)
        )
        assertTrue(
          pa.getOpenairecompatibility()
            .getClassname
            .equals(pb.getOpenairecompatibility.getClassname)
        )

      }
    })

    spark.close()

  }

}
