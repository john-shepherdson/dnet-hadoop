package eu.dnetlib.dhp.oa.graph.hostedbymap

import eu.dnetlib.dhp.schema.oaf.Datasource
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class TestPreprocess extends java.io.Serializable {

  implicit val mapEncoderDats: Encoder[Datasource] = Encoders.kryo[Datasource]
  implicit val schema = Encoders.product[HostedByInfo]

  def toHBIString(hbi: HostedByItemType): String = {
    implicit val formats = DefaultFormats

    write(hbi)
  }

  @Test
  def readDatasource(): Unit = {
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

    val ds: Dataset[HostedByItemType] = SparkProduceHostedByMap.oaHostedByDataset(spark, path)

    assertEquals(9, ds.count)

    assertEquals(8, ds.filter(hbi => !hbi.issn.equals("")).count)
    assertEquals(5, ds.filter(hbi => !hbi.eissn.equals("")).count)
    assertEquals(0, ds.filter(hbi => !hbi.lissn.equals("")).count)

    assertEquals(
      0,
      ds.filter(hbi => hbi.issn.equals("") && hbi.eissn.equals("") && hbi.lissn.equals("")).count
    )

    assertTrue(ds.filter(hbi => hbi.issn.equals("0212-8365")).count == 1)
    assertTrue(ds.filter(hbi => hbi.eissn.equals("2253-900X")).count == 1)
    assertTrue(
      ds.filter(hbi => hbi.issn.equals("0212-8365") && hbi.eissn.equals("2253-900X")).count == 1
    )
    assertTrue(
      ds.filter(hbi => hbi.issn.equals("0212-8365") && hbi.officialname.equals("Thémata")).count == 1
    )
    assertTrue(
      ds.filter(hbi =>
        hbi.issn.equals("0212-8365") && hbi.id
          .equals("10|doajarticles::abbc9265bea9ff62776a1c39785af00c")
      ).count == 1
    )
    ds.foreach(hbi => assertTrue(hbi.id.startsWith("10|")))
    ds.foreach(hbi => println(toHBIString(hbi)))
    spark.close()
  }

  @Test
  def readGold(): Unit = {
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

    val ds: Dataset[HostedByItemType] = SparkProduceHostedByMap.goldHostedByDataset(spark, path)

    assertEquals(29, ds.count)

    assertEquals(29, ds.filter(hbi => !hbi.issn.equals("")).count)
    assertEquals(0, ds.filter(hbi => !hbi.eissn.equals("")).count)
    assertEquals(29, ds.filter(hbi => !hbi.lissn.equals("")).count)

    assertEquals(
      0,
      ds.filter(hbi => hbi.issn.equals("") && hbi.eissn.equals("") && hbi.lissn.equals("")).count
    )

    assertTrue(
      ds.filter(hbi => hbi.issn.equals("2239-6101"))
        .first()
        .officialname
        .equals("European journal of sustainable development.")
    )
    assertTrue(ds.filter(hbi => hbi.issn.equals("2239-6101")).first().lissn.equals("2239-5938"))
    assertTrue(ds.filter(hbi => hbi.issn.equals("2239-6101")).count == 1)
    ds.foreach(hbi => assertTrue(hbi.id.equals(Constants.UNIBI)))
    ds.foreach(hbi => println(toHBIString(hbi)))

    spark.close()
  }

  @Test
  def readDoaj(): Unit = {
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

    val ds: Dataset[HostedByItemType] = SparkProduceHostedByMap.doajHostedByDataset(spark, path)

    assertEquals(25, ds.count)

    assertEquals(14, ds.filter(hbi => !hbi.issn.equals("")).count)
    assertEquals(21, ds.filter(hbi => !hbi.eissn.equals("")).count)
    assertEquals(0, ds.filter(hbi => !hbi.lissn.equals("")).count)

    assertEquals(
      0,
      ds.filter(hbi => hbi.issn.equals("") && hbi.eissn.equals("") && hbi.lissn.equals("")).count
    )

    assertTrue(
      ds.filter(hbi => hbi.issn.equals("2077-3099"))
        .first()
        .officialname
        .equals("Journal of Space Technology")
    )
    assertTrue(ds.filter(hbi => hbi.issn.equals("2077-3099")).first().eissn.equals("2411-5029"))
    assertTrue(ds.filter(hbi => hbi.issn.equals("2077-3099")).count == 1)
    assertTrue(ds.filter(hbi => hbi.eissn.equals("2077-2955")).first().issn.equals(""))
    ds.foreach(hbi => assertTrue(hbi.id.equals(Constants.DOAJ)))
    ds.foreach(hbi => println(toHBIString(hbi)))

    spark.close()
  }

  @Test
  def testAggregator(): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.set("spark.driver.host", "localhost")
    val spark: SparkSession =
      SparkSession
        .builder()
        .appName(getClass.getSimpleName)
        .config(conf)
        .getOrCreate()

    val tmp = SparkProduceHostedByMap
      .oaHostedByDataset(spark, getClass.getResource("datasource.json").getPath)
      .union(
        SparkProduceHostedByMap
          .goldHostedByDataset(spark, getClass.getResource("unibi_transformed.json").getPath)
      )
      .union(
        SparkProduceHostedByMap
          .doajHostedByDataset(spark, getClass.getResource("doaj_transformed.json").getPath)
      )
      .flatMap(hbi => SparkProduceHostedByMap.toList(hbi))(
        Encoders.tuple(Encoders.STRING, Encoders.product[HostedByItemType])
      )

    assertEquals(106, tmp.count)
    assertEquals(82, tmp.map(i => i._1)(Encoders.STRING).distinct().count)

    val ds: Dataset[(String, HostedByItemType)] = Aggregators.explodeHostedByItemType(
      SparkProduceHostedByMap
        .oaHostedByDataset(spark, getClass.getResource("datasource.json").getPath)
        .union(
          SparkProduceHostedByMap
            .goldHostedByDataset(spark, getClass.getResource("unibi_transformed.json").getPath)
        )
        .union(
          SparkProduceHostedByMap
            .doajHostedByDataset(spark, getClass.getResource("doaj_transformed.json").getPath)
        )
        .flatMap(hbi => SparkProduceHostedByMap.toList(hbi))(
          Encoders.tuple(Encoders.STRING, Encoders.product[HostedByItemType])
        )
    )

    assertEquals(82, ds.count)

    assertEquals(13, ds.filter(i => i._2.id.startsWith("10|")).count)

    assertTrue(ds.filter(i => i._1.equals("2077-3757")).first()._2.id.startsWith("10|"))
    assertTrue(ds.filter(i => i._1.equals("2077-3757")).first()._2.openAccess)
    assertEquals(1, ds.filter(i => i._1.equals("2077-3757")).count)

    val hbmap: Dataset[String] = ds
      .filter(hbi => hbi._2.id.startsWith("10|"))
      .map(SparkProduceHostedByMap.toHostedByMap)(Encoders.STRING)

    hbmap.foreach(entry => println(entry))
    spark.close()

  }

}
