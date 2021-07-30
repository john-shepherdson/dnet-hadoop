package eu.dnetlib.dhp.oa.graph.hostedbymap

import com.fasterxml.jackson.databind.ObjectMapper
import eu.dnetlib.dhp.oa.graph.hostedbymap.model.DatasourceInfo
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

class TestPrepare extends java.io.Serializable{

  @Test
  def testPrepareDatasource():Unit = {
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

    val ds :Dataset[DatasourceInfo]= SparkPrepareHostedByInfoToApply.prepareDatasourceInfo(spark, path)

    val mapper :ObjectMapper = new ObjectMapper()

    assertEquals(9,  ds.count)

    assertEquals(8, ds.filter(hbi => !hbi.getIssn.equals("")).count)
    assertEquals(5, ds.filter(hbi => !hbi.getEissn.equals("")).count)
    assertEquals(0, ds.filter(hbi => !hbi.getLissn.equals("")).count)

    assertEquals(0, ds.filter(hbi => hbi.getIssn.equals("") && hbi.getEissn.equals("") && hbi.getLissn.equals("")).count)

    assertTrue(ds.filter(hbi => hbi.getIssn.equals("0212-8365")).count == 1)
    assertTrue(ds.filter(hbi => hbi.getEissn.equals("2253-900X")).count == 1)
    assertTrue(ds.filter(hbi => hbi.getIssn.equals("0212-8365") && hbi.getEissn.equals("2253-900X")).count == 1)
    assertTrue(ds.filter(hbi => hbi.getIssn.equals("0212-8365") && hbi.getOfficialname.equals("Thémata")).count == 1)
    assertTrue(ds.filter(hbi => hbi.getIssn.equals("0212-8365") && hbi.getId.equals("10|doajarticles::abbc9265bea9ff62776a1c39785af00c")).count == 1)
    ds.foreach(hbi => assertTrue(hbi.getId.startsWith("10|")))
    ds.foreach(e => println(mapper.writeValueAsString(e)))
    spark.close()
  }

  @Test
  def testPrepareHostedByMap():Unit = {

  }


}
