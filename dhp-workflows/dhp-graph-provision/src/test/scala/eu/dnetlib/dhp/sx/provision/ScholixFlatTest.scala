package eu.dnetlib.dhp.sx.provision

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.junit.Before

import org.junit.jupiter.api.{Test}

class ScholixFlatTest{


  var spark:SparkSession = null


  def initSpark(): Unit = {

    if (spark!= null)
      return
    println("SONO QUI")
    val conf = new SparkConf
    conf.setAppName(getClass.getSimpleName )
    conf.setMaster("local[*]")
    conf.set("spark.driver.host", "localhost")
    conf.set("hive.metastore.local", "true")
    conf.set("spark.ui.enabled", "false")

    spark = SparkSession
      .builder()
      .appName(getClass.getSimpleName)
    .config(conf)
      .getOrCreate()
  }



  def after(): Unit = {
    spark.stop()
  }


  @Test
  def testScholixConversion (): Unit = {
    initSpark()
    val p = getClass.getResource("/eu/dnetlib/dhp/sx/provision/scholix_dump.zip").getPath

    val t = spark.read.text(p).count
    println(s"total =$t")


  }

}
