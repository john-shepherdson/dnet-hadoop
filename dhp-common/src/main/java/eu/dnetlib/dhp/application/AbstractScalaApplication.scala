package eu.dnetlib.dhp.application

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.Logger

abstract class AbstractScalaApplication (val propertyPath:String, val args:Array[String], log:Logger) extends  SparkScalaApplication {

  var parser: ArgumentApplicationParser = null

  var spark:SparkSession = null


  def initialize():SparkScalaApplication = {
    parser = parseArguments(args)
    spark = createSparkSession()
    this
  }

  /**
   * Utility for creating a spark session starting from parser
   *
   * @return a spark Session
   */
  private def createSparkSession():SparkSession = {
    require(parser!= null)

    val conf:SparkConf = new SparkConf()
    val master = parser.get("master")
    log.info(s"Creating Spark session: Master: $master")
    SparkSession.builder().config(conf)
      .appName(getClass.getSimpleName)
      .master(master)
      .getOrCreate()
  }

}