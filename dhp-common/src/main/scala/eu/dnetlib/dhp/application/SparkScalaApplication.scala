package eu.dnetlib.dhp.application

import eu.dnetlib.dhp.common.Constants
import eu.dnetlib.dhp.utils.DHPUtils.writeHdfsFile
import org.apache.commons.lang3.StringUtils

import scala.io.Source

/** This is the main Interface SparkApplication
  * where all the Spark Scala class should inherit
  */
trait SparkScalaApplication {

  /** This is the path in the classpath of the json
    * describes all the argument needed to run
    */
  val propertyPath: String

  /** Utility to parse the arguments using the
    * property json in the classpath identified from
    * the variable propertyPath
    *
    * @param args the list of arguments
    */
  def parseArguments(args: Array[String]): ArgumentApplicationParser = {
    val parser = new ArgumentApplicationParser(
      Source.fromInputStream(getClass.getResourceAsStream(propertyPath)).mkString
    )
    parser.parseArgument(args)
    parser
  }

  /** Here all the spark applications runs this method
    * where the whole logic of the spark node is defined
    */
  def run(): Unit
}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.Logger

abstract class AbstractScalaApplication(
  val propertyPath: String,
  val args: Array[String],
  log: Logger
) extends SparkScalaApplication {

  var parser: ArgumentApplicationParser = null

  var spark: SparkSession = null

  def initialize(): SparkScalaApplication = {
    parser = parseArguments(args)
    spark = createSparkSession()
    this
  }

  /** Utility for creating a spark session starting from parser
    *
    * @return a spark Session
    */
  private def createSparkSession(): SparkSession = {
    require(parser != null)

    val conf: SparkConf = new SparkConf()
    val master = parser.get("master")
    log.info(s"Creating Spark session: Master: $master")
    val b = SparkSession
      .builder()
      .config(conf)
      .appName(getClass.getSimpleName)
    if (StringUtils.isNotBlank(master))
      b.master(master)
    b.getOrCreate()
  }

  def reportTotalSize(targetPath: String, outputBasePath: String): Unit = {
    val total_items = spark.read.text(targetPath).count()
    writeHdfsFile(
      spark.sparkContext.hadoopConfiguration,
      s"$total_items",
      outputBasePath + Constants.MDSTORE_SIZE_PATH
    )
  }

}
