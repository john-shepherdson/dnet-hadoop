package eu.dnetlib.dhp.application

import scala.io.Source

/**
 * This is the main Interface SparkApplication
 * where all the Spark Scala class should inherit
 *
 */
trait SparkScalaApplication {
  /**
   * This is the path in the classpath of the json
   * describes all the argument needed to run
   */
  val propertyPath: String

  /**
   * Utility to parse the arguments using the
   * property json in the classpath identified from
   * the variable propertyPath
   *
   * @param args the list of arguments
   */
  def parseArguments(args: Array[String]): ArgumentApplicationParser = {
    val parser = new ArgumentApplicationParser(Source.fromInputStream(getClass.getResourceAsStream(propertyPath)).mkString)
    parser.parseArgument(args)
    parser
  }

  /**
   * Here all the spark applications runs this method
   * where the whole logic of the spark node is defined
   */
  def run(): Unit
}
