package eu.dnetlib.dhp.collection.mag

import eu.dnetlib.dhp.application.AbstractScalaApplication
import eu.dnetlib.dhp.schema.oaf.{Publication, Result}
import org.apache.spark.sql.{Encoder, Encoders, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

class SparkMAGtoOAF(propertyPath: String, args: Array[String], log: Logger)
    extends AbstractScalaApplication(propertyPath, args, log: Logger) {

  /** Here all the spark applications runs this method
    * where the whole logic of the spark node is defined
    */
  override def run(): Unit = {
    val mdstorePath: String = parser.get("mdstorePath")
    log.info("found parameters mdstorePath: {}", mdstorePath)
    val workingPath: String = parser.get("workingPath")
    log.info("found parameters workingPath: {}", workingPath)
    convertMAG(spark, workingPath, mdstorePath)
  }

  def convertMAG(spark: SparkSession, workingPath: String, mdStorePath: String): Unit = {
    import spark.implicits._



    spark.read
      .load(s"$workingPath/mag")
      .as[MAGPaper]
      .map(s => MagUtility.convertMAGtoOAF(s))
      .write
      .option("compression", "gzip")
      .mode(SaveMode.Overwrite)
      .text(mdStorePath)

  }
}

object SparkMAGtoOAF {

  val log: Logger = LoggerFactory.getLogger(SparkMAGtoOAF.getClass)

  def main(args: Array[String]): Unit = {
    new SparkMAGtoOAF("/eu/dnetlib/dhp/collection/mag/convert_MAG_to_OAF_properties.json", args, log)
      .initialize()
      .run()
  }
}
