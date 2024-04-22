package eu.dnetlib.dhp.collection.mag

import eu.dnetlib.dhp.application.AbstractScalaApplication
import eu.dnetlib.dhp.schema.action.AtomicAction
import eu.dnetlib.dhp.schema.oaf.Organization
import org.apache.spark.sql.{Encoder, Encoders, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

class SparkMagOrganizationAS (propertyPath: String, args: Array[String], log: Logger)
  extends AbstractScalaApplication(propertyPath, args, log: Logger) {

  /** Here all the spark applications runs this method
   * where the whole logic of the spark node is defined
   */
  override def run(): Unit = {
    val magBasePath:String = parser.get("magBasePath")
    log.info(s"magBasePath is $magBasePath")
    val outputPath:String = parser.get("outputPath")
    log.info(s"outputPath is $outputPath")
    generateAS(spark,magBasePath, outputPath)

  }

  def generateAS(spark:SparkSession, magBasePath:String,outputPath:String  ):Unit = {
    import spark.implicits._
    val organizations = MagUtility.loadMagEntity(spark,"Affiliations", magBasePath)
    organizations.map(r => MagUtility.generateOrganization(r)).write.mode(SaveMode.Overwrite)
      .option("compression", "gzip")
      .text(outputPath)
  }
}

object SparkMagOrganizationAS{

  val log: Logger = LoggerFactory.getLogger(SparkMagOrganizationAS.getClass)
  def main(args: Array[String]): Unit = {
    new SparkMagOrganizationAS("/eu/dnetlib/dhp/collection/mag/create_organization_AS.json", args, log)
      .initialize()
      .run()

  }
}
