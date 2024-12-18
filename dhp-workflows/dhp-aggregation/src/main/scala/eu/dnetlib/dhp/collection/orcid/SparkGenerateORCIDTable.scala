package eu.dnetlib.dhp.collection.orcid

import eu.dnetlib.dhp.application.AbstractScalaApplication
import eu.dnetlib.dhp.collection.orcid.model.{Author, Employment, Pid, Work}
import org.apache.hadoop.io.Text
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Encoder, Encoders, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.JavaConverters._

class SparkGenerateORCIDTable(propertyPath: String, args: Array[String], log: Logger)
    extends AbstractScalaApplication(propertyPath, args, log: Logger) {

  /** Here all the spark applications runs this method
    * where the whole logic of the spark node is defined
    */
  override def run(): Unit = {
    val sourcePath: String = parser.get("sourcePath")
    log.info("found parameters sourcePath: {}", sourcePath)
    val targetPath: String = parser.get("targetPath")
    log.info("found parameters targetPath: {}", targetPath)
    val fromUpdate = "true".equals(parser.get("fromUpdate"))
    val sourceSummaryPath = if (fromUpdate) s"$sourcePath/summary*" else sourcePath
    val sourceEmploymentsPath = if (fromUpdate) s"$sourcePath/employments*" else sourcePath
    val sourceWorksPath = if (fromUpdate) s"$sourcePath/works*" else sourcePath
    extractORCIDTable(spark, sourceSummaryPath, targetPath, fromUpdate)
    extractORCIDEmploymentsTable(spark, sourceEmploymentsPath, targetPath, fromUpdate)
    extractORCIDWorksTable(spark, sourceWorksPath, targetPath, fromUpdate)
  }

  def extractORCIDTable(spark: SparkSession, sourcePath: String, targetPath: String, skipFilterByKey: Boolean): Unit = {
    val sc: SparkContext = spark.sparkContext
    import spark.implicits._
    val df = sc
      .sequenceFile(sourcePath, classOf[Text], classOf[Text])
      .map { case (x, y) => (x.toString, y.toString) }
      .toDF
      .as[(String, String)]
    implicit val orcidAuthor: Encoder[Author] = Encoders.bean(classOf[Author])
    val newDf = if (!skipFilterByKey) df.filter(r => r._1.contains("summaries")) else df
    newDf
      .map { r =>
        val p = new OrcidParser
        p.parseSummary(r._2)
      }
      .filter(p => p != null)
      .write
      .mode(SaveMode.Overwrite)
      .save(s"$targetPath/Authors")
  }

  def extractORCIDWorksTable(
    spark: SparkSession,
    sourcePath: String,
    targetPath: String,
    skipFilterByKey: Boolean
  ): Unit = {
    val sc: SparkContext = spark.sparkContext
    import spark.implicits._
    val df = sc
      .sequenceFile(sourcePath, classOf[Text], classOf[Text])
      .map { case (x, y) => (x.toString, y.toString) }
      .toDF
      .as[(String, String)]
    implicit val orcidWorkAuthor: Encoder[Work] = Encoders.bean(classOf[Work])

    //We are in the case of parsing ORCID UPDATE
    if (skipFilterByKey) {
      df.flatMap { r =>
        val p = new OrcidParser
        p.parseWorks(r._2).asScala
      }.filter(p => p != null)
        .write
        .mode(SaveMode.Overwrite)
        .save(s"$targetPath/Works")
    }
    //We are in the case of parsing ORCID DUMP
    else {
      df.filter(r => r._1.contains("works"))
        .map { r =>
          val p = new OrcidParser
          p.parseWork(r._2)
        }
        .filter(p => p != null)
        .write
        .mode(SaveMode.Overwrite)
        .save(s"$targetPath/Works")
    }
  }

  def extractORCIDEmploymentsTable(
    spark: SparkSession,
    sourcePath: String,
    targetPath: String,
    skipFilterByKey: Boolean
  ): Unit = {
    val sc: SparkContext = spark.sparkContext
    import spark.implicits._
    val df = sc
      .sequenceFile(sourcePath, classOf[Text], classOf[Text])
      .map { case (x, y) => (x.toString, y.toString) }
      .toDF
      .as[(String, String)]
    implicit val orcidEmploymentAuthor: Encoder[Employment] = Encoders.bean(classOf[Employment])
    if (skipFilterByKey) {
      df.flatMap { r =>
        val p = new OrcidParser
        p.parseEmployments(r._2).asScala
      }.filter(p => p != null)
        .write
        .mode(SaveMode.Overwrite)
        .save(s"$targetPath/Employments")
    }
    //We are in the case of parsing ORCID DUMP
    else {
      df.filter(r => r._1.contains("employments"))
        .map { r =>
          val p = new OrcidParser
          p.parseEmployment(r._2)
        }
        .filter(p => p != null)
        .write
        .mode(SaveMode.Overwrite)
        .save(s"$targetPath/Employments")
    }
  }
}

object SparkGenerateORCIDTable {

  val log: Logger = LoggerFactory.getLogger(SparkGenerateORCIDTable.getClass)

  def main(args: Array[String]): Unit = {

    new SparkGenerateORCIDTable("/eu/dnetlib/dhp/collection/orcid/generate_orcid_table_parameter.json", args, log)
      .initialize()
      .run()

  }
}
