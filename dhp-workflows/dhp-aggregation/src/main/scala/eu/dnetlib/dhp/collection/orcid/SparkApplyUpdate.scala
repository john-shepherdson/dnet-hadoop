package eu.dnetlib.dhp.collection.orcid

import eu.dnetlib.dhp.application.AbstractScalaApplication
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

class SparkApplyUpdate(propertyPath: String, args: Array[String], log: Logger)
    extends AbstractScalaApplication(propertyPath, args, log: Logger) {

  /** Here all the spark applications runs this method
    * where the whole logic of the spark node is defined
    */
  override def run(): Unit = {

    val graphPath: String = parser.get("graphPath")
    log.info("found parameters graphPath: {}", graphPath)
    val updatePath: String = parser.get("updatePath")
    log.info("found parameters updatePath: {}", updatePath)
    val targetPath: String = parser.get("targetPath")
    log.info("found parameters targetPath: {}", targetPath)
    applyTableUpdate(spark, graphPath, updatePath, targetPath)
    checkUpdate(spark, graphPath, targetPath)
    moveTable(spark, graphPath, targetPath)

  }

  private def moveTable(spark: SparkSession, graphPath: String, updatePath: String): Unit = {
    spark.read
      .load(s"$updatePath/Authors")
      .repartition(1000)
      .write
      .mode(SaveMode.Overwrite)
      .save(s"$graphPath/Authors")
    spark.read
      .load(s"$updatePath/Works")
      .repartition(1000)
      .write
      .mode(SaveMode.Overwrite)
      .save(s"$graphPath/Works")
    spark.read
      .load(s"$updatePath/Employments")
      .repartition(1000)
      .write
      .mode(SaveMode.Overwrite)
      .save(s"$graphPath/Employments")

  }

  private def updateDataset(
    inputDataset: DataFrame,
    idUpdate: DataFrame,
    updateDataframe: DataFrame,
    targetPath: String
  ): Unit = {
    inputDataset
      .join(idUpdate, inputDataset("orcid").equalTo(idUpdate("orcid")), "leftanti")
      .select(inputDataset("*"))
      .unionByName(updateDataframe)
      .write
      .mode(SaveMode.Overwrite)
      .save(targetPath)
  }

  private def checkUpdate(spark: SparkSession, graphPath: String, updatePath: String): Unit = {
    val totalOriginalAuthors = spark.read.load(s"$graphPath/Authors").count
    val totalOriginalWorks = spark.read.load(s"$graphPath/Works").count
    val totalOriginalEmployments = spark.read.load(s"$graphPath/Employments").count
    val totalUpdateAuthors = spark.read.load(s"$updatePath/Authors").count
    val totalUpdateWorks = spark.read.load(s"$updatePath/Works").count
    val totalUpdateEmployments = spark.read.load(s"$updatePath/Employments").count

    log.info("totalOriginalAuthors: {}", totalOriginalAuthors)
    log.info("totalOriginalWorks: {}", totalOriginalWorks)
    log.info("totalOriginalEmployments: {}", totalOriginalEmployments)
    log.info("totalUpdateAuthors: {}", totalUpdateAuthors)
    log.info("totalUpdateWorks: {}", totalUpdateWorks)
    log.info("totalUpdateEmployments: {}", totalUpdateEmployments)
    if (
      totalUpdateAuthors < totalOriginalAuthors || totalUpdateEmployments < totalOriginalEmployments || totalUpdateWorks < totalOriginalWorks
    )
      throw new RuntimeException("The updated Graph contains less elements of the original one")

  }

  private def applyTableUpdate(spark: SparkSession, graphPath: String, updatePath: String, targetPath: String): Unit = {
    val orcidIDUpdate = spark.read.load(s"$updatePath/Authors").select("orcid")
    updateDataset(
      spark.read.load(s"$graphPath/Authors"),
      orcidIDUpdate,
      spark.read.load(s"$updatePath/Authors"),
      s"$targetPath/Authors"
    )
    updateDataset(
      spark.read.load(s"$graphPath/Employments"),
      orcidIDUpdate,
      spark.read.load(s"$updatePath/Employments"),
      s"$targetPath/Employments"
    )
    updateDataset(
      spark.read.load(s"$graphPath/Works"),
      orcidIDUpdate,
      spark.read.load(s"$updatePath/Works"),
      s"$targetPath/Works"
    )
  }

}

object SparkApplyUpdate {

  val log: Logger = LoggerFactory.getLogger(SparkGenerateORCIDTable.getClass)

  def main(args: Array[String]): Unit = {

    new SparkApplyUpdate("/eu/dnetlib/dhp/collection/orcid/apply_orcid_table_parameter.json", args, log)
      .initialize()
      .run()

  }
}
