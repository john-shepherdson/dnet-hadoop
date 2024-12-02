package eu.dnetlib.dhp.datacite

import eu.dnetlib.dhp.application.AbstractScalaApplication
import org.slf4j.{Logger, LoggerFactory}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, from_json, to_timestamp, unix_timestamp}
import org.apache.spark.sql.types._
import org.apache.hadoop.io.Text

class SparkApplyDump (propertyPath: String, args: Array[String], log: Logger)
    extends AbstractScalaApplication(propertyPath, args, log: Logger) {

    override def run(): Unit = {
      val sourcePath = parser.get("sourcePath")
        log.info(s"SourcePath is '$sourcePath'")
      val currentDump = parser.get("currentDump")
        log.info(s"currentDump is '$currentDump'")
      val workingDir = parser.get("workingDir")
        log.info(s"workingDir is '$workingDir'")
      generateDatasetFromSeqDFile(spark , sourcePath, workingDir )

    }


  def generateDatasetFromSeqDFile(spark:SparkSession, sourcePath:String, workingDir:String):Unit = {
    val schema_ddl = "doi STRING, isActive boolean, updated STRING"
    val schema = StructType.fromDDL(schema_ddl)
    import spark.implicits._
    val sc = spark.sparkContext
    sc.sequenceFile(s"$sourcePath/metadata.seq", classOf[Text], classOf[Text])
      .map(x =>x._2.toString)
      .toDF()
      .selectExpr("value as json")
      .withColumn("metadata", from_json(col("json"), schema))
      .selectExpr("lower(metadata.doi) as doi", "metadata.isActive as isActive", "metadata.updated  as ts", "json")
      .select(col("doi"), col("isActive"), unix_timestamp(to_timestamp(col("ts"), "yyyy-MM-dd'T'HH:mm:ss'Z'")).alias("timestamp"), col("json"))
      .write.mode(SaveMode.Overwrite)
      .save(s"$workingDir/datacite_ds")
  }

}

object SparkApplyDump {
  def main(args: Array[String]): Unit = {
    val log = LoggerFactory.getLogger(getClass)
    val app = new SparkApplyDump("/eu/dnetlib/dhp/datacite/generate_dataset_from_dump_params.json", args, log).initialize()
    app.run()
  }
}
