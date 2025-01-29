package eu.dnetlib.dhp.oa.graph.minidump

import eu.dnetlib.dhp.application.AbstractScalaApplication
import eu.dnetlib.dhp.schema.oaf.{Relation, StructuredProperty}
import org.apache.spark.sql.functions.{broadcast, col, from_json}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

class SparkCreateMiniDumpGraph(propertyPath: String, args: Array[String], log: Logger)
    extends AbstractScalaApplication(propertyPath, args, log: Logger) {

  /** Here all the spark applications runs this method
    * where the whole logic of the spark node is defined
    */
  override def run(): Unit = {
    val sourcePath = parser.get("sourcePath")
    log.info("sourcePath: {}", sourcePath)
    val targetPath = parser.get("targetPath")
    log.info("targetPath: {}", targetPath)
    val pidListPath = parser.get("pidListPath")
    log.info("pidListPath: {}", pidListPath)
    generateMiniDump(spark, sourcePath, pidListPath, targetPath)
  }

  def generateMiniDump(
    spark: SparkSession,
    sourcePath: String,
    pidListPath: String,
    targetPath: String
  ): Unit = {
    import spark.implicits._
    val pidSchema = new StructType().add("pid", StringType).add("pidType", StringType)
    val idSchema = new StructType().add("id", StringType)
    val idWithPidSchema = new StructType()
      .add("id", StringType)
      .add(
        "pid",
        ArrayType(
          new StructType().add("value", StringType).add("qualifier", new StructType().add("classid", StringType))
        )
      )

    val pidList = spark.read.schema(pidSchema).json(pidListPath)

    val idPids = spark.read
      .schema(idWithPidSchema)
      .json(s"$sourcePath/*")
      .where("id is not null")
      .selectExpr("explode(pid) as pids", "id")
      .selectExpr("id", "pids.value as pid", "pids.qualifier.classid as pidType")
      .distinct()

    idPids
      .join(broadcast(pidList), pidList("pid") === idPids("pid"))
      .select("id")
      .distinct()
      .write
      .mode(SaveMode.Overwrite)
      .option("compression", "gzip")
      .json(s"$targetPath/usedIds")

    val filerId = spark.read.json(s"$targetPath/usedIds")

    val typologies = List("publication", "dataset", "software", "otherresearchproduct", "project", "organization")

    typologies.foreach(t => {
      println(s"filtering $t")
      val currentEntity = spark.read.text(s"$sourcePath/$t")
      val resultWithId =
        currentEntity.withColumn("jsonData", from_json(col("value"), idSchema)).selectExpr("jsonData.id as id", "value")
      resultWithId
        .join(broadcast(filerId), resultWithId("id") === filerId("id"))
        .select("value")
        .repartition(10)
        .write
        .mode(SaveMode.Overwrite)
        .option("compression", "gzip")
        .text(s"$targetPath/$t")
    })

    val relations = spark.read.schema(Encoders.bean(classOf[Relation]).schema).json(s"$sourcePath/relation")
    val filteredRelations = relations.join(
      broadcast(filerId),
      relations("source") === filerId("id") || relations("target") === filerId("id")
    )

    filteredRelations.write
      .mode(SaveMode.Overwrite)
      .option("compression", "gzip")
      .json(s"$targetPath/relation")

  }
}

object SparkCreateMiniDumpGraph {

  val log: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    new SparkCreateMiniDumpGraph("/eu/dnetlib/dhp/oa/graph/minidump/minidump_params.json", args, log).initialize().run()
  }
}
