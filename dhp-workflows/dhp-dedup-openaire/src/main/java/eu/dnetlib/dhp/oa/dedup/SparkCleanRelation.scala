package eu.dnetlib.dhp.oa.dedup

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.common.HdfsSupport
import eu.dnetlib.dhp.schema.oaf.Relation
import eu.dnetlib.dhp.utils.ISLookupClientFactory
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.slf4j.LoggerFactory

object SparkCleanRelation {
  private val log = LoggerFactory.getLogger(classOf[SparkCleanRelation])

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val parser = new ArgumentApplicationParser(
      IOUtils.toString(
        classOf[SparkCleanRelation].getResourceAsStream("/eu/dnetlib/dhp/oa/dedup/cleanRelation_parameters.json")
      )
    )
    parser.parseArgument(args)
    val conf = new SparkConf

    new SparkCleanRelation(parser, AbstractSparkAction.getSparkSession(conf))
      .run(ISLookupClientFactory.getLookUpService(parser.get("isLookUpUrl")))
  }
}

class SparkCleanRelation(parser: ArgumentApplicationParser, spark: SparkSession)
    extends AbstractSparkAction(parser, spark) {
  override def run(isLookUpService: ISLookUpService): Unit = {
    val graphBasePath = parser.get("graphBasePath")
    val inputPath = parser.get("inputPath")
    val outputPath = parser.get("outputPath")

    SparkCleanRelation.log.info("graphBasePath: '{}'", graphBasePath)
    SparkCleanRelation.log.info("inputPath: '{}'", inputPath)
    SparkCleanRelation.log.info("outputPath: '{}'", outputPath)

    AbstractSparkAction.removeOutputDir(spark, outputPath)

    val entities =
      Seq("datasource", "project", "organization", "publication", "dataset", "software", "otherresearchproduct")

    val idsSchema = StructType.fromDDL("`id` STRING, `dataInfo` STRUCT<`deletedbyinference`:BOOLEAN,`invisible`:BOOLEAN>")

    val emptyIds = spark.createDataFrame(spark.sparkContext.emptyRDD[Row].setName("empty"),
      idsSchema)

    val ids = entities
      .foldLeft(emptyIds)((ds, entity) => {
        val entityPath = graphBasePath + '/' + entity
        if (HdfsSupport.exists(entityPath, spark.sparkContext.hadoopConfiguration)) {
          ds.union(spark.read.schema(idsSchema).json(entityPath))
        } else {
          ds
        }
      })
      .filter("dataInfo.deletedbyinference != true AND dataInfo.invisible != true")
      .select("id")
      .distinct()

    val relations = spark.read.schema(Encoders.bean(classOf[Relation]).schema).json(inputPath)
      .filter("dataInfo.deletedbyinference != true AND dataInfo.invisible != true")

    AbstractSparkAction.save(
      relations
        .join(ids, col("source") === ids("id"), "leftsemi")
        .join(ids, col("target") === ids("id"), "leftsemi"),
      outputPath,
      SaveMode.Overwrite
    )
  }
}
