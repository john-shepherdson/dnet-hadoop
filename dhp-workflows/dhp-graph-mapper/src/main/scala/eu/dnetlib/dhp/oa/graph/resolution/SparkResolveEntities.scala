package eu.dnetlib.dhp.oa.graph.resolution

import com.fasterxml.jackson.databind.ObjectMapper
import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.schema.common.EntityType
import eu.dnetlib.dhp.schema.oaf.utils.MergeUtils
import eu.dnetlib.dhp.schema.oaf.{Dataset => OafDataset, _}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.slf4j.{Logger, LoggerFactory}

object SparkResolveEntities {

  val mapper = new ObjectMapper()

  val entities = List(
    EntityType.dataset,
    EntityType.publication,
    EntityType.software,
    EntityType.otherresearchproduct
  )

  def main(args: Array[String]): Unit = {
    val log: Logger = LoggerFactory.getLogger(getClass)
    val conf: SparkConf = new SparkConf()
    val parser = new ArgumentApplicationParser(
      IOUtils.toString(
        getClass.getResourceAsStream(
          "/eu/dnetlib/dhp/oa/graph/resolution/resolve_entities_params.json"
        )
      )
    )
    parser.parseArgument(args)
    val spark: SparkSession =
      SparkSession
        .builder()
        .config(conf)
        .appName(getClass.getSimpleName)
        .master(parser.get("master"))
        .getOrCreate()

    val graphBasePath = parser.get("graphBasePath")
    log.info(s"graphBasePath  -> $graphBasePath")
    val workingPath = parser.get("workingPath")
    log.info(s"workingPath  -> $workingPath")
    val unresolvedPath = parser.get("unresolvedPath")
    log.info(s"unresolvedPath  -> $unresolvedPath")

    val targetPath = parser.get("targetPath")
    log.info(s"targetPath  -> $targetPath")

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.mkdirs(new Path(workingPath))

    resolveEntities(spark, workingPath, unresolvedPath)
    generateResolvedEntities(spark, workingPath, graphBasePath, targetPath)
  }

  def resolveEntities(spark: SparkSession, workingPath: String, unresolvedPath: String) = {
    implicit val resEncoder: Encoder[Result] = Encoders.kryo(classOf[Result])
    import spark.implicits._

    val rPid: Dataset[(String, String)] =
      spark.read.load(s"$workingPath/relationResolvedPid").as[(String, String)]
    val up: Dataset[(String, Result)] = spark.read
      .text(unresolvedPath)
      .as[String]
      .map(s => mapper.readValue(s, classOf[Result]))
      .map(r => (r.getId, r))(Encoders.tuple(Encoders.STRING, resEncoder))

    rPid
      .joinWith(up, rPid("_2").equalTo(up("_1")), "inner")
      .map { r =>
        val result = r._2._2
        val dnetId = r._1._1
        result.setId(dnetId)
        result
      }
      .write
      .mode(SaveMode.Overwrite)
      .save(s"$workingPath/resolvedEntities")
  }

  def deserializeObject(input: String, entity: EntityType): Result = {

    entity match {
      case EntityType.publication          => mapper.readValue(input, classOf[Publication])
      case EntityType.dataset              => mapper.readValue(input, classOf[OafDataset])
      case EntityType.software             => mapper.readValue(input, classOf[Software])
      case EntityType.otherresearchproduct => mapper.readValue(input, classOf[OtherResearchProduct])
    }
  }

  def generateResolvedEntities(
    spark: SparkSession,
    workingPath: String,
    graphBasePath: String,
    targetPath: String
  ) = {

    implicit val resEncoder: Encoder[Result] = Encoders.kryo(classOf[Result])
    import spark.implicits._

    val re: Dataset[(String, Result)] = spark.read
      .load(s"$workingPath/resolvedEntities")
      .as[Result]
      .map(r => (r.getId, r))(Encoders.tuple(Encoders.STRING, resEncoder))
    entities.foreach { e =>
      {

        val currentEntityDataset: Dataset[(String, Result)] = spark.read
          .text(s"$graphBasePath/$e")
          .as[String]
          .map(s => deserializeObject(s, e))
          .map(r => (r.getId, r))(Encoders.tuple(Encoders.STRING, resEncoder))

        currentEntityDataset
          .joinWith(re, currentEntityDataset("_1").equalTo(re("_1")), "left")
          .map(k => {
            val a = k._1
            val b = k._2
            if (b == null)
              a._2
            else
              MergeUtils.mergeResult(a._2, b._2)
          })
          .map(r => mapper.writeValueAsString(r))(Encoders.STRING)
          .write
          .mode(SaveMode.Overwrite)
          .option("compression", "gzip")
          .text(s"$targetPath/$e")
      }

    }
  }
}
