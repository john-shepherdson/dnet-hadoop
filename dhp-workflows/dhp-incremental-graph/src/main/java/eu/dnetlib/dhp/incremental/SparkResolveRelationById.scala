package eu.dnetlib.dhp.incremental

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.schema.oaf.Relation
import eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, expr}
import org.slf4j.{Logger, LoggerFactory}

object SparkResolveRelationById {

  def main(args: Array[String]): Unit = {
    val log: Logger = LoggerFactory.getLogger(getClass)
    val conf: SparkConf = new SparkConf()

    val parser = new ArgumentApplicationParser(
      IOUtils.toString(
        getClass.getResourceAsStream(
          "/eu/dnetlib/dhp/oa/graph/incremental/resolution/resolve_relationsbyid_params.json"
        )
      )
    )
    parser.parseArgument(args)
    conf.set("hive.metastore.uris", parser.get("hiveMetastoreUris"))

    val graphBasePath = parser.get("graphBasePath")
    log.info(s"graphBasePath  -> $graphBasePath")
    val relationPath = parser.get("relationPath")
    log.info(s"relationPath  -> $relationPath")
    val targetPath = parser.get("targetGraph")
    log.info(s"targetGraph  -> $targetPath")

    val hiveDbName = parser.get("hiveDbName")
    log.info(s"hiveDbName  -> $hiveDbName")

    val spark: SparkSession =
      SparkSession
        .builder()
        .config(conf)
        .enableHiveSupport()
        .appName(getClass.getSimpleName)
        .getOrCreate()

    implicit val relEncoder: Encoder[Relation] = Encoders.bean(classOf[Relation])

    val mergedrels =
      spark.table(s"${hiveDbName}.relation").where("relclass = 'merges'").selectExpr("source as dedupId", "target as mergedId")

    spark.read
      .schema(Encoders.bean(classOf[Relation]).schema)
      .json(s"$graphBasePath/relation")
      .as[Relation]
      .map(r => resolveRelations(r))
      .join(mergedrels, col("source") === mergedrels.col("mergedId"), "left")
      .withColumn("source", expr("coalesce(dedupId, source)"))
      .drop("mergedId", "dedupID")
      .join(mergedrels, col("target") === mergedrels.col("mergedId"), "left")
      .withColumn("target", expr("coalesce(dedupId, target)"))
      .drop("mergedId", "dedupID")
      .write
      .option("compression", "gzip")
      .mode(SaveMode.Overwrite)
      .json(s"$targetPath/relation")
  }

  private def resolveRelations(r: Relation): Relation = {
    if (r.getSource.startsWith("unresolved::"))
      r.setSource(resolvePid(r.getSource.substring(12)))

    if (r.getTarget.startsWith("unresolved::"))
      r.setTarget(resolvePid(r.getTarget.substring(12)))

    r
  }

  private def resolvePid(str: String): String = {
    val parts = str.split("::")
    val id = parts(0)
    val scheme: String = parts.last match {
      case "arxiv" => "arXiv"
      case _       => parts.last
    }

    IdentifierFactory.idFromPid("50", scheme, id, true)
  }

}
