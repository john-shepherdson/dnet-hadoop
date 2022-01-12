package eu.dnetlib.dhp.oa.graph.resolution

import com.fasterxml.jackson.databind.ObjectMapper
import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.common.HdfsSupport
import eu.dnetlib.dhp.schema.oaf.Relation
import eu.dnetlib.dhp.utils.DHPUtils
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.json4s
import org.json4s.DefaultFormats
import org.json4s.JsonAST.{JField, JObject, JString}
import org.json4s.jackson.JsonMethods.parse
import org.slf4j.{Logger, LoggerFactory}

object SparkResolveRelation {

  def main(args: Array[String]): Unit = {
    val log: Logger = LoggerFactory.getLogger(getClass)
    val conf: SparkConf = new SparkConf()
    val parser = new ArgumentApplicationParser(
      IOUtils.toString(
        getClass.getResourceAsStream(
          "/eu/dnetlib/dhp/oa/graph/resolution/resolve_relations_params.json"
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

    val targetPath = parser.get("targetPath")
    log.info(s"targetPath  -> $targetPath")

    implicit val relEncoder: Encoder[Relation] = Encoders.kryo(classOf[Relation])
    import spark.implicits._

    //CLEANING TEMPORARY FOLDER
    HdfsSupport.remove(workingPath, spark.sparkContext.hadoopConfiguration)
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.mkdirs(new Path(workingPath))

    extractPidResolvedTableFromJsonRDD(spark, graphBasePath, workingPath)

    val mapper: ObjectMapper = new ObjectMapper()

    val rPid: Dataset[(String, String)] =
      spark.read.load(s"$workingPath/relationResolvedPid").as[(String, String)]

    val relationDs: Dataset[(String, Relation)] = spark.read
      .text(s"$graphBasePath/relation")
      .as[String]
      .map(s => mapper.readValue(s, classOf[Relation]))
      .as[Relation]
      .map(r => (r.getSource.toLowerCase, r))(Encoders.tuple(Encoders.STRING, relEncoder))

    relationDs
      .joinWith(rPid, relationDs("_1").equalTo(rPid("_2")), "left")
      .map { m =>
        val sourceResolved = m._2
        val currentRelation = m._1._2
        if (sourceResolved != null && sourceResolved._1 != null && sourceResolved._1.nonEmpty)
          currentRelation.setSource(sourceResolved._1)
        currentRelation
      }
      .write
      .mode(SaveMode.Overwrite)
      .save(s"$workingPath/relationResolvedSource")

    val relationSourceResolved: Dataset[(String, Relation)] = spark.read
      .load(s"$workingPath/relationResolvedSource")
      .as[Relation]
      .map(r => (r.getTarget.toLowerCase, r))(Encoders.tuple(Encoders.STRING, relEncoder))
    relationSourceResolved
      .joinWith(rPid, relationSourceResolved("_1").equalTo(rPid("_2")), "left")
      .map { m =>
        val targetResolved = m._2
        val currentRelation = m._1._2
        if (targetResolved != null && targetResolved._1.nonEmpty)
          currentRelation.setTarget(targetResolved._1)
        currentRelation
      }
      .write
      .mode(SaveMode.Overwrite)
      .save(s"$workingPath/relation_resolved")

    spark.read
      .load(s"$workingPath/relation_resolved")
      .as[Relation]
      .filter(r => !r.getSource.startsWith("unresolved") && !r.getTarget.startsWith("unresolved"))
      .map(r => mapper.writeValueAsString(r))
      .write
      .option("compression", "gzip")
      .mode(SaveMode.Overwrite)
      .text(s"$targetPath/relation")
  }

  def extractInstanceCF(input: String): List[(String, String)] = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json: json4s.JValue = parse(input)
    val result: List[(String, String)] = for {
      JObject(iObj)                                 <- json \ "instance"
      JField("collectedfrom", JObject(cf))          <- iObj
      JField("instancetype", JObject(instancetype)) <- iObj
      JField("value", JString(collectedFrom))       <- cf
      JField("classname", JString(classname))       <- instancetype
    } yield (classname, collectedFrom)

    result

  }

  def extractPidsFromRecord(input: String): (String, List[(String, String)]) = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json: json4s.JValue = parse(input)
    val id: String = (json \ "id").extract[String]
    val result: List[(String, String)] = for {
      JObject(pids)                           <- json \\ "instance" \ "pid"
      JField("value", JString(pidValue))      <- pids
      JField("qualifier", JObject(qualifier)) <- pids
      JField("classid", JString(pidType))     <- qualifier
    } yield (pidValue, pidType)

    (id, result)
  }

  private def isRelation(input: String): Boolean = {

    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json: json4s.JValue = parse(input)
    val source = (json \ "source").extractOrElse[String](null)

    source != null
  }

  def extractPidResolvedTableFromJsonRDD(
    spark: SparkSession,
    graphPath: String,
    workingPath: String
  ) = {
    import spark.implicits._

    val d: RDD[(String, String)] = spark.sparkContext
      .textFile(s"$graphPath/*")
      .filter(i => !isRelation(i))
      .map(i => extractPidsFromRecord(i))
      .filter(s => s != null && s._1 != null && s._2 != null && s._2.nonEmpty)
      .flatMap { p =>
        p._2.map(pid => (p._1, DHPUtils.generateUnresolvedIdentifier(pid._1, pid._2)))
      }
      .filter(r => r._1 != null || r._2 != null)

    spark
      .createDataset(d)
      .groupByKey(_._2)
      .reduceGroups((x, y) => if (x._1.startsWith("50|doi") || x._1.startsWith("50|pmid")) x else y)
      .map(s => s._2)
      .write
      .mode(SaveMode.Overwrite)
      .save(s"$workingPath/relationResolvedPid")
  }

}
