package eu.dnetlib.dhp.sx.graph

import com.fasterxml.jackson.databind.ObjectMapper
import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.schema.oaf.{Relation, Result}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.json4s
import org.json4s.DefaultFormats
import org.json4s.JsonAST.{JField, JObject, JString}
import org.json4s.jackson.JsonMethods.parse
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
object SparkResolveRelation {
  def main(args: Array[String]): Unit = {
    val log: Logger = LoggerFactory.getLogger(getClass)
    val conf: SparkConf = new SparkConf()
    val parser = new ArgumentApplicationParser(IOUtils.toString(getClass.getResourceAsStream("/eu/dnetlib/dhp/sx/graph/resolve_relations_params.json")))
    parser.parseArgument(args)
    val spark: SparkSession =
      SparkSession
        .builder()
        .config(conf)
        .appName(getClass.getSimpleName)
        .master(parser.get("master")).getOrCreate()


    val relationPath = parser.get("relationPath")
    log.info(s"sourcePath  -> $relationPath")
    val entityPath = parser.get("entityPath")
    log.info(s"entityPath  -> $entityPath")
    val workingPath = parser.get("workingPath")
    log.info(s"workingPath  -> $workingPath")

    implicit  val relEncoder: Encoder[Relation] = Encoders.kryo(classOf[Relation])
    import spark.implicits._


    extractPidResolvedTableFromJsonRDD(spark, entityPath, workingPath)

    val mappper = new ObjectMapper()

    val rPid:Dataset[(String,String)] = spark.read.load(s"$workingPath/relationResolvedPid").as[(String,String)]

    val relationDs:Dataset[(String,Relation)] = spark.read.load(relationPath).as[Relation].map(r => (r.getSource.toLowerCase, r))(Encoders.tuple(Encoders.STRING, relEncoder))

    relationDs.joinWith(rPid, relationDs("_1").equalTo(rPid("_2")), "left").map{
      m =>
        val sourceResolved = m._2
        val currentRelation = m._1._2
        if (sourceResolved!=null && sourceResolved._1!=null && sourceResolved._1.nonEmpty)
          currentRelation.setSource(sourceResolved._1)
        currentRelation
    }.write
      .mode(SaveMode.Overwrite)
      .save(s"$workingPath/relationResolvedSource")


    val relationSourceResolved:Dataset[(String,Relation)] = spark.read.load(s"$workingPath/relationResolvedSource").as[Relation].map(r => (r.getTarget.toLowerCase, r))(Encoders.tuple(Encoders.STRING, relEncoder))
    relationSourceResolved.joinWith(rPid, relationSourceResolved("_1").equalTo(rPid("_2")), "left").map{
      m =>
        val targetResolved = m._2
        val currentRelation = m._1._2
        if (targetResolved!=null && targetResolved._1.nonEmpty)
          currentRelation.setTarget(targetResolved._1)
        currentRelation
    }.filter(r => r.getSource.startsWith("50")&& r.getTarget.startsWith("50"))
      .write
      .mode(SaveMode.Overwrite)
      .save(s"$workingPath/relation_resolved")

    spark.read.load(s"$workingPath/relation_resolved").as[Relation]
                    .map(r => mappper.writeValueAsString(r))
                    .rdd.saveAsTextFile(s"$workingPath/relation", classOf[GzipCodec])

  }


  def extractPidsFromRecord(input:String):(String,List[(String,String)]) = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json: json4s.JValue = parse(input)
    val id:String = (json \ "id").extract[String]
    val result: List[(String,String)] = for {
      JObject(pids) <- json \ "pid"
      JField("value", JString(pidValue)) <- pids
      JField("qualifier", JObject(qualifier)) <- pids
      JField("classname", JString(pidType)) <- qualifier
    } yield (pidValue, pidType)

    val alternateIds: List[(String,String)] = for {
      JObject(pids) <- json \\ "alternateIdentifier"
      JField("value", JString(pidValue)) <- pids
      JField("qualifier", JObject(qualifier)) <- pids
      JField("classname", JString(pidType)) <- qualifier
    } yield (pidValue, pidType)

    (id,result:::alternateIds)
  }

  private def extractPidResolvedTableFromJsonRDD(spark: SparkSession, entityPath: String, workingPath: String) = {
    import spark.implicits._

    val d: RDD[(String,String)] = spark.sparkContext.textFile(s"$entityPath/*")
      .map(i => extractPidsFromRecord(i))
      .filter(s => s != null && s._1!= null && s._2!=null && s._2.nonEmpty)
      .flatMap{ p =>
                  p._2.map(pid =>
                    (p._1, convertPidToDNETIdentifier(pid._1, pid._2))
                  )
      }.filter(r =>r._1 != null || r._2 != null)

    spark.createDataset(d)
    .groupByKey(_._2)
      .reduceGroups((x, y) => if (x._1.startsWith("50|doi") || x._1.startsWith("50|pmid")) x else y)
      .map(s => s._2)
      .write
      .mode(SaveMode.Overwrite)
      .save(s"$workingPath/relationResolvedPid")
  }


  /*
    This method should be used once we finally convert everythings in Kryo dataset
    instead of using rdd of json
   */
  private def extractPidResolvedTableFromKryo(spark: SparkSession, entityPath: String, workingPath: String) = {
    import spark.implicits._
    implicit val oafEncoder: Encoder[Result] = Encoders.kryo(classOf[Result])
    val entities: Dataset[Result] = spark.read.load(s"$entityPath/*").as[Result]
    entities.flatMap(e => e.getPid.asScala
      .map(p =>
        convertPidToDNETIdentifier(p.getValue, p.getQualifier.getClassid))
      .filter(s => s != null)
      .map(s => (s, e.getId))
    ).groupByKey(_._1)
      .reduceGroups((x, y) => if (x._2.startsWith("50|doi") || x._2.startsWith("50|pmid")) x else y)
      .map(s => s._2)
      .write
      .mode(SaveMode.Overwrite)
      .save(s"$workingPath/relationResolvedPid")
  }

  def convertPidToDNETIdentifier(pid:String, pidType: String):String = {
    if (pid==null || pid.isEmpty || pidType== null || pidType.isEmpty)
      null
    else
      s"unresolved::${pid.toLowerCase}::${pidType.toLowerCase}"
  }

}