package eu.dnetlib.doiboost.crossref

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Encoder, Encoders, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import org.json4s
import org.json4s.DefaultFormats
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods._

import scala.io.Source

object GenerateCrossrefDatasetSpark {


  val log: Logger = LoggerFactory.getLogger(GenerateCrossrefDatasetSpark.getClass)

  implicit val mrEncoder: Encoder[CrossrefDT] = Encoders.kryo[CrossrefDT]

  def extractDump(input:String):List[String] = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json: json4s.JValue = parse(input)

    val a = (json \ "items").extract[JArray]
    a.arr.map(s => compact(render(s)))
  }

  def crossrefElement(meta: String): CrossrefDT = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json: json4s.JValue = parse(meta)
    val doi:String = (json \ "DOI").extract[String]
    val timestamp: Long = (json \ "indexed" \ "timestamp").extract[Long]
    new CrossrefDT(doi, meta, timestamp)

  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf
    val parser = new ArgumentApplicationParser(Source.fromInputStream(getClass.getResourceAsStream("/eu/dnetlib/dhp/doiboost/crossref_dump_reader/generate_dataset_params.json")).mkString)
    parser.parseArgument(args)
    val master = parser.get("master")
    val sourcePath = parser.get("sourcePath")
    val targetPath = parser.get("targetPath")

    val spark: SparkSession = SparkSession.builder().config(conf)
      .appName(GenerateCrossrefDatasetSpark.getClass.getSimpleName)
      .master(master)
      .getOrCreate()

    import spark.implicits._
    val sc: SparkContext = spark.sparkContext


    sc.wholeTextFiles(sourcePath,2000).flatMap(d =>extractDump(d._2))
      .map(meta => crossrefElement(meta))
      .toDS()
      .write.mode(SaveMode.Overwrite).save(targetPath)

  }
}
