package eu.dnetlib.doiboost.crossref

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import org.apache.commons.io.IOUtils
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}



case class Reference(author:String, firstPage:String) {}

object SparkMapDumpIntoOAF {

  def main(args: Array[String]): Unit = {


    val logger:Logger = LoggerFactory.getLogger(SparkMapDumpIntoOAF.getClass)
    val conf: SparkConf = new SparkConf()
    val parser = new ArgumentApplicationParser(IOUtils.toString(SparkMapDumpIntoOAF.getClass.getResourceAsStream("/eu/dnetlib/dhp/doiboost/convert_map_to_oaf_params.json")))
    parser.parseArgument(args)
    val spark: SparkSession =
      SparkSession
        .builder()
        .config(conf)
        .appName(SparkMapDumpIntoOAF.getClass.getSimpleName)
        .master(parser.get("master")).getOrCreate()

    val sc = spark.sparkContext
    val x: String = sc.sequenceFile(parser.get("sourcePath"), classOf[IntWritable], classOf[Text])
      .map(k => k._2.toString).first()

    val item =CrossrefImporter.decompressBlob(x)


    logger.info(item)

//    lazy val json: json4s.JValue = parse(item)
//
//
//    val references = for {
//      JArray(references) <- json \\ "reference"
//      JObject(reference) <- references
//      JField("first-page", JString(firstPage)) <- reference
//      JField("author", JString(author)) <- reference
//    } yield Reference(author, firstPage)
//
//
//
//
//    logger.info((json \ "created" \ "timestamp").extractOrElse("missing"))
//    logger.info(references.toString())
//
//    logger.info((json \ "type").extractOrElse("missing"))

  }


}
