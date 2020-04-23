package eu.dnetlib.doiboost.crossref

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.schema.oaf.Publication
import org.apache.commons.io.IOUtils
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Encoders, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}


case class Reference(author: String, firstPage: String) {}

object SparkMapDumpIntoOAF {

  def main(args: Array[String]): Unit = {


    val logger: Logger = LoggerFactory.getLogger(SparkMapDumpIntoOAF.getClass)
    val conf: SparkConf = new SparkConf()
    val parser = new ArgumentApplicationParser(IOUtils.toString(SparkMapDumpIntoOAF.getClass.getResourceAsStream("/eu/dnetlib/dhp/doiboost/convert_map_to_oaf_params.json")))
    parser.parseArgument(args)
    val spark: SparkSession =
      SparkSession
        .builder()
        .config(conf)
        .appName(SparkMapDumpIntoOAF.getClass.getSimpleName)
        .master(parser.get("master")).getOrCreate()
    import spark.implicits._
    implicit val mapEncoder = Encoders.bean(classOf[Publication])

    val sc = spark.sparkContext

    val total = sc.sequenceFile(parser.get("sourcePath"), classOf[IntWritable], classOf[Text])
      .map(k => k._2.toString).map(CrossrefImporter.decompressBlob)
      .map(k => Crossref2Oaf.convert(k, logger))
      .filter(k => k != null && k.isInstanceOf[Publication])
      .map(k => k.asInstanceOf[Publication])


    val ds: Dataset[Publication] = spark.createDataset(total)
    val targetPath = parser.get("targetPath")
    ds.write.mode(SaveMode.Overwrite).save(s"${targetPath}/publication")


    logger.info(s"total Item :${total}")

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
