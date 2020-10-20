package eu.dnetlib.doiboost.crossref

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SaveMode, SparkSession}
import org.json4s
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import org.slf4j.{Logger, LoggerFactory}

object CrossrefDataset {


  def extractTimestamp(input:String): Long = {

    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json: json4s.JValue = parse(input)

    (json\"indexed"\"timestamp").extractOrElse[Long](0)

  }


  def main(args: Array[String]): Unit = {


    val logger: Logger = LoggerFactory.getLogger(SparkMapDumpIntoOAF.getClass)
    val conf: SparkConf = new SparkConf()
    val parser = new ArgumentApplicationParser(IOUtils.toString(CrossrefDataset.getClass.getResourceAsStream("/eu/dnetlib/dhp/doiboost/crossref_to_dataset_params.json")))
    parser.parseArgument(args)
    val spark: SparkSession =
      SparkSession
        .builder()
        .config(conf)
        .appName(SparkMapDumpIntoOAF.getClass.getSimpleName)
        .master(parser.get("master")).getOrCreate()
    import spark.implicits._


    val crossrefAggregator = new Aggregator[CrossrefDT, CrossrefDT, CrossrefDT] with Serializable {

      override def zero: CrossrefDT = null

      override def reduce(b: CrossrefDT, a: CrossrefDT): CrossrefDT = {
        if (b == null)
          return a
        if (a == null)
          return b

        val tb = extractTimestamp(b.json)
        val ta = extractTimestamp(a.json)
        if(ta >tb) {
          return a
        }
        b
      }

      override def merge(a: CrossrefDT, b: CrossrefDT): CrossrefDT = {
        if (b == null)
          return a
        if (a == null)
          return b

        val tb = extractTimestamp(b.json)
        val ta = extractTimestamp(a.json)
        if(ta >tb) {
          return a
        }
        b
      }

      override def bufferEncoder: Encoder[CrossrefDT] = implicitly[Encoder[CrossrefDT]]

      override def outputEncoder: Encoder[CrossrefDT] = implicitly[Encoder[CrossrefDT]]

      override def finish(reduction: CrossrefDT): CrossrefDT = reduction
    }

    val sourcePath:String = parser.get("sourcePath")
    val targetPath:String = parser.get("targetPath")

    val ds:Dataset[CrossrefDT] = spark.read.load(sourcePath).as[CrossrefDT]

    ds.groupByKey(_.doi)
      .agg(crossrefAggregator.toColumn)
      .map(s=>s._2)
      .write.mode(SaveMode.Overwrite).save(targetPath)

  }

}
