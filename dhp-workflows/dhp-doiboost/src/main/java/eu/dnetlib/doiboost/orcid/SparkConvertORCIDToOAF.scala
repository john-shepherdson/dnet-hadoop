package eu.dnetlib.doiboost.orcid

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.schema.oaf.Publication
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

object SparkConvertORCIDToOAF {



  def main(args: Array[String]): Unit = {

    val logger: Logger = LoggerFactory.getLogger(SparkConvertORCIDToOAF.getClass)
    val conf: SparkConf = new SparkConf()
    val parser = new ArgumentApplicationParser(IOUtils.toString(SparkConvertORCIDToOAF.getClass.getResourceAsStream("/eu/dnetlib/dhp/doiboost/convert_map_to_oaf_params.json")))
    parser.parseArgument(args)
    val spark: SparkSession =
      SparkSession
        .builder()
        .config(conf)
        .appName(getClass.getSimpleName)
        .master(parser.get("master")).getOrCreate()

    implicit val mapEncoderPubs: Encoder[Publication] = Encoders.bean(classOf[Publication])

    val sourcePath = parser.get("sourcePath")
    val targetPath = parser.get("targetPath")
    val inputRDD:RDD[String] = spark.sparkContext.textFile(s"$sourcePath")

    println(s"SourcePath is $sourcePath, targetPath is $targetPath master is ${parser.get("master")} ")

    logger.info("Converting ORCID to OAF")
    val d:Dataset[Publication] = spark.createDataset(inputRDD.map(ORCIDToOAF.convertTOOAF).filter(p=>p!=null)).as[Publication]
    d.write.mode(SaveMode.Overwrite).save(targetPath)
  }

}
