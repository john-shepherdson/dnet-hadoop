package eu.dnetlib.doiboost.orcid

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.schema.oaf.Publication
import eu.dnetlib.doiboost.mag.ConversionUtil
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

    implicit val mapEncoderPubs: Encoder[Publication] = Encoders.kryo[Publication]
    implicit val tupleForJoinEncoder: Encoder[(String, Publication)] = Encoders.tuple(Encoders.STRING, mapEncoderPubs)
    import spark.implicits._
    val sourcePath = parser.get("sourcePath")
    val targetPath = parser.get("targetPath")
    val dataset:Dataset[ORCIDElement] = spark.read.json(sourcePath).as[ORCIDElement]


    logger.info("Converting ORCID to OAF")
    val d:RDD[Publication] = dataset.map(o => ORCIDToOAF.convertTOOAF(o)).filter(p=>p!=null).map(p=>(p.getId,p)).rdd.reduceByKey(ConversionUtil.mergePublication)
      .map(_._2)

    spark.createDataset(d).as[Publication].write.mode(SaveMode.Overwrite).save(targetPath)
  }

}
