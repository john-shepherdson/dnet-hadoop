package eu.dnetlib.dhp.sx.graph.pangaea

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Encoder, Encoders, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.io.Source

object SparkGeneratePanagaeaDataset {


  def main(args: Array[String]): Unit = {
    val logger: Logger = LoggerFactory.getLogger(getClass)
    val conf: SparkConf = new SparkConf()
    val parser = new ArgumentApplicationParser(Source.fromInputStream(getClass.getResourceAsStream("/eu/dnetlib/dhp/sx/pangaea/pangaea_to_dataset.json")).mkString)
    parser.parseArgument(args)


    val spark: SparkSession =
      SparkSession
        .builder()
        .config(conf)
        .appName(SparkGeneratePanagaeaDataset.getClass.getSimpleName)
        .master(parser.get("master")).getOrCreate()

    parser.getObjectMap.asScala.foreach(s => logger.info(s"${s._1} -> ${s._2}"))
    logger.info("Converting sequential file into Dataset")
    val sc:SparkContext = spark.sparkContext

    val workingPath:String = parser.get("workingPath")

    implicit val pangaeaEncoders: Encoder[PangaeaDataModel] = Encoders.kryo[PangaeaDataModel]

    val inputRDD:RDD[PangaeaDataModel] =  sc.textFile(s"$workingPath/update").map(s => PangaeaUtils.toDataset(s))

    spark.createDataset(inputRDD).as[PangaeaDataModel]
      .map(s => (s.identifier,s))(Encoders.tuple(Encoders.STRING, pangaeaEncoders))
       .groupByKey(_._1)(Encoders.STRING)
      .agg(PangaeaUtils.getDatasetAggregator().toColumn)
      .map(s => s._2)
      .write.mode(SaveMode.Overwrite).save(s"$workingPath/dataset")

  }





}
