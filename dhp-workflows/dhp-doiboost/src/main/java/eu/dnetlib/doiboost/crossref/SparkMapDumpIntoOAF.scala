package eu.dnetlib.doiboost.crossref

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.schema.oaf
import eu.dnetlib.dhp.schema.oaf.{Oaf, Publication, Relation, Result}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SaveMode, SparkSession}
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

    implicit val mapEncoderPubs: Encoder[Publication] = Encoders.kryo(classOf[Publication])
    implicit val mapEncoderRelatons: Encoder[Relation] = Encoders.kryo(classOf[Relation])
    implicit val mapEncoderDatasets: Encoder[oaf.Dataset] = Encoders.kryo(classOf[eu.dnetlib.dhp.schema.oaf.Dataset])

    val sc = spark.sparkContext
    val targetPath = parser.get("targetPath")


    sc.sequenceFile(parser.get("sourcePath"), classOf[IntWritable], classOf[Text])
      .map(k => k._2.toString).map(CrossrefImporter.decompressBlob)
      .flatMap(k => Crossref2Oaf.convert(k)).saveAsObjectFile(s"${targetPath}/mixObject")


    val inputRDD = sc.objectFile[Oaf](s"${targetPath}/mixObject").filter(p=> p!= null)
    val total = inputRDD.count()

    val totalPub = inputRDD.filter(p => p.isInstanceOf[Publication]).count()
    val totalDat = inputRDD.filter(p => p.isInstanceOf[eu.dnetlib.dhp.schema.oaf.Dataset]).count()
    val totalRel = inputRDD.filter(p => p.isInstanceOf[eu.dnetlib.dhp.schema.oaf.Relation]).count()


    logger.info(s"Created     $total")
    logger.info(s"totalPub    $totalPub")
    logger.info(s"totalDat    $totalDat")
    logger.info(s"totalRel    $totalRel")
    val pubs: Dataset[Publication] = spark.createDataset(inputRDD.filter(k => k != null && k.isInstanceOf[Publication])
      .map(k => k.asInstanceOf[Publication]))
    pubs.write.mode(SaveMode.Overwrite).save(s"${targetPath}/publication")

    val ds: Dataset[eu.dnetlib.dhp.schema.oaf.Dataset] = spark.createDataset(inputRDD.filter(k => k != null && k.isInstanceOf[eu.dnetlib.dhp.schema.oaf.Dataset])
      .map(k => k.asInstanceOf[eu.dnetlib.dhp.schema.oaf.Dataset]))
    ds.write.mode(SaveMode.Overwrite).save(s"${targetPath}/dataset")

    val rels: Dataset[Relation] = spark.createDataset(inputRDD.filter(k => k != null && k.isInstanceOf[Relation])
      .map(k => k.asInstanceOf[Relation]))
    rels.write.mode(SaveMode.Overwrite).save(s"${targetPath}/relations")




  }


}
