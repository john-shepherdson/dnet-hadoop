package eu.dnetlib.doiboost.crossref

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.schema.oaf
import eu.dnetlib.dhp.schema.oaf.{Oaf, Publication, Relation, Dataset => OafDataset}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
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

    implicit val oafEncoder: Encoder[Oaf] = Encoders.kryo[Oaf]
    implicit val mapEncoderPubs: Encoder[Publication] = Encoders.kryo[Publication]
    implicit val mapEncoderRelatons: Encoder[Relation] = Encoders.kryo[Relation]
    implicit val mapEncoderDatasets: Encoder[oaf.Dataset] = Encoders.kryo[OafDataset]

    val sc = spark.sparkContext
    val targetPath = parser.get("targetPath")
    import spark.implicits._


    spark.read.load(parser.get("sourcePath")).as[CrossrefDT]
      .flatMap(k => Crossref2Oaf.convert(k.json))
      .filter(o => o != null)
      .write.mode(SaveMode.Overwrite).save(s"$targetPath/mixObject")


    val ds:Dataset[Oaf] = spark.read.load(s"$targetPath/mixObject").as[Oaf]

    ds.filter(o => o.isInstanceOf[Publication]).map(o => o.asInstanceOf[Publication]).write.save(s"$targetPath/publication")

    ds.filter(o => o.isInstanceOf[Relation]).map(o => o.asInstanceOf[Relation]).write.save(s"$targetPath/relation")

    ds.filter(o => o.isInstanceOf[OafDataset]).map(o => o.asInstanceOf[OafDataset]).write.save(s"$targetPath/dataset")



//
//
//
//    sc.sequenceFile(parser.get("sourcePath"), classOf[IntWritable], classOf[Text])
//      .map(k => k._2.toString).map(CrossrefImporter.decompressBlob)
//      .flatMap(k => Crossref2Oaf.convert(k)).saveAsObjectFile(s"${targetPath}/mixObject")
//
//    val inputRDD = sc.objectFile[Oaf](s"${targetPath}/mixObject").filter(p=> p!= null)
//
//    val distinctPubs:RDD[Publication] = inputRDD.filter(k => k != null && k.isInstanceOf[Publication])
//      .map(k => k.asInstanceOf[Publication]).map { p: Publication => Tuple2(p.getId, p) }.reduceByKey { case (p1: Publication, p2: Publication) =>
//      var r = if (p1 == null) p2 else p1
//      if (p1 != null && p2 != null) {
//        if (p1.getLastupdatetimestamp != null && p2.getLastupdatetimestamp != null) {
//          if (p1.getLastupdatetimestamp < p2.getLastupdatetimestamp)
//            r = p2
//          else
//            r = p1
//        } else {
//          r = if (p1.getLastupdatetimestamp == null) p2 else p1
//        }
//      }
//      r
//    }.map(_._2)
//
//    val pubs:Dataset[Publication] = spark.createDataset(distinctPubs)
//    pubs.write.mode(SaveMode.Overwrite).save(s"${targetPath}/publication")
//
//
//    val distincDatasets:RDD[OafDataset] = inputRDD.filter(k => k != null && k.isInstanceOf[OafDataset])
//      .map(k => k.asInstanceOf[OafDataset]).map(p => Tuple2(p.getId, p)).reduceByKey { case (p1: OafDataset, p2: OafDataset) =>
//      var r = if (p1 == null) p2 else p1
//      if (p1 != null && p2 != null) {
//        if (p1.getLastupdatetimestamp != null && p2.getLastupdatetimestamp != null) {
//          if (p1.getLastupdatetimestamp < p2.getLastupdatetimestamp)
//            r = p2
//          else
//            r = p1
//        } else {
//          r = if (p1.getLastupdatetimestamp == null) p2 else p1
//        }
//      }
//      r
//    }.map(_._2)
//
//    spark.createDataset(distincDatasets).write.mode(SaveMode.Overwrite).save(s"${targetPath}/dataset")
//
//
//
//    val distinctRels =inputRDD.filter(k => k != null && k.isInstanceOf[Relation])
//      .map(k => k.asInstanceOf[Relation]).map(r=> (s"${r.getSource}::${r.getTarget}",r))
//      .reduceByKey { case (p1: Relation, p2: Relation) =>
//        if (p1 == null) p2 else p1
//      }.map(_._2)
//
//    val rels: Dataset[Relation] = spark.createDataset(distinctRels)
//
//    rels.write.mode(SaveMode.Overwrite).save(s"${targetPath}/relations")
  }


}
