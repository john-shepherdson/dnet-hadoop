package eu.dnetlib.dhp.`export`

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.schema.oaf.{Publication, Relation, Dataset => OafDataset}
import eu.dnetlib.dhp.schema.scholexplorer.{DLIDataset, DLIPublication, DLIRelation}
import org.apache.commons.io.IOUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.codehaus.jackson.map.ObjectMapper
import scala.collection.mutable.ArrayBuffer


object SparkExportContentForOpenAire {


  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    val parser = new ArgumentApplicationParser(IOUtils.toString(SparkExportContentForOpenAire.getClass.getResourceAsStream("input_export_content_parameters.json")))
    parser.parseArgument(args)
    val spark: SparkSession =
      SparkSession
        .builder()
        .config(conf)
        .appName(SparkExportContentForOpenAire.getClass.getSimpleName)
        .master(parser.get("master")).getOrCreate()


    val sc:SparkContext = spark.sparkContext

    val workingPath = parser.get("workingDirPath")

    implicit  val pubEncoder: Encoder[Publication] = Encoders.bean(classOf[Publication])
    implicit  val datEncoder: Encoder[OafDataset] = Encoders.bean(classOf[OafDataset])
    implicit  val relEncoder: Encoder[Relation] = Encoders.bean(classOf[Relation])
    implicit  val dliRelEncoder: Encoder[DLIRelation] = Encoders.bean(classOf[DLIRelation])
    import spark.implicits._

//
//    val relRDD:RDD[Relation] = sc.textFile(s"$workingPath/relation_j")
//      .map(s => new ObjectMapper().readValue(s, classOf[DLIRelation]))
//      .filter(p => p.getDataInfo.getDeletedbyinference == false)
//      .map(DLIToOAF.convertDLIRelation).filter(p=>p!= null)
//    spark.createDataset(relRDD).write.mode(SaveMode.Overwrite).save(s"$workingPath/relationDS")
//
//    val datRDD:RDD[OafDataset] = sc.textFile(s"$workingPath/dataset")
//      .map(s => new ObjectMapper().readValue(s, classOf[DLIDataset]))
//      .filter(p => p.getDataInfo.getDeletedbyinference == false)
//      .map(DLIToOAF.convertDLIDatasetTOOAF).filter(p=>p!= null)
//    spark.createDataset(datRDD).write.mode(SaveMode.Overwrite).save(s"$workingPath/datasetDS")
//
//
//    val pubRDD:RDD[Publication] = sc.textFile(s"$workingPath/publication")
//      .map(s => new ObjectMapper().readValue(s, classOf[DLIPublication]))
//      .filter(p => p.getDataInfo.getDeletedbyinference == false)
//      .map(DLIToOAF.convertDLIPublicationToOAF).filter(p=>p!= null)
//    spark.createDataset(pubRDD).write.mode(SaveMode.Overwrite).save(s"$workingPath/publicationDS")
//
//
//
//    val pubs:Dataset[Publication] = spark.read.load(s"$workingPath/publicationDS").as[Publication]
//    val dats :Dataset[OafDataset] = spark.read.load(s"$workingPath/datasetDS").as[OafDataset]
    var relDS :Dataset[Relation] = spark.read.load(s"$workingPath/relationDS").as[Relation]
//
//
//    pubs.joinWith(relDS, pubs("id").equalTo(relDS("source"))).map(k => k._2).write.mode(SaveMode.Overwrite).save(s"$workingPath/relationDS_f1")
//
//    relDS= spark.read.load(s"$workingPath/relationDS_f1").as[Relation]
//
//    relDS.joinWith(dats, relDS("target").equalTo(dats("id"))).map(k => k._1).write.mode(SaveMode.Overwrite).save(s"$workingPath/relationDS_filtered")
//
//
//    val r_source = relDS.select(relDS("source")).distinct()
//    val r_target = relDS.select(relDS("source")).distinct()
//
//
//    pubs.joinWith(r_source, pubs("id").equalTo(r_source("source")), "inner").map(k => k._1).write.mode(SaveMode.Overwrite).save(s"$workingPath/publicationDS_filtered")
//
//    dats.joinWith(r_target, dats("id").equalTo(r_target("target")), "inner").map(k => k._1).write.mode(SaveMode.Overwrite).save(s"$workingPath/datasetDS_filtered")
//
//    spark.createDataset(sc.textFile(s"$workingPath/dataset")
//      .map(s => new ObjectMapper().readValue(s, classOf[DLIDataset]))
//      .map(DLIToOAF.convertDLIDatasetToExternalReference)
//      .filter(p => p != null)).as[DLIExternalReference].write.mode(SaveMode.Overwrite).save(s"$workingPath/externalReference")
//

    val pf = spark.read.load(s"$workingPath/publicationDS_filtered").select("id")
    relDS  = spark.read.load(s"$workingPath/relationDS").as[Relation]
    val relationTo = pf.joinWith(relDS, pf("id").equalTo(relDS("source")),"inner").map(t =>t._2)

    val extRef =  spark.read.load(s"$workingPath/externalReference").as[DLIExternalReference]

    spark.createDataset(relationTo.joinWith(extRef, relationTo("target").equalTo(extRef("id")), "inner").map(d => {
        val r = d._1
        val ext = d._2
        (r.getSource, ext)
      }).rdd.groupByKey.map(f =>  {
      var dli_ext = ArrayBuffer[DLIExternalReference]()
      f._2.foreach(d => if (dli_ext.size < 100) dli_ext += d )
      (f._1, dli_ext)
    })).write.mode(SaveMode.Overwrite).save(s"$workingPath/externalReference_grouped")














  }

}
