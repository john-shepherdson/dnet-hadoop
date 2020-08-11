package eu.dnetlib.dhp.`export`

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.schema.oaf.{Instance, Publication, Relation, Dataset => OafDataset}
import eu.dnetlib.dhp.schema.scholexplorer.{DLIDataset, DLIPublication}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.hadoop.mapred.SequenceFileOutputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
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

    import spark.implicits._


    val relRDD:RDD[Relation] = sc.textFile(s"$workingPath/relation_j")
      .map(s => new ObjectMapper().readValue(s, classOf[Relation]))
      .filter(p => p.getDataInfo.getDeletedbyinference == false)
    spark.createDataset(relRDD).write.mode(SaveMode.Overwrite).save(s"$workingPath/relationDS")

    val datRDD:RDD[OafDataset] = sc.textFile(s"$workingPath/dataset")
      .map(s => new ObjectMapper().readValue(s, classOf[DLIDataset]))
      .filter(p => p.getDataInfo.getDeletedbyinference == false)
      .map(DLIToOAF.convertDLIDatasetTOOAF).filter(p=>p!= null)
    spark.createDataset(datRDD).write.mode(SaveMode.Overwrite).save(s"$workingPath/datasetDS")


    val pubRDD:RDD[Publication] = sc.textFile(s"$workingPath/publication")
      .map(s => new ObjectMapper().readValue(s, classOf[DLIPublication]))
      .filter(p => p.getDataInfo.getDeletedbyinference == false)
      .map(DLIToOAF.convertDLIPublicationToOAF).filter(p=>p!= null)
    spark.createDataset(pubRDD).write.mode(SaveMode.Overwrite).save(s"$workingPath/publicationDS")



    val pubs:Dataset[Publication] = spark.read.load(s"$workingPath/publicationDS").as[Publication]
    val dats :Dataset[OafDataset] = spark.read.load(s"$workingPath/datasetDS").as[OafDataset]
    val relDS1 :Dataset[Relation] = spark.read.load(s"$workingPath/relationDS").as[Relation]


    val pub_id = pubs.select("id").distinct()
    val dat_id = dats.select("id").distinct()


    pub_id.joinWith(relDS1, pub_id("id").equalTo(relDS1("source"))).map(k => k._2).write.mode(SaveMode.Overwrite).save(s"$workingPath/relationDS_f1")

    val relDS2= spark.read.load(s"$workingPath/relationDS_f1").as[Relation]

    relDS2.joinWith(dat_id, relDS2("target").equalTo(dats("id"))).map(k => k._1).write.mode(SaveMode.Overwrite).save(s"$workingPath/relationDS_filtered")


    val r_source = relDS2.select(relDS2("source")).distinct()
    val r_target = relDS2.select(relDS2("target")).distinct()


    val w2 = Window.partitionBy("id").orderBy("lastupdatetimestamp")

    pubs.joinWith(r_source, pubs("id").equalTo(r_source("source")), "inner").map(k => k._1)
      .withColumn("row",row_number.over(w2)).where($"row" === 1).drop("row")
      .write.mode(SaveMode.Overwrite).save(s"$workingPath/publicationDS_filtered")

    dats.joinWith(r_target, dats("id").equalTo(r_target("target")), "inner").map(k => k._1)
      .withColumn("row",row_number.over(w2)).where($"row" === 1).drop("row")
      .write.mode(SaveMode.Overwrite).save(s"$workingPath/datasetAS")

    spark.createDataset(sc.textFile(s"$workingPath/dataset")
      .map(s => new ObjectMapper().readValue(s, classOf[DLIDataset]))
      .map(DLIToOAF.convertDLIDatasetToExternalReference)
      .filter(p => p != null)).as[DLIExternalReference].write.mode(SaveMode.Overwrite).save(s"$workingPath/externalReference")

    val pf = spark.read.load(s"$workingPath/publicationDS_filtered").select("id")
    val relDS3  = spark.read.load(s"$workingPath/relationDS").as[Relation]
    val relationTo = pf.joinWith(relDS3, pf("id").equalTo(relDS3("source")),"inner").map(t =>t._2)

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

    val pubf :Dataset[Publication] = spark.read.load(s"$workingPath/publicationDS_filtered").as[Publication]

    val groupedERf:Dataset[(String, List[DLIExternalReference])]= spark.read.load(s"$workingPath/externalReference_grouped").as[(String, List[DLIExternalReference])]

    groupedERf.joinWith(pubf,pubf("id").equalTo(groupedERf("_1"))).map(t =>
      {
        val publication = t._2
        if (t._1 != null) {
          val eRefs = t._1._2
          DLIToOAF.insertExternalRefs(publication, eRefs)

        } else
          publication
      }
    ).write.mode(SaveMode.Overwrite).save(s"$workingPath/publicationAS")


    spark.createDataset(sc.textFile(s"$workingPath/dataset")
      .map(s => new ObjectMapper().readValue(s, classOf[DLIDataset]))
      .map(DLIToOAF.convertClinicalTrial)
      .filter(p => p != null))
      .write.mode(SaveMode.Overwrite).save(s"$workingPath/clinicalTrials")

    val ct:Dataset[(String,String)] = spark.read.load(s"$workingPath/clinicalTrials").as[(String,String)]

    val relDS= spark.read.load(s"$workingPath/relationDS_f1").as[Relation]

    relDS.joinWith(ct, relDS("target").equalTo(ct("_1")), "inner")
      .map(k =>{
        val currentRel = k._1
        currentRel.setTarget(k._2._2)
        currentRel
      }).write.mode(SaveMode.Overwrite).save(s"$workingPath/clinicalTrialsRels")


    val clRels:Dataset[Relation] = spark.read.load(s"$workingPath/clinicalTrialsRels").as[Relation]
    val rels:Dataset[Relation] = spark.read.load(s"$workingPath/relationDS_filtered").as[Relation]

    rels.union(clRels).flatMap(r => {
      val inverseRel = new Relation
      inverseRel.setSource(r.getTarget)
      inverseRel.setTarget(r.getSource)
      inverseRel.setDataInfo(r.getDataInfo)
      inverseRel.setCollectedfrom(r.getCollectedfrom)
      inverseRel.setRelType(r.getRelType)
      inverseRel.setSubRelType(r.getSubRelType)
      inverseRel.setRelClass(DLIToOAF.rel_inverse(r.getRelClass))
      List(r, inverseRel)
    }).write.mode(SaveMode.Overwrite).save(s"$workingPath/relationAS")



    spark.read.load(s"$workingPath/publicationAS").as[Publication].map(DLIToOAF.fixInstance).write.mode(SaveMode.Overwrite).save(s"$workingPath/publicationAS_fixed")
    spark.read.load(s"$workingPath/datasetAS").as[OafDataset].map(DLIToOAF.fixInstanceDataset).write.mode(SaveMode.Overwrite).save(s"$workingPath/datasetAS_fixed")

    val fRels:Dataset[(String,String)] = spark.read.load(s"$workingPath/relationAS").as[Relation].map(DLIToOAF.toActionSet)
    val fpubs:Dataset[(String,String)] = spark.read.load(s"$workingPath/publicationAS_fixed").as[Publication].map(DLIToOAF.toActionSet)
    val fdats:Dataset[(String,String)] = spark.read.load(s"$workingPath/datasetAS_fixed").as[OafDataset].map(DLIToOAF.toActionSet)

    fRels.union(fpubs).union(fdats).rdd.map(s => (new Text(s._1), new Text(s._2))).saveAsHadoopFile(s"$workingPath/rawset", classOf[Text], classOf[Text], classOf[SequenceFileOutputFormat[Text,Text]], classOf[GzipCodec])
  }








}
