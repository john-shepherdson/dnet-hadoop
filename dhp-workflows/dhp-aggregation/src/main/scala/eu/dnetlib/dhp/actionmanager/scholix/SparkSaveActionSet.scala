package eu.dnetlib.dhp.actionmanager.scholix

import com.fasterxml.jackson.databind.ObjectMapper
import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.schema.action.AtomicAction
import eu.dnetlib.dhp.schema.oaf.{Dataset => OafDataset, Oaf, Publication, Software, OtherResearchProduct, Relation}
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.hadoop.mapred.SequenceFileOutputFormat
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.io.Source

object SparkSaveActionSet {


  def toActionSet(item: Oaf): (String, String) = {
    val mapper = new ObjectMapper()

    item match {
      case dataset: OafDataset =>
        val a: AtomicAction[OafDataset] = new AtomicAction[OafDataset]
        a.setClazz(classOf[OafDataset])
        a.setPayload(dataset)
        (dataset.getClass.getCanonicalName, mapper.writeValueAsString(a))
      case publication: Publication =>
        val a: AtomicAction[Publication] = new AtomicAction[Publication]
        a.setClazz(classOf[Publication])
        a.setPayload(publication)
        (publication.getClass.getCanonicalName, mapper.writeValueAsString(a))
      case software: Software =>
        val a: AtomicAction[Software] = new AtomicAction[Software]
        a.setClazz(classOf[Software])
        a.setPayload(software)
        (software.getClass.getCanonicalName, mapper.writeValueAsString(a))
      case orp: OtherResearchProduct =>
        val a: AtomicAction[OtherResearchProduct] = new AtomicAction[OtherResearchProduct]
        a.setClazz(classOf[OtherResearchProduct])
        a.setPayload(orp)
        (orp.getClass.getCanonicalName, mapper.writeValueAsString(a))

      case relation: Relation =>
        val a: AtomicAction[Relation] = new AtomicAction[Relation]
        a.setClazz(classOf[Relation])
        a.setPayload(relation)
        (relation.getClass.getCanonicalName, mapper.writeValueAsString(a))
      case _ =>
        null
    }

  }

  def main(args: Array[String]): Unit = {
    val log: Logger = LoggerFactory.getLogger(getClass)
    val conf: SparkConf = new SparkConf()
    val parser = new ArgumentApplicationParser(Source.fromInputStream(getClass.getResourceAsStream("/eu/dnetlib/dhp/sx/actionset/save_actionset.json")).mkString)
    parser.parseArgument(args)


    val spark: SparkSession =
      SparkSession
        .builder()
        .config(conf)
        .appName(getClass.getSimpleName)
        .master(parser.get("master")).getOrCreate()


    val sourcePath = parser.get("sourcePath")
    log.info(s"sourcePath  -> $sourcePath")

    val targetPath = parser.get("targetPath")
    log.info(s"targetPath  -> $targetPath")

    implicit val oafEncoders: Encoder[Oaf] = Encoders.kryo[Oaf]
    implicit val tEncoder: Encoder[(String, String)] = Encoders.tuple(Encoders.STRING, Encoders.STRING)

    spark.read.load(sourcePath).as[Oaf]
      .map(o => toActionSet(o))
      .filter(o => o != null)
      .rdd.map(s => (new Text(s._1), new Text(s._2))).saveAsHadoopFile(s"$targetPath", classOf[Text], classOf[Text], classOf[SequenceFileOutputFormat[Text, Text]], classOf[GzipCodec])

  }

}
