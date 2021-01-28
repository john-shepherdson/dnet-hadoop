package eu.dnetlib.dhp.actionmanager.datacite

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.schema.oaf.Oaf
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.hadoop.mapred.SequenceFileOutputFormat
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.io.Source

object ExportActionSetJobNode {

  val log: Logger = LoggerFactory.getLogger(ExportActionSetJobNode.getClass)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf
    val parser = new ArgumentApplicationParser(Source.fromInputStream(getClass.getResourceAsStream("/eu/dnetlib/dhp/actionmanager/datacite/exportDataset_parameters.json")).mkString)
    parser.parseArgument(args)
    val master = parser.get("master")
    val sourcePath = parser.get("sourcePath")
    val targetPath = parser.get("targetPath")

    val spark: SparkSession = SparkSession.builder().config(conf)
      .appName(ExportActionSetJobNode.getClass.getSimpleName)
      .master(master)
      .getOrCreate()
    implicit val resEncoder: Encoder[Oaf] = Encoders.kryo[Oaf]
    implicit val tEncoder:Encoder[(String,String)] = Encoders.tuple(Encoders.STRING,Encoders.STRING)

    spark.read.load(sourcePath).as[Oaf]
      .map(o =>DataciteToOAFTransformation.toActionSet(o))
      .filter(o => o!= null)
      .rdd.map(s => (new Text(s._1), new Text(s._2))).saveAsHadoopFile(s"$targetPath", classOf[Text], classOf[Text], classOf[SequenceFileOutputFormat[Text,Text]], classOf[GzipCodec])


  }

}