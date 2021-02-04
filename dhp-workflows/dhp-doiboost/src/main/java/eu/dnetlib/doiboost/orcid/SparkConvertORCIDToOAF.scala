package eu.dnetlib.doiboost.orcid

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.oa.merge.AuthorMerger
import eu.dnetlib.dhp.schema.oaf.Publication
import eu.dnetlib.dhp.schema.orcid.OrcidDOI
import eu.dnetlib.doiboost.mag.ConversionUtil
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

object SparkConvertORCIDToOAF {
  val logger: Logger = LoggerFactory.getLogger(SparkConvertORCIDToOAF.getClass)

  def getPublicationAggregator(): Aggregator[(String, Publication), Publication, Publication] = new Aggregator[(String, Publication), Publication, Publication]{

    override def zero: Publication = new Publication()

    override def reduce(b: Publication, a: (String, Publication)): Publication = {
      b.mergeFrom(a._2)
      b.setAuthor(AuthorMerger.mergeAuthor(a._2.getAuthor, b.getAuthor))
      if (b.getId == null)
        b.setId(a._2.getId)
      b
    }


    override def merge(wx: Publication, wy: Publication): Publication = {
      wx.mergeFrom(wy)
      wx.setAuthor(AuthorMerger.mergeAuthor(wy.getAuthor, wx.getAuthor))
      if(wx.getId == null && wy.getId.nonEmpty)
        wx.setId(wy.getId)
      wx
    }
    override def finish(reduction: Publication): Publication = reduction

    override def bufferEncoder: Encoder[Publication] =
      Encoders.kryo(classOf[Publication])

    override def outputEncoder: Encoder[Publication] =
      Encoders.kryo(classOf[Publication])
  }

def run(spark:SparkSession,sourcePath:String, targetPath:String):Unit = {
  implicit val mapEncoderPubs: Encoder[Publication] = Encoders.kryo[Publication]
  implicit val mapOrcid: Encoder[OrcidDOI] = Encoders.kryo[OrcidDOI]
  implicit val tupleForJoinEncoder: Encoder[(String, Publication)] = Encoders.tuple(Encoders.STRING, mapEncoderPubs)

  val mapper = new ObjectMapper()
  mapper.getDeserializationConfig.withFeatures(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)

  val dataset:Dataset[OrcidDOI] = spark.createDataset(spark.sparkContext.textFile(sourcePath).map(s => mapper.readValue(s,classOf[OrcidDOI])))

  logger.info("Converting ORCID to OAF")
  dataset.map(o => ORCIDToOAF.convertTOOAF(o)).filter(p=>p!=null)
    .map(d => (d.getId, d))
    .groupByKey(_._1)(Encoders.STRING)
    .agg(getPublicationAggregator().toColumn)
    .map(p => p._2)
    .write.mode(SaveMode.Overwrite).save(targetPath)
}

  def main(args: Array[String]): Unit = {


    val conf: SparkConf = new SparkConf()
    val parser = new ArgumentApplicationParser(IOUtils.toString(SparkConvertORCIDToOAF.getClass.getResourceAsStream("/eu/dnetlib/dhp/doiboost/convert_map_to_oaf_params.json")))
    parser.parseArgument(args)
    val spark: SparkSession =
      SparkSession
        .builder()
        .config(conf)
        .appName(getClass.getSimpleName)
        .master(parser.get("master")).getOrCreate()



    val sourcePath = parser.get("sourcePath")
    val targetPath = parser.get("targetPath")
    run(spark, sourcePath, targetPath)

  }

}
