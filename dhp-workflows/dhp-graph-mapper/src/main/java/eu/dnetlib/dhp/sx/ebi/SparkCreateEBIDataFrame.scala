package eu.dnetlib.dhp.sx.ebi

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.schema.oaf.{Oaf, Publication, Relation, Dataset => OafDataset}
import eu.dnetlib.dhp.sx.graph.parser.{DatasetScholexplorerParser, PublicationScholexplorerParser}
import eu.dnetlib.scholexplorer.relation.RelationMapper
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SaveMode, SparkSession}
import org.codehaus.jackson.map.{ObjectMapper, SerializationConfig}
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.JavaConverters._

object SparkCreateEBIDataFrame {


  def main(args: Array[String]): Unit = {
    val logger: Logger = LoggerFactory.getLogger(SparkCreateEBIDataFrame.getClass)
    val conf: SparkConf = new SparkConf()
    val parser = new ArgumentApplicationParser(IOUtils.toString(SparkCreateEBIDataFrame.getClass.getResourceAsStream("/eu/dnetlib/dhp/sx/ebi/ebi_to_df_params.json")))
    parser.parseArgument(args)
    val spark: SparkSession =
      SparkSession
        .builder()
        .config(conf)
        .appName(SparkCreateEBIDataFrame.getClass.getSimpleName)
        .master(parser.get("master")).getOrCreate()

    val sc = spark.sparkContext


    val workingPath = parser.get("workingPath")
    val relationMapper = RelationMapper.load

    implicit val oafEncoder: Encoder[Oaf] = Encoders.kryo(classOf[Oaf])
    implicit val datasetEncoder: Encoder[OafDataset] = Encoders.kryo(classOf[OafDataset])
    implicit val pubEncoder: Encoder[Publication] = Encoders.kryo(classOf[Publication])
    implicit val relEncoder: Encoder[Relation] = Encoders.kryo(classOf[Relation])

    logger.info("Extract Publication and relation from publication_xml")
    val oafPubsRDD:RDD[Oaf] = sc.textFile(s"$workingPath/publication_xml").map(s =>
    {
      new ObjectMapper().readValue(s, classOf[String])
    }).flatMap(s => {
      val d = new PublicationScholexplorerParser
      d.parseObject(s, relationMapper).asScala.iterator})

    val mapper = new ObjectMapper()
    mapper.getSerializationConfig.enable(SerializationConfig.Feature.INDENT_OUTPUT)
    spark.createDataset(oafPubsRDD).write.mode(SaveMode.Overwrite).save(s"$workingPath/oaf")

    logger.info("Extract Publication and relation from dataset_xml")
    val oafDatsRDD:RDD[Oaf] = sc.textFile(s"$workingPath/_dataset_xml").map(s =>
    {
      new ObjectMapper().readValue(s, classOf[String])
    }).flatMap(s => {
      val d = new DatasetScholexplorerParser
      d.parseObject(s, relationMapper).asScala.iterator})

    spark.createDataset(oafDatsRDD).write.mode(SaveMode.Append).save(s"$workingPath/oaf")
    val dataset: Dataset[OafDataset] = spark.read.load(s"$workingPath/oaf").as[Oaf].filter(o => o.isInstanceOf[OafDataset]).map(d => d.asInstanceOf[OafDataset])
    val publication: Dataset[Publication] = spark.read.load(s"$workingPath/oaf").as[Oaf].filter(o => o.isInstanceOf[Publication]).map(d => d.asInstanceOf[Publication])
    val relations: Dataset[Relation] = spark.read.load(s"$workingPath/oaf").as[Oaf].filter(o => o.isInstanceOf[Relation]).map(d => d.asInstanceOf[Relation])
    publication.map(d => (d.getId, d))(Encoders.tuple(Encoders.STRING, pubEncoder))
      .groupByKey(_._1)(Encoders.STRING)
      .agg(EBIAggregator.getPublicationAggregator().toColumn)
      .map(p => p._2)
      .write.mode(SaveMode.Overwrite).save(s"$workingPath/publication")

    dataset.map(d => (d.getId, d))(Encoders.tuple(Encoders.STRING, datasetEncoder))
      .groupByKey(_._1)(Encoders.STRING)
      .agg(EBIAggregator.getDatasetAggregator().toColumn)
      .map(p => p._2)
      .write.mode(SaveMode.Overwrite).save(s"$workingPath/dataset")

    relations.map(d => (s"${d.getSource}::${d.getRelType}::${d.getTarget}", d))(Encoders.tuple(Encoders.STRING, relEncoder))
      .groupByKey(_._1)(Encoders.STRING)
      .agg(EBIAggregator.getRelationAggregator().toColumn)
      .map(p => p._2)
      .write.mode(SaveMode.Overwrite).save(s"$workingPath/relation")
  }
}
