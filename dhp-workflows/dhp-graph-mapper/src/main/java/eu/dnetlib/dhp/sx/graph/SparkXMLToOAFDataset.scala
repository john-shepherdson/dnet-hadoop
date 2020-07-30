package eu.dnetlib.dhp.sx.graph

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.schema.oaf.Oaf
import eu.dnetlib.dhp.schema.scholexplorer.{DLIDataset, DLIPublication, DLIRelation}
import eu.dnetlib.dhp.sx.graph.parser.{DatasetScholexplorerParser, PublicationScholexplorerParser}
import eu.dnetlib.scholexplorer.relation.RelationMapper
import org.apache.commons.io.IOUtils
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Encoder, Encoders, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._


/**
 * This new version of the  Job read a sequential File containing XML stored in the aggregator and generates a Dataset OAF of heterogeneous
 * entities like Dataset, Relation, Publication and Unknown
 */

object SparkXMLToOAFDataset {


  def main(args: Array[String]): Unit = {
    val logger = LoggerFactory.getLogger(SparkXMLToOAFDataset.getClass)
    val conf = new SparkConf()
    val parser = new ArgumentApplicationParser(IOUtils.toString(SparkXMLToOAFDataset.getClass.getResourceAsStream("/eu/dnetlib/dhp/sx/graph/argumentparser/input_graph_scholix_parameters.json")))
    parser.parseArgument(args)
    val spark =
      SparkSession
        .builder()
        .config(conf)
        .appName(SparkXMLToOAFDataset.getClass.getSimpleName)
        .master(parser.get("master")).getOrCreate()

    val sc = spark.sparkContext

    implicit  val oafEncoder:Encoder[Oaf] = Encoders.kryo[Oaf]
    implicit  val datasetEncoder:Encoder[DLIDataset] = Encoders.kryo[DLIDataset]
    implicit  val publicationEncoder:Encoder[DLIPublication] = Encoders.kryo[DLIPublication]
    implicit  val relationEncoder:Encoder[DLIRelation] = Encoders.kryo[DLIRelation]

    val relationMapper = RelationMapper.load

    val inputPath: String = parser.get("sourcePath")
    val entity: String = parser.get("entity")
    val targetPath = parser.get("targetPath")

    logger.info(s"Input path is $inputPath")
    logger.info(s"Entity path is $entity")
    logger.info(s"Target Path is $targetPath")

    val scholixRdd:RDD[Oaf] = sc.sequenceFile(inputPath, classOf[IntWritable], classOf[Text])
      .map(s => s._2.toString)
      .flatMap(s => {
        entity match {
          case "publication" =>
            val p = new PublicationScholexplorerParser
            val l =p.parseObject(s, relationMapper)
            if (l != null) l.asScala  else List()
          case "dataset" =>
            val d = new DatasetScholexplorerParser
            val l =d.parseObject(s, relationMapper)
            if (l != null) l.asScala  else List()
        }
    }).filter(s => s!= null)
    spark.createDataset(scholixRdd).write.mode(SaveMode.Append).save(targetPath)

  }

}
