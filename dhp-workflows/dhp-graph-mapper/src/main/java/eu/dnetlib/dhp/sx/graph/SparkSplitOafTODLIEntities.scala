package eu.dnetlib.dhp.sx.graph

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.schema.oaf.{Oaf, Relation}
import eu.dnetlib.dhp.schema.scholexplorer.{DLIDataset, DLIPublication, DLIUnknown}
import eu.dnetlib.dhp.sx.ebi.EBIAggregator
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

object SparkSplitOafTODLIEntities {


  def getKeyRelation(rel:Relation):String = {
    s"${rel.getSource}::${rel.getRelType}::${rel.getTarget}"


  }


  def extract_dataset(spark:SparkSession, workingPath:String) :Unit = {

    implicit val oafEncoder: Encoder[Oaf] = Encoders.kryo[Oaf]
    implicit val datEncoder: Encoder[DLIDataset] = Encoders.kryo[DLIDataset]

    val OAFDataset:Dataset[Oaf] = spark.read.load(s"$workingPath/input/OAFDataset").as[Oaf].repartition(4000)

    val ebi_dataset:Dataset[DLIDataset] = spark.read.load(s"$workingPath/ebi/baseline_dataset_ebi").as[DLIDataset].repartition(1000)


    OAFDataset
      .filter(s => s != null && s.isInstanceOf[DLIDataset])
      .map(s =>s.asInstanceOf[DLIDataset])
      .union(ebi_dataset)
      .map(d => (d.getId, d))(Encoders.tuple(Encoders.STRING, datEncoder))
      .groupByKey(_._1)(Encoders.STRING)
      .agg(EBIAggregator.getDLIDatasetAggregator().toColumn)
      .map(p => p._2)
      .repartition(2000)
      .write.mode(SaveMode.Overwrite).save(s"$workingPath/graph/dataset")

  }

  def extract_publication(spark:SparkSession, workingPath:String) :Unit = {

    implicit val oafEncoder: Encoder[Oaf] = Encoders.kryo[Oaf]
    implicit val pubEncoder: Encoder[DLIPublication] = Encoders.kryo[DLIPublication]

    val OAFDataset:Dataset[Oaf] = spark.read.load(s"$workingPath/input/OAFDataset").as[Oaf]

    val ebi_publication:Dataset[DLIPublication] = spark.read.load(s"$workingPath/ebi/baseline_publication_ebi").as[DLIPublication].repartition(1000)


    OAFDataset
      .filter(s => s != null && s.isInstanceOf[DLIPublication])
      .map(s =>s.asInstanceOf[DLIPublication])
      .union(ebi_publication)
      .map(d => (d.getId, d))(Encoders.tuple(Encoders.STRING, pubEncoder))
      .groupByKey(_._1)(Encoders.STRING)
      .agg(EBIAggregator.getDLIPublicationAggregator().toColumn)
      .map(p => p._2)
      .repartition(2000)
      .write.mode(SaveMode.Overwrite).save(s"$workingPath/graph/publication")

  }

  def extract_unknown(spark:SparkSession, workingPath:String) :Unit = {

    implicit val oafEncoder: Encoder[Oaf] = Encoders.kryo[Oaf]
    implicit val unkEncoder: Encoder[DLIUnknown] = Encoders.kryo[DLIUnknown]

    val OAFDataset:Dataset[Oaf] = spark.read.load(s"$workingPath/input/OAFDataset").as[Oaf]

    OAFDataset
      .filter(s => s != null && s.isInstanceOf[DLIUnknown])
      .map(s =>s.asInstanceOf[DLIUnknown])
      .map(d => (d.getId, d))(Encoders.tuple(Encoders.STRING, unkEncoder))
      .groupByKey(_._1)(Encoders.STRING)
      .agg(EBIAggregator.getDLIUnknownAggregator().toColumn)
      .map(p => p._2)
      .write.mode(SaveMode.Overwrite).save(s"$workingPath/graph/unknown")

  }


  def extract_relations(spark:SparkSession, workingPath:String) :Unit = {

    implicit val oafEncoder: Encoder[Oaf] = Encoders.kryo[Oaf]
    implicit val relEncoder: Encoder[Relation] = Encoders.kryo[Relation]

    val OAFDataset:Dataset[Oaf] = spark.read.load(s"$workingPath/input/OAFDataset").as[Oaf]
    val ebi_relation:Dataset[Relation] = spark.read.load(s"$workingPath/ebi/baseline_relation_ebi").as[Relation].repartition(2000)

    OAFDataset
      .filter(s => s != null && s.isInstanceOf[Relation])
      .map(s =>s.asInstanceOf[Relation])
      .union(ebi_relation)
      .map(d => (getKeyRelation(d), d))(Encoders.tuple(Encoders.STRING, relEncoder))
      .groupByKey(_._1)(Encoders.STRING)
      .agg(EBIAggregator.getRelationAggregator().toColumn)
      .map(p => p._2)
      .repartition(4000)
      .write.mode(SaveMode.Overwrite).save(s"$workingPath/graph/relation")

  }


  def main(args: Array[String]): Unit = {
    val parser = new ArgumentApplicationParser(IOUtils.toString(SparkSplitOafTODLIEntities.getClass.getResourceAsStream("/eu/dnetlib/dhp/sx/graph/argumentparser/input_extract_entities_parameters.json")))
    val logger = LoggerFactory.getLogger(SparkSplitOafTODLIEntities.getClass)
    parser.parseArgument(args)

    val workingPath: String = parser.get("workingPath")
    val entity:String = parser.get("entity")
    logger.info(s"Working dir path = $workingPath")

    val spark:SparkSession  = SparkSession
      .builder()
      .appName(SparkSplitOafTODLIEntities.getClass.getSimpleName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .master(parser.get("master"))
      .getOrCreate()


    entity match {
      case "publication" => extract_publication(spark, workingPath)
      case "dataset" => extract_dataset(spark,workingPath)
      case "relation" => extract_relations(spark, workingPath)
      case "unknown" => extract_unknown(spark, workingPath)
    }





  }

}
