package eu.dnetlib.dhp.sx.graph

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.schema.oaf.Oaf
import eu.dnetlib.dhp.schema.scholexplorer.{DLIDataset, DLIPublication, DLIRelation, DLIUnknown}
import eu.dnetlib.dhp.sx.ebi.EBIAggregator
import eu.dnetlib.dhp.sx.ebi.model.{PMArticle, PMAuthor, PMJournal}
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

object SparkSplitOafTODLIEntities {


  def getKeyRelation(rel:DLIRelation):String = {
    s"${rel.getSource}::${rel.getRelType}::${rel.getTarget}"


  }

  def main(args: Array[String]): Unit = {
    val parser = new ArgumentApplicationParser(IOUtils.toString(SparkSplitOafTODLIEntities.getClass.getResourceAsStream("/eu/dnetlib/dhp/sx/graph/argumentparser/input_extract_entities_parameters.json")))
    val logger = LoggerFactory.getLogger(SparkSplitOafTODLIEntities.getClass)
    parser.parseArgument(args)

    val workingPath: String = parser.get("workingPath")
    logger.info(s"Working dir path = $workingPath")

    implicit val oafEncoder: Encoder[Oaf] = Encoders.kryo[Oaf]
    implicit val pubEncoder: Encoder[DLIPublication] = Encoders.kryo[DLIPublication]
    implicit val datEncoder: Encoder[DLIDataset] = Encoders.kryo[DLIDataset]
    implicit val unkEncoder: Encoder[DLIUnknown] = Encoders.kryo[DLIUnknown]
    implicit val relEncoder: Encoder[DLIRelation] = Encoders.kryo[DLIRelation]



    val spark:SparkSession  = SparkSession
      .builder()
      .appName(SparkSplitOafTODLIEntities.getClass.getSimpleName)
      .master(parser.get("master"))
      .getOrCreate()




    val OAFDataset:Dataset[Oaf] = spark.read.load(s"$workingPath/input/OAFDataset").as[Oaf]

    val ebi_dataset:Dataset[DLIDataset] = spark.read.load(s"$workingPath/ebi/baseline_dataset_ebi").as[DLIDataset]
    val ebi_publication:Dataset[DLIPublication] = spark.read.load(s"$workingPath/ebi/baseline_publication_ebi").as[DLIPublication]
    val ebi_relation:Dataset[DLIRelation] = spark.read.load(s"$workingPath/ebi/baseline_relation_ebi").as[DLIRelation]



    OAFDataset
      .filter(s => s != null && s.isInstanceOf[DLIPublication])
      .map(s =>s.asInstanceOf[DLIPublication])
      .union(ebi_publication)
      .map(d => (d.getId, d))(Encoders.tuple(Encoders.STRING, pubEncoder))
      .groupByKey(_._1)(Encoders.STRING)
      .agg(EBIAggregator.getDLIPublicationAggregator().toColumn)
      .map(p => p._2)
      .repartition(1000)
      .write.mode(SaveMode.Overwrite).save(s"$workingPath/graph/publication")

    OAFDataset
      .filter(s => s != null && s.isInstanceOf[DLIDataset])
      .map(s =>s.asInstanceOf[DLIDataset])
      .union(ebi_dataset)
      .map(d => (d.getId, d))(Encoders.tuple(Encoders.STRING, datEncoder))
      .groupByKey(_._1)(Encoders.STRING)
      .agg(EBIAggregator.getDLIDatasetAggregator().toColumn)
      .map(p => p._2)
      .repartition(1000)
      .write.mode(SaveMode.Overwrite).save(s"$workingPath/graph/dataset")


    OAFDataset
      .filter(s => s != null && s.isInstanceOf[DLIUnknown])
      .map(s =>s.asInstanceOf[DLIUnknown])
      .map(d => (d.getId, d))(Encoders.tuple(Encoders.STRING, unkEncoder))
      .groupByKey(_._1)(Encoders.STRING)
      .agg(EBIAggregator.getDLIUnknownAggregator().toColumn)
      .map(p => p._2)
      .repartition(1000)
      .write.mode(SaveMode.Overwrite).save(s"$workingPath/graph/unknown")


    OAFDataset
      .filter(s => s != null && s.isInstanceOf[DLIRelation])
      .map(s =>s.asInstanceOf[DLIRelation])
      .union(ebi_relation)
      .map(d => (getKeyRelation(d), d))(Encoders.tuple(Encoders.STRING, relEncoder))
      .groupByKey(_._1)(Encoders.STRING)
      .agg(EBIAggregator.getDLIRelationAggregator().toColumn)
      .map(p => p._2)
      .repartition(1000)
      .write.mode(SaveMode.Overwrite).save(s"$workingPath/graph/relation")






  }

}
