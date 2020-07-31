package eu.dnetlib.dedup.sx

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.schema.oaf.{Oaf, OafEntity, Relation}
import eu.dnetlib.dhp.schema.scholexplorer.{DLIDataset, DLIPublication, DLIRelation, DLIUnknown, OafUtils}
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SaveMode, SparkSession}
import org.slf4j.LoggerFactory
import org.apache.spark.sql.functions.col

object SparkUpdateEntityWithDedupInfo {

  def main(args: Array[String]): Unit = {
    val parser = new ArgumentApplicationParser(IOUtils.toString(SparkUpdateEntityWithDedupInfo.getClass.getResourceAsStream("/eu/dnetlib/dhp/sx/graph/argumentparser/input_extract_entities_parameters.json")))
    val logger = LoggerFactory.getLogger(SparkUpdateEntityWithDedupInfo.getClass)
    parser.parseArgument(args)

    val workingPath: String = parser.get("workingPath")
    logger.info(s"Working dir path = $workingPath")

    implicit val oafEncoder: Encoder[OafEntity] = Encoders.kryo[OafEntity]
    implicit val relEncoder: Encoder[Relation] = Encoders.bean(classOf[Relation])

    implicit val pubEncoder: Encoder[DLIPublication] = Encoders.kryo[DLIPublication]
    implicit val datEncoder: Encoder[DLIDataset] = Encoders.kryo[DLIDataset]
    implicit val unkEncoder: Encoder[DLIUnknown] = Encoders.kryo[DLIUnknown]
    implicit val dlirelEncoder: Encoder[DLIRelation] = Encoders.kryo[DLIRelation]


    val spark: SparkSession = SparkSession
      .builder()
      .appName(SparkUpdateEntityWithDedupInfo.getClass.getSimpleName)
      .master(parser.get("master"))
      .getOrCreate()


    val entityPath = parser.get("entityPath")
    val mergeRelPath = parser.get("mergeRelPath")
    val dedupRecordPath = parser.get("dedupRecordPath")
    val entity = parser.get("entity")
    val destination = parser.get("targetPath")

    val mergedIds = spark.read.load(mergeRelPath).as[Relation]
      .where("relClass == 'merges'")
      .select(col("target"))


    val entities: Dataset[(String, OafEntity)] = spark
      .read
      .load(entityPath).as[OafEntity]
      .map(o => (o.getId, o))(Encoders.tuple(Encoders.STRING, oafEncoder))


    val finalDataset:Dataset[OafEntity] = entities.joinWith(mergedIds, entities("_1").equalTo(mergedIds("target")), "left")
      .map(k => {
        val e: OafEntity = k._1._2
        val t = k._2
        if (t != null && t.getString(0).nonEmpty) {
          if (e.getDataInfo == null) {
            e.setDataInfo(OafUtils.generateDataInfo())
          }
          e.getDataInfo.setDeletedbyinference(true)
        }
        e
      })

    val dedupRecords :Dataset[OafEntity] = spark.read.load(dedupRecordPath).as[OafEntity]

    finalDataset.union(dedupRecords)
      .repartition(1200).write
      .mode(SaveMode.Overwrite).save(destination)

  }

}
