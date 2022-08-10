package eu.dnetlib.dhp.sx.graph

import com.fasterxml.jackson.databind.ObjectMapper
import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.schema.common.ModelConstants
import eu.dnetlib.dhp.schema.oaf.{OtherResearchProduct, Publication, Relation, Result, Software, Dataset => OafDataset}
import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

object SparkConvertRDDtoDataset {

  def main(args: Array[String]): Unit = {

    val log: Logger = LoggerFactory.getLogger(getClass)
    val conf: SparkConf = new SparkConf()
    val parser = new ArgumentApplicationParser(
      IOUtils.toString(
        getClass.getResourceAsStream("/eu/dnetlib/dhp/sx/graph/convert_dataset_json_params.json")
      )
    )
    parser.parseArgument(args)
    val spark: SparkSession =
      SparkSession
        .builder()
        .config(conf)
        .appName(getClass.getSimpleName)
        .master(parser.get("master"))
        .getOrCreate()

    val sourcePath = parser.get("sourcePath")
    log.info(s"sourcePath  -> $sourcePath")
    val t = parser.get("targetPath")
    log.info(s"targetPath  -> $t")

    val filterRelation = parser.get("filterRelation")
    log.info(s"filterRelation  -> $filterRelation")

    val entityPath = s"$t/entities"
    val relPath = s"$t/relation"
    val mapper = new ObjectMapper()
    implicit val datasetEncoder: Encoder[OafDataset] = Encoders.kryo(classOf[OafDataset])
    implicit val publicationEncoder: Encoder[Publication] = Encoders.kryo(classOf[Publication])
    implicit val relationEncoder: Encoder[Relation] = Encoders.kryo(classOf[Relation])
    implicit val orpEncoder: Encoder[OtherResearchProduct] =
      Encoders.kryo(classOf[OtherResearchProduct])
    implicit val softwareEncoder: Encoder[Software] = Encoders.kryo(classOf[Software])

    log.info("Converting dataset")
    val rddDataset = spark.sparkContext
      .textFile(s"$sourcePath/dataset")
      .map(s => mapper.readValue(s, classOf[OafDataset]))
      .filter(r => r.getDataInfo != null && r.getDataInfo.getDeletedbyinference == false)
    spark
      .createDataset(rddDataset)
      .as[OafDataset]
      .write
      .mode(SaveMode.Overwrite)
      .save(s"$entityPath/dataset")

    log.info("Converting publication")
    val rddPublication = spark.sparkContext
      .textFile(s"$sourcePath/publication")
      .map(s => mapper.readValue(s, classOf[Publication]))
      .filter(r => r.getDataInfo != null && r.getDataInfo.getDeletedbyinference == false)
    spark
      .createDataset(rddPublication)
      .as[Publication]
      .write
      .mode(SaveMode.Overwrite)
      .save(s"$entityPath/publication")

    log.info("Converting software")
    val rddSoftware = spark.sparkContext
      .textFile(s"$sourcePath/software")
      .map(s => mapper.readValue(s, classOf[Software]))
      .filter(r => r.getDataInfo != null && r.getDataInfo.getDeletedbyinference == false)
    spark
      .createDataset(rddSoftware)
      .as[Software]
      .write
      .mode(SaveMode.Overwrite)
      .save(s"$entityPath/software")

    log.info("Converting otherresearchproduct")
    val rddOtherResearchProduct = spark.sparkContext
      .textFile(s"$sourcePath/otherresearchproduct")
      .map(s => mapper.readValue(s, classOf[OtherResearchProduct]))
      .filter(r => r.getDataInfo != null && r.getDataInfo.getDeletedbyinference == false)
    spark
      .createDataset(rddOtherResearchProduct)
      .as[OtherResearchProduct]
      .write
      .mode(SaveMode.Overwrite)
      .save(s"$entityPath/otherresearchproduct")

    log.info("Converting Relation")

    val relationSemanticFilter = List(
      ModelConstants.MERGES,
      ModelConstants.IS_MERGED_IN,
      ModelConstants.HAS_AMONG_TOP_N_SIMILAR_DOCS,
      ModelConstants.IS_AMONG_TOP_N_SIMILAR_DOCS
    )

    val rddRelation = spark.sparkContext
      .textFile(s"$sourcePath/relation")
      .map(s => mapper.readValue(s, classOf[Relation]))
      .filter(r => r.getDataInfo != null && r.getDataInfo.getDeletedbyinference == false)
      .filter(r => r.getSource.startsWith("50") && r.getTarget.startsWith("50"))
      //filter OpenCitations relations
      .filter(r =>
        r.getDataInfo.getProvenanceaction != null &&
        !"sysimport:crosswalk:opencitations".equals(r.getDataInfo.getProvenanceaction.getClassid)
      )
      .filter(r => filterRelations(filterRelation, relationSemanticFilter, r))
    spark.createDataset(rddRelation).as[Relation].write.mode(SaveMode.Overwrite).save(s"$relPath")
  }

  private def filterRelations(filterRelation: String, relationSemanticFilter: List[String], r: Relation): Boolean = {
    if (filterRelation != null && StringUtils.isNoneBlank(filterRelation)) {
      r.getSubRelType != null && r.getSubRelType.equalsIgnoreCase(filterRelation)
    } else {
      !relationSemanticFilter.exists(k => k.equalsIgnoreCase(r.getRelClass))
    }
  }
}
