package eu.dnetlib.dhp.sx.graph

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.schema.oaf.utils.MergeUtils
import eu.dnetlib.dhp.schema.oaf.{Dataset => OafDataset, _}
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

object SparkCreateInputGraph {

  def main(args: Array[String]): Unit = {

    val log: Logger = LoggerFactory.getLogger(getClass)
    val conf: SparkConf = new SparkConf()
    val parser = new ArgumentApplicationParser(
      IOUtils.toString(
        getClass.getResourceAsStream("/eu/dnetlib/dhp/sx/graph/extract_entities_params.json")
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

    val resultObject = List(
      ("publication", classOf[Publication]),
      ("dataset", classOf[OafDataset]),
      ("software", classOf[Software]),
      ("otherResearchProduct", classOf[OtherResearchProduct])
    )

    implicit val oafEncoder: Encoder[Oaf] = Encoders.kryo(classOf[Oaf])
    implicit val publicationEncoder: Encoder[Publication] = Encoders.kryo(classOf[Publication])
    implicit val datasetEncoder: Encoder[OafDataset] = Encoders.kryo(classOf[OafDataset])
    implicit val softwareEncoder: Encoder[Software] = Encoders.kryo(classOf[Software])
    implicit val orpEncoder: Encoder[OtherResearchProduct] =
      Encoders.kryo(classOf[OtherResearchProduct])
    implicit val relEncoder: Encoder[Relation] = Encoders.kryo(classOf[Relation])

    val sourcePath = parser.get("sourcePath")
    log.info(s"sourcePath  -> $sourcePath")
    val targetPath = parser.get("targetPath")
    log.info(s"targetPath  -> $targetPath")

    val oafDs: Dataset[Oaf] = spark.read.load(s"$sourcePath/*").as[Oaf]

    log.info("Extract Publication")
    oafDs
      .filter(o => o.isInstanceOf[Publication])
      .map(p => p.asInstanceOf[Publication])
      .write
      .mode(SaveMode.Overwrite)
      .save(s"$targetPath/extracted/publication")

    log.info("Extract dataset")
    oafDs
      .filter(o => o.isInstanceOf[OafDataset])
      .map(p => p.asInstanceOf[OafDataset])
      .write
      .mode(SaveMode.Overwrite)
      .save(s"$targetPath/extracted/dataset")

    log.info("Extract software")
    oafDs
      .filter(o => o.isInstanceOf[Software])
      .map(p => p.asInstanceOf[Software])
      .write
      .mode(SaveMode.Overwrite)
      .save(s"$targetPath/extracted/software")

    log.info("Extract otherResearchProduct")
    oafDs
      .filter(o => o.isInstanceOf[OtherResearchProduct])
      .map(p => p.asInstanceOf[OtherResearchProduct])
      .write
      .mode(SaveMode.Overwrite)
      .save(s"$targetPath/extracted/otherResearchProduct")

    log.info("Extract Relation")
    oafDs
      .filter(o => o.isInstanceOf[Relation])
      .map(p => p.asInstanceOf[Relation])
      .write
      .mode(SaveMode.Overwrite)
      .save(s"$targetPath/extracted/relation")

    resultObject.foreach { r =>
      log.info(s"Make ${r._1} unique")
      makeDatasetUnique(
        s"$targetPath/extracted/${r._1}",
        s"$targetPath/preprocess/${r._1}",
        spark,
        r._2
      )
    }
  }

  def extractEntities[T <: Oaf](
    oafDs: Dataset[Oaf],
    targetPath: String,
    clazz: Class[T],
    log: Logger
  ): Unit = {

    implicit val resEncoder: Encoder[T] = Encoders.kryo(clazz)
    log.info(s"Extract ${clazz.getSimpleName}")
    oafDs
      .filter(o => o.isInstanceOf[T])
      .map(p => p.asInstanceOf[T])
      .write
      .mode(SaveMode.Overwrite)
      .save(targetPath)
  }

  def makeDatasetUnique[T <: Result](
    sourcePath: String,
    targetPath: String,
    spark: SparkSession,
    clazz: Class[T]
  ): Unit = {
    import spark.implicits._

    implicit val resEncoder: Encoder[T] = Encoders.kryo(clazz)

    val ds: Dataset[T] = spark.read.load(sourcePath).as[T]

    ds.groupByKey(_.getId)
      .mapGroups { (id, it) => MergeUtils.mergeGroup(id, it.asJava).asInstanceOf[T] }
//      .reduceGroups { (x: T, y: T) => MergeUtils.merge(x, y).asInstanceOf[T] }
//      .map(_)
      .write
      .mode(SaveMode.Overwrite)
      .save(targetPath)

  }

}
