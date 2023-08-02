package eu.dnetlib.dhp.sx.graph

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.schema.oaf.Relation
import eu.dnetlib.dhp.schema.sx.scholix.{Scholix, ScholixResource}
import eu.dnetlib.dhp.schema.sx.summary.ScholixSummary
import eu.dnetlib.dhp.sx.graph.scholix.ScholixUtils
import eu.dnetlib.dhp.sx.graph.scholix.ScholixUtils.RelatedEntities
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, count}
import org.apache.spark.sql._
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Try

object SparkCreateScholix {

  def main(args: Array[String]): Unit = {
    val log: Logger = LoggerFactory.getLogger(getClass)
    val conf: SparkConf = new SparkConf()
    val parser = new ArgumentApplicationParser(
      IOUtils.toString(
        getClass.getResourceAsStream("/eu/dnetlib/dhp/sx/graph/create_scholix_params.json")
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

    val relationPath = parser.get("relationPath")
    log.info(s"relationPath  -> $relationPath")
    val summaryPath = parser.get("summaryPath")
    log.info(s"summaryPath  -> $summaryPath")
    val targetPath = parser.get("targetPath")
    log.info(s"targetPath  -> $targetPath")
    val dumpCitations = Try(parser.get("dumpCitations").toBoolean).getOrElse(false)
    log.info(s"dumpCitations  -> $dumpCitations")

    implicit val relEncoder: Encoder[Relation] = Encoders.bean(classOf[Relation])
    implicit val summaryEncoder: Encoder[ScholixSummary] = Encoders.bean(classOf[ScholixSummary])
    implicit val resourceEncoder: Encoder[ScholixResource] = Encoders.bean(classOf[ScholixResource])
    implicit val scholixEncoder: Encoder[Scholix] = Encoders.bean(classOf[Scholix])
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    val relationDS: Dataset[Relation] = spark.read
      .schema(relEncoder.schema)
      .json(relationPath)
      .as[Relation]
      .filter(r =>
        (r.getDataInfo == null || r.getDataInfo.getDeletedbyinference == false) && !r.getRelClass.toLowerCase
          .contains("merge")
      )

    val summaryDS: Dataset[ScholixResource] = spark.read
      .schema(summaryEncoder.schema)
      .json(summaryPath)
      .as[ScholixSummary]
      .map(s => ScholixUtils.generateScholixResourceFromSummary(s))

    val scholixSource: Dataset[Scholix] = relationDS
      .joinWith(summaryDS, relationDS("source").equalTo(summaryDS("dnetIdentifier")), "left")
      .map { input: (Relation, ScholixResource) =>
        if (input._1 != null && input._2 != null) {
          val rel: Relation = input._1
          val source: ScholixResource = input._2
          val s = ScholixUtils.scholixFromSource(rel, source)
          s.setIdentifier(rel.getTarget)
          s
        } else null
      }(scholixEncoder)
      .filter(r => r != null)

    scholixSource
      .joinWith(summaryDS, scholixSource("identifier").equalTo(summaryDS("dnetIdentifier")), "left")
      .map { input: (Scholix, ScholixResource) =>
        if (input._2 == null) {
          null
        } else {
          val s: Scholix = input._1
          val target: ScholixResource = input._2
          ScholixUtils.generateCompleteScholix(s, target)
        }
      }
      .filter(s => s != null)
      .write
      .option("compression", "lz4")
      .mode(SaveMode.Overwrite)
      .save(s"$targetPath/scholix_one_verse")

    val scholix_o_v: Dataset[Scholix] =
      spark.read.load(s"$targetPath/scholix_one_verse").as[Scholix]

    def scholix_complete(s: Scholix): Boolean = {
      if (s == null || s.getIdentifier == null) {
        false
      } else if (s.getSource == null || s.getTarget == null) {
        false
      } else if (s.getLinkprovider == null || s.getLinkprovider.isEmpty)
        false
      else
        true
    }

    val scholix_final: Dataset[Scholix] = scholix_o_v
      .filter(s => scholix_complete(s))
      .groupByKey(s =>
        scala.Ordering.String
          .min(s.getSource.getDnetIdentifier, s.getTarget.getDnetIdentifier)
          .concat(s.getRelationship.getName)
          .concat(scala.Ordering.String.max(s.getSource.getDnetIdentifier, s.getTarget.getDnetIdentifier))
      )
      .flatMapGroups((id, scholixes) => {
        val s = scholixes.toList
        if (s.size == 1) Seq(s(0), ScholixUtils.createInverseScholixRelation(s(0)))
        else s
      })

    scholix_final.write.mode(SaveMode.Overwrite).save(s"$targetPath/scholix")

    val stats: Dataset[(String, String, Long)] = scholix_final
      .map(s => (s.getSource.getDnetIdentifier, s.getTarget.getObjectType))
      .groupBy("_1", "_2")
      .agg(count("_1"))
      .as[(String, String, Long)]

    stats
      .map(s =>
        RelatedEntities(
          s._1,
          if ("dataset".equalsIgnoreCase(s._2)) s._3 else 0,
          if ("publication".equalsIgnoreCase(s._2)) s._3 else 0
        )
      )
      .groupByKey(_.id)
      .reduceGroups((a, b) =>
        RelatedEntities(
          a.id,
          a.relatedDataset + b.relatedDataset,
          a.relatedPublication + b.relatedPublication
        )
      )
      .map(_._2)
      .write
      .option("compression", "lz4")
      .mode(SaveMode.Overwrite)
      .save(s"$targetPath/related_entities")

    val relatedEntitiesDS: Dataset[RelatedEntities] = spark.read
      .load(s"$targetPath/related_entities")
      .as[RelatedEntities]
      .filter(r => dumpCitations || r.relatedPublication > 0 || r.relatedDataset > 0)

    val summaryDS2: Dataset[ScholixSummary] = spark.read
      .schema(summaryEncoder.schema)
      .json(summaryPath)
      .as[ScholixSummary]

    relatedEntitiesDS
      .joinWith(summaryDS2, relatedEntitiesDS("id").equalTo(summaryDS("id")), "inner")
      .map { i =>
        val re = i._1
        val sum = i._2
        sum.setRelatedDatasets(re.relatedDataset)
        sum.setRelatedPublications(re.relatedPublication)
        sum
      }
      .write
      .mode(SaveMode.Overwrite)
      .option("compression", "lz4")
      .save(s"${summaryPath}_filtered")
  }
}
