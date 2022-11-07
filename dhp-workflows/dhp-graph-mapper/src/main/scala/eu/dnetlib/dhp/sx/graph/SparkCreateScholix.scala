package eu.dnetlib.dhp.sx.graph

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.schema.oaf.Relation
import eu.dnetlib.dhp.schema.sx.scholix.Scholix
import eu.dnetlib.dhp.schema.sx.summary.ScholixSummary
import eu.dnetlib.dhp.sx.graph.scholix.ScholixUtils
import eu.dnetlib.dhp.sx.graph.scholix.ScholixUtils.RelatedEntities
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.count
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

    implicit val relEncoder: Encoder[Relation] = Encoders.kryo[Relation]
    implicit val summaryEncoder: Encoder[ScholixSummary] = Encoders.kryo[ScholixSummary]
    implicit val scholixEncoder: Encoder[Scholix] = Encoders.kryo[Scholix]

    import spark.implicits._

    val relationDS: Dataset[(String, Relation)] = spark.read
      .load(relationPath)
      .as[Relation]
      .filter(r =>
        (r.getDataInfo == null || r.getDataInfo.getDeletedbyinference == false) && !r.getRelClass.toLowerCase
          .contains("merge")
      )
      .map(r => (r.getSource, r))(Encoders.tuple(Encoders.STRING, relEncoder))

    val summaryDS: Dataset[(String, ScholixSummary)] = spark.read
      .load(summaryPath)
      .as[ScholixSummary]
      .map(r => (r.getId, r))(Encoders.tuple(Encoders.STRING, summaryEncoder))

    relationDS
      .joinWith(summaryDS, relationDS("_1").equalTo(summaryDS("_1")), "left")
      .map { input: ((String, Relation), (String, ScholixSummary)) =>
        if (input._1 != null && input._2 != null) {
          val rel: Relation = input._1._2
          val source: ScholixSummary = input._2._2
          (rel.getTarget, ScholixUtils.scholixFromSource(rel, source))
        } else null
      }(Encoders.tuple(Encoders.STRING, scholixEncoder))
      .filter(r => r != null)
      .write
      .mode(SaveMode.Overwrite)
      .save(s"$targetPath/scholix_from_source")

    val scholixSource: Dataset[(String, Scholix)] = spark.read
      .load(s"$targetPath/scholix_from_source")
      .as[(String, Scholix)](Encoders.tuple(Encoders.STRING, scholixEncoder))

    scholixSource
      .joinWith(summaryDS, scholixSource("_1").equalTo(summaryDS("_1")), "left")
      .map { input: ((String, Scholix), (String, ScholixSummary)) =>
        if (input._2 == null) {
          null
        } else {
          val s: Scholix = input._1._2
          val target: ScholixSummary = input._2._2
          ScholixUtils.generateCompleteScholix(s, target)
        }
      }
      .filter(s => s != null)
      .write
      .mode(SaveMode.Overwrite)
      .save(s"$targetPath/scholix_one_verse")

    val scholix_o_v: Dataset[Scholix] =
      spark.read.load(s"$targetPath/scholix_one_verse").as[Scholix]

    scholix_o_v
      .flatMap(s => List(s, ScholixUtils.createInverseScholixRelation(s)))
      .as[Scholix]
      .map(s => (s.getIdentifier, s))(Encoders.tuple(Encoders.STRING, scholixEncoder))
      .groupByKey(_._1)
      .agg(ScholixUtils.scholixAggregator.toColumn)
      .map(s => s._2)
      .write
      .mode(SaveMode.Overwrite)
      .save(s"$targetPath/scholix")

    val scholix_final: Dataset[Scholix] = spark.read.load(s"$targetPath/scholix").as[Scholix]

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
      .mode(SaveMode.Overwrite)
      .save(s"$targetPath/related_entities")

    val relatedEntitiesDS: Dataset[RelatedEntities] = spark.read
      .load(s"$targetPath/related_entities")
      .as[RelatedEntities]
      .filter(r => dumpCitations || r.relatedPublication > 0 || r.relatedDataset > 0)

    relatedEntitiesDS
      .joinWith(summaryDS, relatedEntitiesDS("id").equalTo(summaryDS("_1")), "inner")
      .map { i =>
        val re = i._1
        val sum = i._2._2

        sum.setRelatedDatasets(re.relatedDataset)
        sum.setRelatedPublications(re.relatedPublication)
        sum
      }
      .write
      .mode(SaveMode.Overwrite)
      .save(s"${summaryPath}_filtered")

  }
}
