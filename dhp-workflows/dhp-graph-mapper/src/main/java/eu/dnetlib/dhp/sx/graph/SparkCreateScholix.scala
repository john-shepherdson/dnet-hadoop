package eu.dnetlib.dhp.sx.graph

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.schema.oaf.Relation
import eu.dnetlib.dhp.schema.sx.scholix.Scholix
import eu.dnetlib.dhp.schema.sx.summary.ScholixSummary
import eu.dnetlib.dhp.sx.graph.scholix.ScholixUtils
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

object SparkCreateScholix {

  def main(args: Array[String]): Unit = {
    val log: Logger = LoggerFactory.getLogger(getClass)
    val conf: SparkConf = new SparkConf()
    val parser = new ArgumentApplicationParser(IOUtils.toString(getClass.getResourceAsStream("/eu/dnetlib/dhp/sx/graph/create_scholix_params.json")))
    parser.parseArgument(args)
    val spark: SparkSession =
      SparkSession
        .builder()
        .config(conf)
        .appName(getClass.getSimpleName)
        .master(parser.get("master")).getOrCreate()

    val relationPath = parser.get("relationPath")
    log.info(s"relationPath  -> $relationPath")
    val summaryPath = parser.get("summaryPath")
    log.info(s"summaryPath  -> $summaryPath")
    val targetPath = parser.get("targetPath")
    log.info(s"targetPath  -> $targetPath")


    implicit val relEncoder: Encoder[Relation] = Encoders.kryo[Relation]
    implicit val summaryEncoder: Encoder[ScholixSummary] = Encoders.kryo[ScholixSummary]
    implicit val scholixEncoder: Encoder[Scholix] = Encoders.kryo[Scholix]

    import spark.implicits._


    val relationDS: Dataset[(String, Relation)] = spark.read.load(relationPath).as[Relation]
      .map(r => (r.getSource, r))(Encoders.tuple(Encoders.STRING, relEncoder))

    val summaryDS: Dataset[(String, ScholixSummary)] = spark.read.load(summaryPath).as[ScholixSummary]
      .map(r => (r.getId, r))(Encoders.tuple(Encoders.STRING, summaryEncoder))


    relationDS.joinWith(summaryDS, relationDS("_1").equalTo(summaryDS("_1")), "left")
      .map { input: ((String, Relation), (String, ScholixSummary)) =>
        val rel: Relation = input._1._2
        val source: ScholixSummary = input._2._2
        (rel.getTarget, ScholixUtils.scholixFromSource(rel, source))
      }(Encoders.tuple(Encoders.STRING, scholixEncoder))
      .write.mode(SaveMode.Overwrite).save(s"$targetPath/scholix_from_source")

    val scholixSource: Dataset[(String, Scholix)] = spark.read.load(s"$targetPath/scholix_from_source").as[(String, Scholix)](Encoders.tuple(Encoders.STRING, scholixEncoder))

    scholixSource.joinWith(summaryDS, scholixSource("_1").equalTo(summaryDS("_1")), "left")
      .map { input: ((String, Scholix), (String, ScholixSummary)) =>
        val s: Scholix = input._1._2
        val target: ScholixSummary = input._2._2
        ScholixUtils.generateCompleteScholix(s, target)
      }.write.mode(SaveMode.Overwrite).save(s"$targetPath/scholix_one_verse")


    val scholix_o_v: Dataset[Scholix] = spark.read.load(s"$targetPath/scholix_one_verse").as[Scholix]

    scholix_o_v.flatMap(s => List(s, ScholixUtils.createInverseScholixRelation(s))).groupByKey(_.getIdentifier).reduceGroups { (x, y) =>
      if (x != null)
        x
      else
        y
    }.write.mode(SaveMode.Overwrite).save(s"$targetPath/scholix")
  }
}
