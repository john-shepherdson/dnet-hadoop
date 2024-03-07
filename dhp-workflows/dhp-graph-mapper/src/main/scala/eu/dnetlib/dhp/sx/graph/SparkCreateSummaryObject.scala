package eu.dnetlib.dhp.sx.graph

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.schema.oaf.{Oaf, Result}
import eu.dnetlib.dhp.schema.sx.summary.ScholixSummary
import eu.dnetlib.dhp.sx.graph.scholix.ScholixUtils
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.slf4j.{Logger, LoggerFactory}

object SparkCreateSummaryObject {

  def main(args: Array[String]): Unit = {
    val log: Logger = LoggerFactory.getLogger(getClass)
    val conf: SparkConf = new SparkConf()
    val parser = new ArgumentApplicationParser(
      IOUtils.toString(
        getClass.getResourceAsStream("/eu/dnetlib/dhp/sx/graph/create_summaries_params.json")
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
    val targetPath = parser.get("targetPath")
    log.info(s"targetPath  -> $targetPath")

    implicit val resultEncoder: Encoder[Result] = Encoders.kryo[Result]
    implicit val oafEncoder: Encoder[Oaf] = Encoders.kryo[Oaf]

    implicit val summaryEncoder: Encoder[ScholixSummary] = Encoders.kryo[ScholixSummary]

    val ds: Dataset[Result] = spark.read
      .load(s"$sourcePath/*")
      .as[Result]
      .filter(r => r.getDataInfo == null || r.getDataInfo.getDeletedbyinference == false)

    ds.repartition(6000)
      .map(r => ScholixUtils.resultToSummary(r))
      .filter(s => s != null)
      .write
      .mode(SaveMode.Overwrite)
      .save(targetPath)

  }

}
