package eu.dnetlib.dhp.sx.graph

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.schema.oaf.Relation
import eu.dnetlib.dhp.schema.sx.scholix.Scholix
import eu.dnetlib.dhp.schema.sx.summary.ScholixSummary
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
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


    implicit val relEncoder:Encoder[Relation] = Encoders.kryo[Relation]
    implicit val  summaryEncoder :Encoder[ScholixSummary] = Encoders.kryo[ScholixSummary]
    implicit val  scholixEncoder :Encoder[Scholix] = Encoders.kryo[Scholix]


    val relationDS:Dataset[(String, Relation)] = spark.read.load(relationPath).as[Relation]
                                                  .map(r => (r.getSource, r))(Encoders.tuple(Encoders.STRING, relEncoder))

    val summaryDS:Dataset[(String, ScholixSummary)] = spark.read.load(summaryPath).as[ScholixSummary]
                                                .map(r => (r.getId, r))(Encoders.tuple(Encoders.STRING, summaryEncoder))


    val res: Array[((String, Relation), (String, ScholixSummary))] =relationDS.joinWith(summaryDS, relationDS("_1").equalTo(summaryDS("_1")), "left").take(10)


    res.foreach(r =>println(r._1._2))

//    relationDS.joinWith(summaryDS, relationDS("_1").equalTo(summaryDS("_1")), "left")
//      .map {input:((String,Relation), (String, ScholixSummary)) =>
//        val rel:Relation = input._1._2
//        val source:ScholixSummary = input._2._2
//
//
//        val s = new Scholix
//
//
//      }







  }

}
