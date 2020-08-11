package eu.dnetlib.dhp.provision

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.provision.scholix.Scholix
import eu.dnetlib.dhp.provision.scholix.summary.ScholixSummary
import org.apache.commons.io.IOUtils
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.codehaus.jackson.map.ObjectMapper

object SparkConvertDatasetToJson {

  def main(args: Array[String]): Unit = {
    val parser = new ArgumentApplicationParser(IOUtils.toString(SparkConvertDatasetToJson.getClass.getResourceAsStream("/eu/dnetlib/dhp/provision/dataset2Json.json")))
    parser.parseArgument(args)
    val conf = new SparkConf
    val spark = SparkSession.builder.config(conf).appName(SparkConvertDatasetToJson.getClass.getSimpleName).master(parser.get("master")).getOrCreate

    implicit val summaryEncoder: Encoder[ScholixSummary] = Encoders.kryo[ScholixSummary]
    implicit val scholixEncoder: Encoder[Scholix] = Encoders.kryo[Scholix]


    val workingPath = parser.get("workingPath")



    spark.read.load(s"$workingPath/summary").as[ScholixSummary]
      .map(s => new ObjectMapper().writeValueAsString(s))(Encoders.STRING)
      .rdd.repartition(500).saveAsTextFile(s"$workingPath/summary_json", classOf[GzipCodec])

    spark.read.load(s"$workingPath/scholix").as[Scholix]
      .map(s => new ObjectMapper().writeValueAsString(s))(Encoders.STRING)
      .rdd.repartition(2000).saveAsTextFile(s"$workingPath/scholix_json", classOf[GzipCodec])

  }

}
