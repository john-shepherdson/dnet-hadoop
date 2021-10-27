package eu.dnetlib.dhp.oa.graph.raw

import com.fasterxml.jackson.databind.ObjectMapper
import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.common.HdfsSupport
import eu.dnetlib.dhp.schema.common.ModelSupport
import eu.dnetlib.dhp.schema.mdstore.MDStoreWithInfo
import eu.dnetlib.dhp.schema.oaf.Oaf
import eu.dnetlib.dhp.utils.DHPUtils
import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.StringUtils
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.spark.sql.{Encoder, Encoders, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.io.Source

object CopyHdfsOafSparkApplication {

  def main(args: Array[String]): Unit = {
    val log = LoggerFactory.getLogger(getClass)
    val conf = new SparkConf()
    val parser = new ArgumentApplicationParser(Source.fromInputStream(getClass.getResourceAsStream("/eu/dnetlib/dhp/oa/graph/copy_hdfs_oaf_parameters.json")).mkString)
    parser.parseArgument(args)

    val spark =
      SparkSession
        .builder()
        .config(conf)
        .appName(getClass.getSimpleName)
        .master(parser.get("master")).getOrCreate()

    val sc: SparkContext = spark.sparkContext

    val mdstoreManagerUrl = parser.get("mdstoreManagerUrl")
    log.info("mdstoreManagerUrl: {}", mdstoreManagerUrl)

    val mdFormat = parser.get("mdFormat")
    log.info("mdFormat: {}", mdFormat)

    val mdLayout = parser.get("mdLayout")
    log.info("mdLayout: {}", mdLayout)

    val mdInterpretation = parser.get("mdInterpretation")
    log.info("mdInterpretation: {}", mdInterpretation)

    val hdfsPath = parser.get("hdfsPath")
    log.info("hdfsPath: {}", hdfsPath)

    implicit val oafEncoder: Encoder[Oaf] = Encoders.kryo[Oaf]

    val paths = DHPUtils.mdstorePaths(mdstoreManagerUrl, mdFormat, mdLayout, mdInterpretation, true).asScala

    val validPaths: List[String] = paths.filter(p => HdfsSupport.exists(p, sc.hadoopConfiguration)).toList

    if (validPaths.nonEmpty) {
      val oaf = spark.read.load(validPaths: _*).as[Oaf]
      val mapper = new ObjectMapper()
      val l =ModelSupport.oafTypes.entrySet.asScala.map(e => e.getKey).toList
      l.foreach(
        e =>
          oaf.filter(o => o.getClass.getSimpleName.equalsIgnoreCase(e))
            .map(s => mapper.writeValueAsString(s))(Encoders.STRING)
            .write
            .option("compression", "gzip")
            .mode(SaveMode.Append)
            .text(s"$hdfsPath/${e}")
      )
    }
  }
}
