package eu.dnetlib.dhp.incremental

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.bulktag.community.TaggingConstants
import eu.dnetlib.dhp.common.enrichment.Constants
import eu.dnetlib.dhp.schema.common.ModelSupport
import eu.dnetlib.dhp.schema.oaf.{Oaf, OafEntity}
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters.{
  collectionAsScalaIterableConverter,
  mapAsScalaMapConverter,
  seqAsJavaListConverter
}

object SparkAppendContextCleanedGraph {

  def main(args: Array[String]): Unit = {
    val log: Logger = LoggerFactory.getLogger(getClass)
    val conf: SparkConf = new SparkConf()

    val parser = new ArgumentApplicationParser(
      IOUtils.toString(
        getClass.getResourceAsStream(
          "/eu/dnetlib/dhp/oa/graph/incremental/export_hive/append_context_cleaned_graph.json"
        )
      )
    )
    parser.parseArgument(args)
    conf.set("hive.metastore.uris", parser.get("hiveMetastoreUris"))

    val outputPath = parser.get("outputPath")
    log.info(s"outputPath  -> $outputPath")

    val hiveDbName = parser.get("hiveDbName")
    log.info(s"hiveDbName  -> $hiveDbName")

    val spark: SparkSession =
      SparkSession
        .builder()
        .config(conf)
        .enableHiveSupport()
        .appName(getClass.getSimpleName)
        .getOrCreate()

    for ((entity, clazz) <- ModelSupport.oafTypes.asScala.filter(t => !Seq("datasource", "organization", "person", "project").contains(t._1))) {
      if (classOf[OafEntity].isAssignableFrom(clazz)) {
        val classEnc: Encoder[Oaf] = Encoders.bean(clazz).asInstanceOf[Encoder[Oaf]]

        spark
          .table(s"${hiveDbName}.${entity}")
          .as(classEnc)
          .map(e => {
            val oaf = e.asInstanceOf[OafEntity]
            if (oaf.getContext != null) {
              val newContext = oaf.getContext.asScala
                .map(c => {
                  if (c.getDataInfo != null) {
                    c.setDataInfo(
                      c.getDataInfo.asScala
                        .filter(
                          di =>
                            di == null || di.getInferenceprovenance == null ||
                              (!di.getInferenceprovenance.equals(Constants.PROPAGATION_DATA_INFO_TYPE)
                                && !di.getInferenceprovenance.equals(TaggingConstants.BULKTAG_DATA_INFO_TYPE))
                        )
                        .toList
                        .asJava
                    )
                  }
                  c
                })
                .filter(!_.getDataInfo.isEmpty)
                .toList
                .asJava
              oaf.setContext(newContext)
            }
            e
          })(classEnc)
          .write
          .option("compression", "gzip")
          .mode(SaveMode.Append)
          .json(s"$outputPath/${entity}")
      } else {
        spark
          .table(s"${hiveDbName}.${entity}")
          .write
          .option("compression", "gzip")
          .mode(SaveMode.Append)
          .json(s"$outputPath/${entity}")
      }
    }
  }
}
