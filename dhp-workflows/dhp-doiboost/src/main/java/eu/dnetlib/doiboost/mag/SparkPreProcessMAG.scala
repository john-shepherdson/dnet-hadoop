package eu.dnetlib.doiboost.mag

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.spark.sql.functions._

object SparkPreProcessMAG {


  def main(args: Array[String]): Unit = {

    val logger: Logger = LoggerFactory.getLogger(getClass)
    val conf: SparkConf = new SparkConf()
    val parser = new ArgumentApplicationParser(IOUtils.toString(getClass.getResourceAsStream("/eu/dnetlib/dhp/doiboost/mag/preprocess_mag_params.json")))
    parser.parseArgument(args)
    val spark: SparkSession =
      SparkSession
        .builder()
        .config(conf)
        .appName(getClass.getSimpleName)
        .master(parser.get("master")).getOrCreate()
    import spark.implicits._

    logger.info("Phase 1) make uninque DOI in Papers:")

    val d: Dataset[Papers] = spark.read.load(s"${parser.get("sourcePath")}/Papers").as[Papers]


    // Filtering Papers with DOI, and since for the same DOI we have multiple version of item with different PapersId we get the last one
    val result: RDD[Papers] = d.where(col("Doi").isNotNull).rdd.map { p: Papers => Tuple2(p.Doi, p) }.reduceByKey { case (p1: Papers, p2: Papers) =>
      var r = if (p1 == null) p2 else p1
      if (p1 != null && p2 != null) {
        if (p1.CreatedDate != null && p2.CreatedDate != null) {
          if (p1.CreatedDate.before(p2.CreatedDate))
            r = p1
          else
            r = p2
        } else {
          r = if (p1.CreatedDate == null) p2 else p1
        }
      }
      r
    }.map(_._2)

    val distinctPaper: Dataset[Papers] = spark.createDataset(result)
    distinctPaper.write.mode(SaveMode.Overwrite).save(s"${parser.get("targetPath")}/Papers_distinct")
    logger.info(s"Total number of element: ${result.count()}")

    logger.info("Phase 2) convert InverdIndex Abastrac to string")
    val pa = spark.read.load(s"${parser.get("sourcePath")}/PaperAbstractsInvertedIndex").as[PaperAbstract]
    pa.map(ConversionUtil.transformPaperAbstract).write.mode(SaveMode.Overwrite).save(s"${parser.get("targetPath")}/PaperAbstract")


    distinctPaper.joinWith(pa, col("PaperId").eqia)

  }


}
