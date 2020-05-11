package eu.dnetlib.doiboost.mag

import org.apache.spark.SparkConf
import org.apache.spark.api.java.function.ReduceFunction
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Encoders, SaveMode, SparkSession}
import org.codehaus.jackson.map.ObjectMapper
import org.junit.jupiter.api.Test
import org.slf4j.{Logger, LoggerFactory}
import org.apache.spark.sql.functions._


class MAGMappingTest {

  val logger: Logger = LoggerFactory.getLogger(getClass)
  val mapper = new ObjectMapper()


  //@Test
  def testMAGCSV(): Unit = {

    val conf: SparkConf = new SparkConf()
    val spark: SparkSession =
      SparkSession
        .builder()
        .config(conf)
        .appName(getClass.getSimpleName)
        .master("local[*]").getOrCreate()


    import spark.implicits._
    val d: Dataset[Papers] = spark.read.load("/data/doiboost/mag/datasets/Papers").as[Papers]
    logger.info(s"Total number of element: ${d.where(col("Doi").isNotNull).count()}")
    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Papers]
    val result: RDD[Papers] = d.where(col("Doi").isNotNull).rdd.map { p: Papers => Tuple2(p.Doi, p) }.reduceByKey {case (p1:Papers, p2:Papers)  =>
      var r = if (p1==null) p2 else p1
      if (p1!=null && p2!=null ) if (p1.CreatedDate.before(p2.CreatedDate))
        r = p1
      else
        r = p2
      r
    }.map(_._2)


    val distinctPaper:Dataset[Papers] = spark.createDataset(result)
    distinctPaper.write.mode(SaveMode.Overwrite).save("/data/doiboost/mag/datasets/Papers_d")
    logger.info(s"Total number of element: ${result.count()}")


  }


}
