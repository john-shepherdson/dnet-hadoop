package eu.dnetlib.doiboost

//import org.apache.spark.SparkConf
//import org.apache.spark.sql.{Dataset, Encoders, Row, SparkSession}
//
//object SparkDownloadContentFromCrossref {
//
//
//  def main(args: Array[String]): Unit = {
//
//
//    val conf: SparkConf = new SparkConf().setAppName("DownloadContentFromCrossref").setMaster("local[*]")
//
//    val spark = SparkSession.builder().config(conf).getOrCreate()
//
//
//    val sc = spark.sparkContext
//    import spark.implicits._
//    spark.read.option("header", "false")
//      .option("delimiter", "\t")
//      .csv("/Users/sandro/Downloads/doiboost/mag_Journals.txt.gz")
//
//
//    val d = spark.read.option("header", "false")
//      .option("delimiter", "\t")
//      .csv("/Users/sandro/Downloads/doiboost/mag_Journals.txt.gz")
//      .map(f =>
//        Journal( f.getAs[String](0).toLong, f.getAs[String](1).toInt, f.getAs[String](2),
//          f.getAs[String](3), f.getAs[String](4), f.getAs[String](5), f.getAs[String](6),
//          f.getAs[String](7).toLong, f.getAs[String](8).toLong, f.getAs[String](9)
//        ))
//
//    d.show()
//
//    d.printSchema()
//
//
//
//
//
//
//
//
//  }
//
//
//}
//

