package eu.dnetlib.dhp.application.dedup.log

import org.apache.spark.sql.{SaveMode, SparkSession}

class DedupLogWriter (path:String) {


  def appendLog(dedupLogModel: DedupLogModel, spark:SparkSession): Unit = {
    import spark.implicits._
    val df  = spark.createDataset[DedupLogModel](data = List(dedupLogModel))
    df.write.mode(SaveMode.Append).save(path)


  }

}
