package eu.dnetlib.dhp.provision

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, col, count, lit}

object DatasetJoiner {

  def startJoin(spark: SparkSession, relPath:String, targetPath:String) {
    val relation = spark.read.load(relPath)

    val relatedPublication = relation.where("target like '50%'").groupBy("source").agg(count("target").as("publication")).select(col("source"). alias("p_source"), col("publication"))
    val relatedDataset = relation.where("target like '60%'").groupBy("source").agg(count("target").as("dataset")).select(col("source"). alias("d_source"), col("dataset"))
    val relatedUnknown = relation.where("target like '70%'").groupBy("source").agg(count("target").as("unknown")).select(col("source"). alias("u_source"), col("unknown"))
    val firstJoin = relatedPublication
        .join(relatedDataset,col("p_source").equalTo(col("d_source")),"full")
      .select(coalesce(col("p_source"), col("d_source")).alias("id"),
              col("publication"),
              col("dataset"))
      .join(relatedUnknown, col("u_source").equalTo(col("id")),"full")
      .select(coalesce(col("u_source"), col("id")).alias("source"),
        coalesce(col("publication"),lit(0)).alias("relatedPublication"),
        coalesce(col("dataset"),lit(0)).alias("relatedDataset"),
        coalesce(col("unknown"),lit(0)).alias("relatedUnknown")
      )
    firstJoin.write.mode("overwrite").save(targetPath)

  }

}
