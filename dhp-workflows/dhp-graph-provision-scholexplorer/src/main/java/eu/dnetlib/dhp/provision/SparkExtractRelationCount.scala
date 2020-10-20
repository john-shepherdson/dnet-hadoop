package eu.dnetlib.dhp.provision

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.schema.oaf.Relation
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.{Encoder, Encoders, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{coalesce, col, count, lit}


/**
 * SparkExtractRelationCount is a spark job that takes in input relation RDD and retrieve for each item in relation
 * which are the number of - Related Dataset - Related Publication - Related Unknown
 */
object SparkExtractRelationCount {


  def main(args: Array[String]): Unit = {

    val parser = new ArgumentApplicationParser(IOUtils.toString(SparkExtractRelationCount.getClass.getResourceAsStream("/eu/dnetlib/dhp/provision/input_related_entities_parameters.json")))
    parser.parseArgument(args)
    val spark = SparkSession.builder.appName(SparkExtractRelationCount.getClass.getSimpleName).master(parser.get("master")).getOrCreate

    val workingDirPath = parser.get("workingDirPath")

    val relationPath = parser.get("relationPath")

    implicit val relEncoder: Encoder[Relation] = Encoders.kryo[Relation]

    val relation = spark.read.load(relationPath).as[Relation].map(r =>r)(Encoders.bean(classOf[Relation]))

    val relatedPublication = relation
        .where("target like '50%'")
        .groupBy("source")
        .agg(count("target").as("publication"))
        .select(col("source"). alias("p_source"), col("publication"))
    val relatedDataset = relation
        .where("target like '60%'")
        .groupBy("source")
        .agg(count("target").as("dataset"))
        .select(col("source"). alias("d_source"), col("dataset"))
    val relatedUnknown = relation
        .where("target like '70%'")
        .groupBy("source")
        .agg(count("target").as("unknown"))
        .select(col("source"). alias("u_source"), col("unknown"))
    val firstJoin = relatedPublication
          .join(relatedDataset,col("p_source").equalTo(col("d_source")),"full")
          .select(    coalesce( col("p_source"), col("d_source")).alias("id"),
                      col("publication"),
                      col("dataset"))
          .join(relatedUnknown, col("u_source").equalTo(col("id")),"full")
          .select(  coalesce(col("u_source"), col("id")).alias("source"),
                    coalesce(col("publication"),lit(0)).alias("relatedPublication"),
                    coalesce(col("dataset"),lit(0)).alias("relatedDataset"),
                    coalesce(col("unknown"),lit(0)).alias("relatedUnknown")
                )
    firstJoin.write.mode(SaveMode.Overwrite).save(s"$workingDirPath/relatedItemCount")
  }

}
