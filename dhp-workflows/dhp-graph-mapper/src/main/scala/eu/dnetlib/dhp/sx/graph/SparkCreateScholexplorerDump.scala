package eu.dnetlib.dhp.sx.graph

import eu.dnetlib.dhp.application.AbstractScalaApplication
import eu.dnetlib.dhp.schema.oaf.{
  KeyValue,
  OtherResearchProduct,
  Publication,
  Relation,
  Result,
  Software,
  Dataset => OafDataset
}
import eu.dnetlib.dhp.schema.sx.scholix.{Scholix, ScholixResource}
import org.apache.spark.sql.functions.{col, concat, expr, first, md5}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql._
import org.slf4j.{Logger, LoggerFactory}

class SparkCreateScholexplorerDump(propertyPath: String, args: Array[String], log: Logger)
    extends AbstractScalaApplication(propertyPath, args, log: Logger) {

  /** Here all the spark applications runs this method
    * where the whole logic of the spark node is defined
    */
  override def run(): Unit = {
    val sourcePath = parser.get("sourcePath")
    log.info("sourcePath: {}", sourcePath)
    val targetPath = parser.get("targetPath")
    log.info("targetPath: {}", targetPath)
    generateBidirectionalRelations(sourcePath, targetPath, spark)
    generateScholixResource(sourcePath, targetPath, spark)
    generateScholix(targetPath, spark)
  }

  def generateScholixResource(inputPath: String, outputPath: String, spark: SparkSession): Unit = {
    val entityMap: Map[String, StructType] = Map(
      "publication"          -> Encoders.bean(classOf[Publication]).schema,
      "dataset"              -> Encoders.bean(classOf[OafDataset]).schema,
      "software"             -> Encoders.bean(classOf[Software]).schema,
      "otherresearchproduct" -> Encoders.bean(classOf[OtherResearchProduct]).schema
    )

    implicit val scholixResourceEncoder: Encoder[ScholixResource] = Encoders.bean(classOf[ScholixResource])
    implicit val resultEncoder: Encoder[Result] = Encoders.bean(classOf[Result])

    val resDs = spark.emptyDataset[ScholixResource]
    val scholixResourceDS = entityMap.foldLeft[Dataset[ScholixResource]](resDs)((res, item) => {
      println(s"adding ${item._1}")
      res.union(
        spark.read
          .schema(item._2)
          .json(s"$inputPath/${item._1}")
          .as[Result]
          .map(r => ScholexplorerUtils.generateScholixResourceFromResult(r))
          .filter(s => s != null)
      )
    })
    scholixResourceDS.write.mode(SaveMode.Overwrite).save(s"$outputPath/resource")
  }

  def generateBidirectionalRelations(inputPath: String, otuputPath: String, spark: SparkSession): Unit = {
    val relSchema = Encoders.bean(classOf[Relation]).schema

    val relDF = spark.read
      .schema(relSchema)
      .json(s"$inputPath/relation")
      .where(
        "datainfo.deletedbyinference is false and source like '50%' and target like '50%' " +
        "and relClass <> 'merges' and relClass <> 'isMergedIn'"
      )
      .select("source", "target", "collectedfrom", "relClass")

    def invRel: String => String = { s =>
      ScholexplorerUtils.invRel(s)
    }

    import org.apache.spark.sql.functions.udf
    val inverseRelationUDF = udf(invRel)
    val inverseRelation = relDF.select(
      col("target").alias("source"),
      col("source").alias("target"),
      col("collectedfrom"),
      inverseRelationUDF(col("relClass")).alias("relClass")
    )

    val bidRel = inverseRelation
      .union(relDF)
      .withColumn("id", md5(concat(col("source"), col("relClass"), col("target"))))
      .withColumn("cf", expr("transform(collectedfrom, x -> struct(x.key, x.value))"))
      .drop("collectedfrom")
      .withColumnRenamed("cf", "collectedfrom")
      .groupBy(col("id"))
      .agg(
        first("source").alias("source"),
        first("target").alias("target"),
        first("relClass").alias("relClass"),
        first("collectedfrom").alias("collectedfrom")
      )

    bidRel.write.mode(SaveMode.Overwrite).save(s"$otuputPath/relation")

  }

  def generateScholix(outputPath: String, spark: SparkSession): Unit = {
    implicit val scholixResourceEncoder: Encoder[ScholixResource] = Encoders.bean(classOf[ScholixResource])
    implicit val scholixEncoder: Encoder[Scholix] = Encoders.kryo(classOf[Scholix])

    import spark.implicits._
    val relations = spark.read.load(s"$outputPath/relation").as[RelationInfo]
    val resource = spark.read.load(s"$outputPath/resource").as[ScholixResource]

    val scholix_one_verse = relations
      .joinWith(resource, relations("source") === resource("dnetIdentifier"), "inner")
      .map(res => ScholexplorerUtils.generateScholix(res._1, res._2))
      .map(s => (s.getIdentifier, s))(Encoders.tuple(Encoders.STRING, Encoders.kryo(classOf[Scholix])))

    val resourceTarget = relations
      .joinWith(resource, relations("target") === resource("dnetIdentifier"), "inner")
      .map(res => (res._1.id, res._2))(Encoders.tuple(Encoders.STRING, Encoders.kryo(classOf[ScholixResource])))

    scholix_one_verse
      .joinWith(resourceTarget, scholix_one_verse("_1") === resourceTarget("_1"), "inner")
      .map(k => ScholexplorerUtils.updateTarget(k._1._2, k._2._2))
      .write
      .mode(SaveMode.Overwrite)
      .option("compression", "gzip")
      .text(s"$outputPath/scholix")
  }
}

object SparkCreateScholexplorerDump {
  val logger: Logger = LoggerFactory.getLogger(SparkCreateScholexplorerDump.getClass)

  def main(args: Array[String]): Unit = {
    new SparkCreateScholexplorerDump(
      log = logger,
      args = args,
      propertyPath = "/eu/dnetlib/dhp/sx/create_scholix_dump_params.json"
    ).initialize().run()
  }
}
