package eu.dnetlib.dhp.collection.mag

import eu.dnetlib.dhp.application.AbstractScalaApplication
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.slf4j.Logger

class SparkCreateMagDataset (propertyPath: String, args: Array[String], log: Logger)
  extends AbstractScalaApplication(propertyPath, args, log: Logger) {



  /** Here all the spark applications runs this method
   * where the whole logic of the spark node is defined
   */
  override def run(): Unit = {

  }


  private def loadAndFilterPapers(spark:SparkSession, crossrefPath:String, magBasePath:String, workingPath): Unit = {

    import spark.implicits._
    val schema:StructType= StructType(StructField("DOI", StringType)::Nil)
    log.info("Phase 1 intersect MAG Paper containing doi also present in crossref")

    //Filter all the MAG Papers that intersect with a Crossref DOI
    val crId= spark.read.schema(schema).json(crossrefPath).withColumn("crId", lower(col("DOI"))).distinct.select("crId")
    val magPapers = MagUtility.loadMagEntity(spark, "Papers", magBasePath).withColumn("Doi", lower(col("Doi"))).where(col("Doi").isNotNull)


    val intersectedPapers:Dataset[Row] =magPapers.join(crId, magPapers("Doi").equalTo(crId("crId")), "leftsemi").dropDuplicates("Doi")
    intersectedPapers.cache()
    intersectedPapers.count()
    log.info("Create current abstract")

    //Abstract is an inverted list, we define a function that convert in string the abstract and recreate
    // a table(PaperId, Abstract)
    val paperAbstract = MagUtility.loadMagEntity(spark, "PaperAbstractsInvertedIndex", magBasePath)
      .map(s => (s.getLong(0),MagUtility.convertInvertedIndexString(s.getString(1))))
      .withColumnRenamed("_1","PaperId")
      .withColumnRenamed("_2","Abstract")

    //We define Step0 as the result of the  Join between PaperIntersection and the PaperAbstract

    val step0 =intersectedPapers
      .join(paperAbstract, intersectedPapers("PaperId") === paperAbstract("PaperId"), "left")
      .select(intersectedPapers("*"),paperAbstract("Abstract")).cache()

    step0.count()

    intersectedPapers.unpersist()

    // We have three table Author, Affiliation, and PaperAuthorAffiliation, in the
    //next step we create a table containing
    val authors = MagUtility.loadMagEntity(spark, "Authors", magBasePath)
    val affiliations= MagUtility.loadMagEntity(spark, "Affiliations", magBasePath)
    val paaf= MagUtility.loadMagEntity(spark, "PaperAuthorAffiliations", magBasePath)

    val paperAuthorAffiliations =paaf.join(step0,paaf("PaperId") === step0("PaperId"),"leftsemi")

    val j1 = paperAuthorAffiliations.join(authors,paperAuthorAffiliations("AuthorId") === authors("AuthorId"), "inner")
      .select(col("PaperId"), col("AffiliationId"),col("AuthorSequenceNumber"), authors("DisplayName").alias("AuthorName"), authors("AuthorId"))

    val paperAuthorAffiliationNormalized = j1.join(affiliations, j1("AffiliationId")=== affiliations("AffiliationId"), "left")
      .select(j1("*"), affiliations("DisplayName").alias("AffiliationName"), affiliations("GridId"))
      .groupBy("PaperId")
      .agg(collect_list(struct("AffiliationId","AuthorSequenceNumber","AffiliationName","AuthorName","AuthorId","GridId")).alias("authors"))
    val step1 =step0.join(paperAuthorAffiliationNormalized, step0("PaperId")=== paperAuthorAffiliationNormalized("PaperId"), "left").cache()
    step1.count()

    step0.unpersist()


  }
}
