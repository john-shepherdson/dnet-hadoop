package eu.dnetlib.dhp.collection.mag

import eu.dnetlib.dhp.application.AbstractScalaApplication
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

class SparkCreateMagDenormalizedTable(propertyPath: String, args: Array[String], log: Logger)
    extends AbstractScalaApplication(propertyPath, args, log: Logger) {

  /** Here all the spark applications runs this method
    * where the whole logic of the spark node is defined
    */
  override def run(): Unit = {
    val crossrefPath: String = parser.get("crossrefPath")
    log.info("found parameters crossrefPath: {}", crossrefPath)
    val magBasePath: String = parser.get("magBasePath")
    log.info("found parameters magBasePath: {}", magBasePath)
    val workingPath: String = parser.get("workingPath")
    log.info("found parameters workingPath: {}", workingPath)
    generatedDenormalizedMAGTable(spark, crossrefPath, magBasePath, workingPath)
  }

  private def generatedDenormalizedMAGTable(
    spark: SparkSession,
    crossrefPath: String,
    magBasePath: String,
    workingPath: String
  ): Unit = {

    import spark.implicits._
    val schema: StructType = StructType(StructField("DOI", StringType) :: Nil)

    //Filter all the MAG Papers that intersect with a Crossref DOI
    val crId =
      spark.read.schema(schema).json(crossrefPath).withColumn("crId", lower(col("DOI"))).distinct.select("crId")
    val magPapers = MagUtility
      .loadMagEntity(spark, "Papers", magBasePath)
      .withColumn("Doi", lower(col("Doi")))
      .where(col("Doi").isNotNull)

    val intersectedPapers: Dataset[Row] =
      magPapers.join(crId, magPapers("Doi").equalTo(crId("crId")), "leftsemi").dropDuplicates("Doi")
    intersectedPapers.cache()
    intersectedPapers.count()
    //log.info("Create current abstract")

    //Abstract is an inverted list, we define a function that convert in string the abstract and recreate
    // a table(PaperId, Abstract)
    val paperAbstract = MagUtility
      .loadMagEntity(spark, "PaperAbstractsInvertedIndex", magBasePath)
      .map(s => (s.getLong(0), MagUtility.convertInvertedIndexString(s.getString(1))))
      .withColumnRenamed("_1", "PaperId")
      .withColumnRenamed("_2", "Abstract")

    //We define Step0 as the result of the  Join between PaperIntersection and the PaperAbstract

    val step0 = intersectedPapers
      .join(paperAbstract, intersectedPapers("PaperId") === paperAbstract("PaperId"), "left")
      .select(intersectedPapers("*"), paperAbstract("Abstract"))
      .cache()

    step0.count()

    intersectedPapers.unpersist()

    // We have three table Author, Affiliation, and PaperAuthorAffiliation, in the
    //next step we create a table containing
    val authors = MagUtility.loadMagEntity(spark, "Authors", magBasePath)
    val affiliations = MagUtility.loadMagEntity(spark, "Affiliations", magBasePath)
    val paaf = MagUtility.loadMagEntity(spark, "PaperAuthorAffiliations", magBasePath)

    val paperAuthorAffiliations = paaf.join(step0, paaf("PaperId") === step0("PaperId"), "leftsemi")

    val j1 = paperAuthorAffiliations
      .join(authors, paperAuthorAffiliations("AuthorId") === authors("AuthorId"), "inner")
      .select(
        col("PaperId"),
        col("AffiliationId"),
        col("AuthorSequenceNumber"),
        authors("DisplayName").alias("AuthorName"),
        authors("AuthorId")
      )

    val paperAuthorAffiliationNormalized = j1
      .join(affiliations, j1("AffiliationId") === affiliations("AffiliationId"), "left")
      .select(j1("*"), affiliations("DisplayName").alias("AffiliationName"), affiliations("GridId"))
      .groupBy("PaperId")
      .agg(
        collect_list(
          struct("AffiliationId", "AuthorSequenceNumber", "AffiliationName", "AuthorName", "AuthorId", "GridId")
        ).alias("authors")
      )
    val step1 = step0
      .join(paperAuthorAffiliationNormalized, step0("PaperId") === paperAuthorAffiliationNormalized("PaperId"), "left")
      .select(step0("*"), paperAuthorAffiliationNormalized("authors"))
      .cache()
    step1.count()

    step0.unpersist()

    val conference = MagUtility
      .loadMagEntity(spark, "ConferenceInstances", magBasePath)
      .select(
        $"ConferenceInstanceId",
        $"DisplayName".as("conferenceName"),
        $"Location".as("conferenceLocation"),
        $"StartDate".as("conferenceStartDate"),
        $"EndDate".as("conferenceEndDate")
      )

    val step2 = step1
      .join(conference, step1("ConferenceInstanceId") === conference("ConferenceInstanceId"), "left")
      .select(
        step1("*"),
        conference("conferenceName"),
        conference("conferenceLocation"),
        conference("conferenceStartDate"),
        conference("conferenceEndDate")
      )
      .cache()
    step2.count()
    step1.unpersist()

    val fos = MagUtility
      .loadMagEntity(spark, "FieldsOfStudy", magBasePath)
      .select($"FieldOfStudyId".alias("fos"), $"DisplayName", $"MainType")

    val paperFieldsOfStudy = MagUtility
      .loadMagEntity(spark, "PaperFieldsOfStudy", magBasePath)
      .select($"FieldOfStudyId", $"Score", $"PaperId")

    val paperFoS = paperFieldsOfStudy
      .join(broadcast(fos), fos("fos") === paperFieldsOfStudy("FieldOfStudyId"))
      .groupBy("PaperId")
      .agg(collect_set(struct("FieldOfStudyId", "DisplayName", "MainType", "Score")).as("FoS"))

    val step3 = step2
      .join(paperFoS, step2("PaperId") === paperFoS("PaperId"), "left")
      .select(step2("*"), paperFoS("FoS"))
      .cache()
    step3.count()

    step2.unpersist()

    val journals = MagUtility
      .loadMagEntity(spark, "Journals", magBasePath)
      .select(
        $"JournalId",
        $"DisplayName".as("journalName"),
        $"Issn".as("journalIssn"),
        $"Publisher".as("journalPublisher"),
        $"Webpage".as("journalWebpage")
      )
    step3
      .join(journals, step3("JournalId") === journals("JournalId"), "left")
      .select(
        step3("*"),
        journals("journalName"),
        journals("journalIssn"),
        journals("journalPublisher"),
        journals("journalWebpage")
      )
      .select(
        $"PaperId".as("paperId"),
        $"Rank".as("rank"),
        $"Doi".as("doi"),
        $"DocType".as("docType"),
        $"PaperTitle".as("paperTitle"),
        $"OriginalTitle".as("originalTitle"),
        $"BookTitle".as("bookTitle"),
        $"Year".as("year"),
        $"Date".as("date"),
        $"OnlineDate".as("onlineDate"),
        $"Publisher".as("publisher"),
        $"JournalId".as("journalId"),
        $"ConferenceSeriesId".as("conferenceSeriesId"),
        $"ConferenceInstanceId".as("conferenceInstanceId"),
        $"Volume".as("volume"),
        $"Issue".as("issue"),
        $"FirstPage".as("firstPage"),
        $"LastPage".as("lastPage"),
        $"ReferenceCount".as("referenceCount"),
        $"CitationCount".as("citationCount"),
        $"EstimatedCitation".as("estimatedCitation"),
        $"OriginalVenue".as("originalVenue"),
        $"FamilyId".as("familyId"),
        $"FamilyRank".as("familyRank"),
        $"DocSubTypes".as("docSubTypes"),
        $"CreatedDate".as("createdDate"),
        $"Abstract".as("abstractText"),
        $"authors".as("authors"),
        $"conferenceName".as("conferenceName"),
        $"conferenceLocation".as("conferenceLocation"),
        $"conferenceStartDate".as("conferenceStartDate"),
        $"conferenceEndDate".as("conferenceEndDate"),
        $"FoS".as("fos"),
        $"journalName".as("journalName"),
        $"journalIssn".as("journalIssn"),
        $"journalPublisher".as("journalPublisher"),
        $"journalWebpage".as("journalWebpage")
      )
      .write
      .mode("OverWrite")
      .save(s"$workingPath/mag")
    step3.unpersist()
  }
}

object SparkCreateMagDenormalizedTable {

  val log: Logger = LoggerFactory.getLogger(SparkCreateMagDenormalizedTable.getClass)

  def main(args: Array[String]): Unit = {
    new SparkCreateMagDenormalizedTable(
      "/eu/dnetlib/dhp/collection/mag/create_MAG_denormalized_table_properites.json",
      args,
      log
    )
  }
}
