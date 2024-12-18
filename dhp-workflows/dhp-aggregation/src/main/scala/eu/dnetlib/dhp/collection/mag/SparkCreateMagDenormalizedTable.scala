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
    val magBasePath: String = parser.get("magBasePath")
    log.info("found parameters magBasePath: {}", magBasePath)
    generatedDenormalizedMAGTable(spark, magBasePath)
  }

  private def generatedDenormalizedMAGTable(
    spark: SparkSession,
    magBasePath: String
  ): Unit = {

    import spark.implicits._
    val schema: StructType = StructType(StructField("DOI", StringType) :: Nil)

    //Filter all the MAG Papers that intersect with a Crossref DOI

    val magPapers = MagUtility
      .loadMagEntity(spark, "Papers", magBasePath)
      .withColumn("Doi", lower(col("Doi")))

    magPapers.cache()
    magPapers.count()
    //log.info("Create current abstract")

    //Abstract is an inverted list, we define a function that convert in string the abstract and recreate
    // a table(PaperId, Abstract)
    val paperAbstract = MagUtility
      .loadMagEntity(spark, "PaperAbstractsInvertedIndex", magBasePath)
      .map(s => (s.getLong(0), MagUtility.convertInvertedIndexString(s.getString(1))))
      .withColumnRenamed("_1", "PaperId")
      .withColumnRenamed("_2", "Abstract")

    //We define Step0 as the result of the  Join between PaperIntersection and the PaperAbstract

    val step0 = magPapers
      .join(paperAbstract, magPapers("PaperId") === paperAbstract("PaperId"), "left")
      .select(magPapers("*"), paperAbstract("Abstract"))
      .cache()

    step0.count()

    magPapers.unpersist()

    // We have three table Author, Affiliation, and PaperAuthorAffiliation, in the
    //next step we create a table containing
    val authors = MagUtility.loadMagEntity(spark, "Authors", magBasePath)
    val affiliations = MagUtility.loadMagEntity(spark, "Affiliations", magBasePath)
    val paperAuthorAffiliations = MagUtility.loadMagEntity(spark, "PaperAuthorAffiliations", magBasePath)

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

    val journals = MagUtility
      .loadMagEntity(spark, "Journals", magBasePath)
      .select(
        $"JournalId",
        $"DisplayName".as("journalName"),
        $"Issn".as("journalIssn"),
        $"Publisher".as("journalPublisher")
      )
    val step3 = step2
      .join(journals, step2("JournalId") === journals("JournalId"), "left")
      .select(
        step2("*"),
        journals("journalName"),
        journals("journalIssn"),
        journals("journalPublisher")
      )
      .cache
    step3.count()

    val paper_urls = MagUtility
      .loadMagEntity(spark, "PaperUrls", magBasePath)
      .groupBy("PaperId")
      .agg(slice(collect_set("SourceUrl"), 1, 6).alias("urls"))
      .cache

    paper_urls.count

    step3
      .join(paper_urls, step3("PaperId") === paper_urls("PaperId"))
      .select(step3("*"), paper_urls("urls"))
      .select(
        $"PaperId".as("paperId"),
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
        $"journalName".as("journalName"),
        $"journalIssn".as("journalIssn"),
        $"journalPublisher".as("journalPublisher"),
        $"urls"
      )
      .write
      .mode("OverWrite")
      .save(s"$magBasePath/mag_denormalized")
    step3.unpersist()
  }
}

object SparkCreateMagDenormalizedTable {

  val log: Logger = LoggerFactory.getLogger(SparkCreateMagDenormalizedTable.getClass)

  def main(args: Array[String]): Unit = {
    new SparkCreateMagDenormalizedTable(
      "/eu/dnetlib/dhp/collection/mag/create_MAG_denormalized_table_properties.json",
      args,
      log
    ).initialize().run()
  }
}
