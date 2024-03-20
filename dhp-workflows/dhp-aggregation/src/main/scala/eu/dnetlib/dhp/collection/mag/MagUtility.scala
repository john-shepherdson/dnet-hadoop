package eu.dnetlib.dhp.collection.mag

import eu.dnetlib.dhp.schema.common.ModelConstants
import eu.dnetlib.dhp.schema.oaf.{Author, Journal, Publication}
import eu.dnetlib.dhp.schema.oaf.utils.{IdentifierFactory, PidType}
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.json4s
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

import scala.collection.JavaConverters._

case class MAGPaper(
  paperId: Option[Long],
  doi: Option[String],
  docType: Option[String],
  paperTitle: Option[String],
  originalTitle: Option[String],
  bookTitle: Option[String],
  year: Option[Int],
  date: Option[String],
  onlineDate: Option[String],
  publisher: Option[String],
  // Journal or Conference information (one will be populated)
  journalId: Option[Long],
  journalName: Option[String],
  journalIssn: Option[String],
  journalPublisher: Option[String],
  conferenceSeriesId: Option[Long],
  conferenceInstanceId: Option[Long],
  conferenceName: Option[String],
  conferenceLocation: Option[String],
  conferenceStartDate: Option[String],
  conferenceEndDate: Option[String],
  volume: Option[String],
  issue: Option[String],
  firstPage: Option[String],
  lastPage: Option[String],
  referenceCount: Option[Long],
  citationCount: Option[Long],
  estimatedCitation: Option[Long],
  originalVenue: Option[String],
  familyId: Option[Long],
  familyRank: Option[Int],
  docSubTypes: Option[String],
  createdDate: Option[String],
  abstractText: Option[String],
  // List of authors
  authors: Option[List[MAGAuthor]],
  urls: Option[List[String]]
)

case class MAGAuthor(
  AffiliationId: Option[Long],
  AuthorSequenceNumber: Option[Int],
  AffiliationName: Option[String],
  AuthorName: Option[String],
  AuthorId: Option[Long],
  GridId: Option[String]
)

object MagUtility extends Serializable {

  val datatypedict = Map(
    "bool"     -> BooleanType,
    "int"      -> IntegerType,
    "uint"     -> IntegerType,
    "long"     -> LongType,
    "ulong"    -> LongType,
    "float"    -> FloatType,
    "string"   -> StringType,
    "DateTime" -> DateType
  )

  val stream = Map(
    "Affiliations" -> Tuple2(
      "mag/Affiliations.txt",
      Seq(
        "AffiliationId:long",
        "Rank:uint",
        "NormalizedName:string",
        "DisplayName:string",
        "GridId:string",
        "OfficialPage:string",
        "WikiPage:string",
        "PaperCount:long",
        "PaperFamilyCount:long",
        "CitationCount:long",
        "Iso3166Code:string",
        "Latitude:float?",
        "Longitude:float?",
        "CreatedDate:DateTime"
      )
    ),
    "AuthorExtendedAttributes" -> Tuple2(
      "mag/AuthorExtendedAttributes.txt",
      Seq("AuthorId:long", "AttributeType:int", "AttributeValue:string")
    ),
    "Authors" -> Tuple2(
      "mag/Authors.txt",
      Seq(
        "AuthorId:long",
        "Rank:uint",
        "NormalizedName:string",
        "DisplayName:string",
        "LastKnownAffiliationId:long?",
        "PaperCount:long",
        "PaperFamilyCount:long",
        "CitationCount:long",
        "CreatedDate:DateTime"
      )
    ),
    "ConferenceInstances" -> Tuple2(
      "mag/ConferenceInstances.txt",
      Seq(
        "ConferenceInstanceId:long",
        "NormalizedName:string",
        "DisplayName:string",
        "ConferenceSeriesId:long",
        "Location:string",
        "OfficialUrl:string",
        "StartDate:DateTime?",
        "EndDate:DateTime?",
        "AbstractRegistrationDate:DateTime?",
        "SubmissionDeadlineDate:DateTime?",
        "NotificationDueDate:DateTime?",
        "FinalVersionDueDate:DateTime?",
        "PaperCount:long",
        "PaperFamilyCount:long",
        "CitationCount:long",
        "Latitude:float?",
        "Longitude:float?",
        "CreatedDate:DateTime"
      )
    ),
    "ConferenceSeries" -> Tuple2(
      "mag/ConferenceSeries.txt",
      Seq(
        "ConferenceSeriesId:long",
        "Rank:uint",
        "NormalizedName:string",
        "DisplayName:string",
        "PaperCount:long",
        "PaperFamilyCount:long",
        "CitationCount:long",
        "CreatedDate:DateTime"
      )
    ),
    "EntityRelatedEntities" -> Tuple2(
      "advanced/EntityRelatedEntities.txt",
      Seq(
        "EntityId:long",
        "EntityType:string",
        "RelatedEntityId:long",
        "RelatedEntityType:string",
        "RelatedType:int",
        "Score:float"
      )
    ),
    "FieldOfStudyChildren" -> Tuple2(
      "advanced/FieldOfStudyChildren.txt",
      Seq("FieldOfStudyId:long", "ChildFieldOfStudyId:long")
    ),
    "FieldOfStudyExtendedAttributes" -> Tuple2(
      "advanced/FieldOfStudyExtendedAttributes.txt",
      Seq("FieldOfStudyId:long", "AttributeType:int", "AttributeValue:string")
    ),
    "FieldsOfStudy" -> Tuple2(
      "advanced/FieldsOfStudy.txt",
      Seq(
        "FieldOfStudyId:long",
        "Rank:uint",
        "NormalizedName:string",
        "DisplayName:string",
        "MainType:string",
        "Level:int",
        "PaperCount:long",
        "PaperFamilyCount:long",
        "CitationCount:long",
        "CreatedDate:DateTime"
      )
    ),
    "Journals" -> Tuple2(
      "mag/Journals.txt",
      Seq(
        "JournalId:long",
        "Rank:uint",
        "NormalizedName:string",
        "DisplayName:string",
        "Issn:string",
        "Publisher:string",
        "Webpage:string",
        "PaperCount:long",
        "PaperFamilyCount:long",
        "CitationCount:long",
        "CreatedDate:DateTime"
      )
    ),
    "PaperAbstractsInvertedIndex" -> Tuple2(
      "nlp/PaperAbstractsInvertedIndex.txt.*",
      Seq("PaperId:long", "IndexedAbstract:string")
    ),
    "PaperAuthorAffiliations" -> Tuple2(
      "mag/PaperAuthorAffiliations.txt",
      Seq(
        "PaperId:long",
        "AuthorId:long",
        "AffiliationId:long?",
        "AuthorSequenceNumber:uint",
        "OriginalAuthor:string",
        "OriginalAffiliation:string"
      )
    ),
    "PaperCitationContexts" -> Tuple2(
      "nlp/PaperCitationContexts.txt",
      Seq("PaperId:long", "PaperReferenceId:long", "CitationContext:string")
    ),
    "PaperExtendedAttributes" -> Tuple2(
      "mag/PaperExtendedAttributes.txt",
      Seq("PaperId:long", "AttributeType:int", "AttributeValue:string")
    ),
    "PaperFieldsOfStudy" -> Tuple2(
      "advanced/PaperFieldsOfStudy.txt",
      Seq("PaperId:long", "FieldOfStudyId:long", "Score:float")
    ),
    "PaperMeSH" -> Tuple2(
      "advanced/PaperMeSH.txt",
      Seq(
        "PaperId:long",
        "DescriptorUI:string",
        "DescriptorName:string",
        "QualifierUI:string",
        "QualifierName:string",
        "IsMajorTopic:bool"
      )
    ),
    "PaperRecommendations" -> Tuple2(
      "advanced/PaperRecommendations.txt",
      Seq("PaperId:long", "RecommendedPaperId:long", "Score:float")
    ),
    "PaperReferences" -> Tuple2(
      "mag/PaperReferences.txt",
      Seq("PaperId:long", "PaperReferenceId:long")
    ),
    "PaperResources" -> Tuple2(
      "mag/PaperResources.txt",
      Seq(
        "PaperId:long",
        "ResourceType:int",
        "ResourceUrl:string",
        "SourceUrl:string",
        "RelationshipType:int"
      )
    ),
    "PaperUrls" -> Tuple2(
      "mag/PaperUrls.txt",
      Seq("PaperId:long", "SourceType:int?", "SourceUrl:string", "LanguageCode:string")
    ),
    "Papers" -> Tuple2(
      "mag/Papers.txt",
      Seq(
        "PaperId:long",
        "Rank:uint",
        "Doi:string",
        "DocType:string",
        "PaperTitle:string",
        "OriginalTitle:string",
        "BookTitle:string",
        "Year:int?",
        "Date:DateTime?",
        "OnlineDate:DateTime?",
        "Publisher:string",
        "JournalId:long?",
        "ConferenceSeriesId:long?",
        "ConferenceInstanceId:long?",
        "Volume:string",
        "Issue:string",
        "FirstPage:string",
        "LastPage:string",
        "ReferenceCount:long",
        "CitationCount:long",
        "EstimatedCitation:long",
        "OriginalVenue:string",
        "FamilyId:long?",
        "FamilyRank:uint?",
        "DocSubTypes:string",
        "CreatedDate:DateTime"
      )
    ),
    "RelatedFieldOfStudy" -> Tuple2(
      "advanced/RelatedFieldOfStudy.txt",
      Seq(
        "FieldOfStudyId1:long",
        "Type1:string",
        "FieldOfStudyId2:long",
        "Type2:string",
        "Rank:float"
      )
    )
  )

  def getSchema(streamName: String): StructType = {
    var schema = new StructType()
    val d: Seq[String] = stream(streamName)._2
    d.foreach { case t =>
      val currentType = t.split(":")
      val fieldName: String = currentType.head
      var fieldType: String = currentType.last
      val nullable: Boolean = fieldType.endsWith("?")
      if (nullable)
        fieldType = fieldType.replace("?", "")
      schema = schema.add(StructField(fieldName, datatypedict(fieldType), nullable))
    }
    schema
  }

  def loadMagEntity(spark: SparkSession, entity: String, basePath: String): Dataset[Row] = {
    if (stream.contains(entity)) {
      val s = getSchema(entity)
      val pt = stream(entity)._1
      spark.read
        .option("header", "false")
        .option("charset", "UTF8")
        .option("delimiter", "\t")
        .schema(s)
        .csv(s"$basePath/$pt")
    } else
      null

  }

  def convertMAGtoOAF(paper: MAGPaper): Publication = {

    if (paper.doi.isDefined) {
      val pub = new Publication
      pub.setPid(
        List(
          structuredProperty(
            paper.doi.get,
            qualifier(
              PidType.doi.toString,
              PidType.doi.toString,
              ModelConstants.DNET_PID_TYPES,
              ModelConstants.DNET_PID_TYPES
            ),
            null
          )
        ).asJava
      )

      pub.setOriginalId(List(paper.paperId.get.toString, paper.doi.get).asJava)

      //IMPORTANT
      //The old method result.setId(generateIdentifier(result, doi))
      //will be replaced using IdentifierFactory
      pub.setId(IdentifierFactory.createDOIBoostIdentifier(pub))

      val mainTitles = structuredProperty(paper.originalTitle.get, ModelConstants.MAIN_TITLE_QUALIFIER, null)

      val originalTitles = structuredProperty(paper.paperTitle.get, ModelConstants.ALTERNATIVE_TITLE_QUALIFIER, null)

      pub.setTitle(List(mainTitles, originalTitles).asJava)

      if (paper.bookTitle.isDefined)
        pub.setSource(List(field[String](paper.bookTitle.get, null)).asJava)
      if (paper.abstractText.isDefined)
        pub.setDescription(List(field(paper.abstractText.get, null)).asJava)
      if (paper.authors.isDefined && paper.authors.get.nonEmpty) {
        pub.setAuthor(
          paper.authors.get
            .filter(a => a.AuthorName.isDefined)
            .map(a => {
              val author = new Author
              author.setFullname(a.AuthorName.get)
              if (a.AffiliationName.isDefined)
                author.setAffiliation(List(field(a.AffiliationName.get, null)).asJava)
              author.setPid(
                List(
                  structuredProperty(
                    s"https://academic.microsoft.com/#/detail/${a.AuthorId.get}",
                    qualifier("url", "url", ModelConstants.DNET_PID_TYPES, ModelConstants.DNET_PID_TYPES),
                    null
                  )
                ).asJava
              )
              author
            })
            .asJava
        )

      }

      if (paper.date.isDefined)
        pub.setDateofacceptance(field(paper.date.get, null))

      if (paper.publisher.isDefined)
        pub.setPublisher(field(paper.publisher.get, null))

      if (paper.journalId.isDefined && paper.journalName.isDefined) {
        val j = new Journal

        j.setName(paper.journalName.get)
        j.setSp(paper.firstPage.orNull)
        j.setEp(paper.lastPage.orNull)
        if (paper.publisher.isDefined)
          pub.setPublisher(field(paper.publisher.get, null))
        j.setIssnPrinted(paper.journalIssn.orNull)
        j.setVol(paper.volume.orNull)
        j.setIss(paper.issue.orNull)
        j.setConferenceplace(paper.conferenceLocation.orNull)
        j.setEdition(paper.conferenceName.orNull)
        pub.setJournal(j)
      }

      pub
    } else {
      null
    }

  }

  def convertInvertedIndexString(json_input: String): String = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json: json4s.JValue = parse(json_input)
    val idl = (json \ "IndexLength").extract[Int]
    if (idl > 0) {
      val res = Array.ofDim[String](idl)

      val iid = (json \ "InvertedIndex").extract[Map[String, List[Int]]]

      for { (k: String, v: List[Int]) <- iid } {
        v.foreach(item => res(item) = k)
      }
      (0 until idl).foreach(i => {
        if (res(i) == null)
          res(i) = ""
      })
      return res.mkString(" ")
    }
    ""
  }

}
