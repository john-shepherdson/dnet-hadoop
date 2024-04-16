package eu.dnetlib.dhp.collection.mag

import com.fasterxml.jackson.databind.ObjectMapper
import eu.dnetlib.dhp.schema.action.AtomicAction
import eu.dnetlib.dhp.schema.common.ModelConstants
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils._
import eu.dnetlib.dhp.schema.oaf.utils.{OafMapperUtils, PidType}
import eu.dnetlib.dhp.schema.oaf.{Author, DataInfo, Instance, Journal, Organization, Publication, Relation, Result, Dataset => OafDataset}
import eu.dnetlib.dhp.utils.DHPUtils
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

  val mapper = new ObjectMapper()

  private val MAGCollectedFrom = keyValue(ModelConstants.MAG_ID, ModelConstants.MAG_NAME)

  private val MAGDataInfo: DataInfo = {
    val di = new DataInfo
    di.setDeletedbyinference(false)
    di.setInferred(false)
    di.setInvisible(false)
    di.setTrust("0.9")
    di.setProvenanceaction(
      OafMapperUtils.qualifier(
        ModelConstants.SYSIMPORT_ACTIONSET,
        ModelConstants.SYSIMPORT_ACTIONSET,
        ModelConstants.DNET_PROVENANCE_ACTIONS,
        ModelConstants.DNET_PROVENANCE_ACTIONS
      )
    )
    di
  }

  private val MAGDataInfoInvisible: DataInfo = {
    val di = new DataInfo
    di.setDeletedbyinference(false)
    di.setInferred(false)
    di.setInvisible(true)
    di.setTrust("0.9")
    di.setProvenanceaction(
      OafMapperUtils.qualifier(
        ModelConstants.SYSIMPORT_ACTIONSET,
        ModelConstants.SYSIMPORT_ACTIONSET,
        ModelConstants.DNET_PROVENANCE_ACTIONS,
        ModelConstants.DNET_PROVENANCE_ACTIONS
      )
    )
    di
  }

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

  val stream: Map[String, (String, Seq[String])] = Map(
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
    d.foreach { t =>
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

  def createResultFromType(magType: Option[String], source: Option[String]): Result = {
    var result: Result = null

    if (magType == null || magType.orNull == null) {
      result = new Publication
      result.setDataInfo(MAGDataInfo)
      val i = new Instance
      i.setInstancetype(
        qualifier(
          "0038",
          "Other literature type",
          ModelConstants.DNET_PUBLICATION_RESOURCE,
          ModelConstants.DNET_PUBLICATION_RESOURCE
        )
      )

      result.setInstance(List(i).asJava)
      return result
    }

    val currentType: String = magType.get

    val tp = currentType.toLowerCase match {
      case "book" =>
        result = new Publication
        qualifier("0002", "Book", ModelConstants.DNET_PUBLICATION_RESOURCE, ModelConstants.DNET_PUBLICATION_RESOURCE)
      case "bookchapter" =>
        result = new Publication
        qualifier(
          "00013",
          "Part of book or chapter of book",
          ModelConstants.DNET_PUBLICATION_RESOURCE,
          ModelConstants.DNET_PUBLICATION_RESOURCE
        )
      case "journal" =>
        result = new Publication
        qualifier("0043", "Journal", ModelConstants.DNET_PUBLICATION_RESOURCE, ModelConstants.DNET_PUBLICATION_RESOURCE)
      case "patent" =>
        if (source != null && source.orNull != null) {
          val s = source.get.toLowerCase
          if (s.contains("patent") || s.contains("brevet")) {
            result = new Publication
            qualifier(
              "0019",
              "Patent",
              ModelConstants.DNET_PUBLICATION_RESOURCE,
              ModelConstants.DNET_PUBLICATION_RESOURCE
            )
          } else if (s.contains("journal of")) {
            result = new Publication
            qualifier(
              "0043",
              "Journal",
              ModelConstants.DNET_PUBLICATION_RESOURCE,
              ModelConstants.DNET_PUBLICATION_RESOURCE
            )
          } else if (
            s.contains("proceedings") || s.contains("conference") || s.contains("workshop") || s.contains(
              "symposium"
            )
          ) {
            result = new Publication
            qualifier(
              "0001",
              "Article",
              ModelConstants.DNET_PUBLICATION_RESOURCE,
              ModelConstants.DNET_PUBLICATION_RESOURCE
            )
          } else null
        } else null

      case "repository" =>
        result = new Publication()
        result.setDataInfo(MAGDataInfoInvisible)
        qualifier(
          "0038",
          "Other literature type",
          ModelConstants.DNET_PUBLICATION_RESOURCE,
          ModelConstants.DNET_PUBLICATION_RESOURCE
        )

      case "thesis" =>
        result = new Publication
        qualifier(
          "0044",
          "Thesis",
          ModelConstants.DNET_PUBLICATION_RESOURCE,
          ModelConstants.DNET_PUBLICATION_RESOURCE
        )
      case "dataset" =>
        result = new OafDataset
        qualifier(
          "0021",
          "Dataset",
          ModelConstants.DNET_PUBLICATION_RESOURCE,
          ModelConstants.DNET_PUBLICATION_RESOURCE
        )
      case "conference" =>
        result = new Publication
        qualifier(
          "0001",
          "Article",
          ModelConstants.DNET_PUBLICATION_RESOURCE,
          ModelConstants.DNET_PUBLICATION_RESOURCE
        )
    }

    if (result != null) {
      if (result.getDataInfo == null)
        result.setDataInfo(MAGDataInfo)
      val i = new Instance
      i.setInstancetype(tp)
      i.setInstanceTypeMapping(
        List(instanceTypeMapping(currentType, ModelConstants.OPENAIRE_COAR_RESOURCE_TYPES_3_1)).asJava
      )
      result.setInstance(List(i).asJava)
    }
    result

  }

  def convertMAGtoOAF(paper: MAGPaper): String = {

    // FILTER all the  MAG paper with no URL
    if (paper.urls.orNull == null)
      return null

    val result = createResultFromType(paper.docType, paper.originalVenue)
    if (result == null)
      return null

    result.setCollectedfrom(List(MAGCollectedFrom).asJava)
    val pidList = List(
      structuredProperty(
        paper.paperId.get.toString,
        qualifier(
          PidType.mag_id.toString,
          PidType.mag_id.toString,
          ModelConstants.DNET_PID_TYPES,
          ModelConstants.DNET_PID_TYPES
        ),
        null
      )
    )

    result.setPid(pidList.asJava)

    result.setOriginalId(pidList.map(s => s.getValue).asJava)

    result.setId(s"50|mag_________::${DHPUtils.md5(paper.paperId.get.toString)}")

    val originalTitles = structuredProperty(paper.paperTitle.get, ModelConstants.MAIN_TITLE_QUALIFIER, null)

    result.setTitle(List(originalTitles).asJava)

    if (paper.date.orNull != null) {
      result.setDateofacceptance(field(paper.date.get, null))
    } else {
      if (paper.year.isDefined && paper.year.get > 1700) {
        result.setDateofacceptance(field(s"${paper.year.get}-01-01", null))
      }
    }

    if (paper.onlineDate.orNull != null) {
      result.setRelevantdate(
        List(
          structuredProperty(
            paper.onlineDate.get,
            qualifier(
              "published-online",
              "published-online",
              ModelConstants.DNET_DATACITE_DATE,
              ModelConstants.DNET_DATACITE_DATE
            ),
            null
          )
        ).asJava
      )
    }

    if (paper.publisher.orNull != null) {
      result.setPublisher(field(paper.publisher.get, null))
    }

    if (paper.date.isDefined)
      result.setDateofacceptance(field(paper.date.get, null))
    if (paper.onlineDate.orNull != null)
      result.setRelevantdate(
        List(
          structuredProperty(
            paper.onlineDate.get,
            qualifier(
              "published-online",
              "published-online",
              ModelConstants.DNET_DATACITE_DATE,
              ModelConstants.DNET_DATACITE_DATE
            ),
            null
          )
        ).asJava
      )

    if (paper.publisher.isDefined)
      result.setPublisher(field(paper.publisher.get, null))

    if (paper.journalId.isDefined && paper.journalName.isDefined) {
      val j = new Journal

      j.setName(paper.journalName.get)
      j.setSp(paper.firstPage.orNull)
      j.setEp(paper.lastPage.orNull)
      if (paper.publisher.isDefined)
        result.setPublisher(field(paper.publisher.get, null))
      j.setIssnPrinted(paper.journalIssn.orNull)
      j.setVol(paper.volume.orNull)
      j.setIss(paper.issue.orNull)
      j.setConferenceplace(paper.conferenceLocation.orNull)
      result match {
        case publication: Publication => publication.setJournal(j)
      }
    }

    if (paper.abstractText.isDefined)
      result.setDescription(List(field(paper.abstractText.get, null)).asJava)
    if (paper.authors.isDefined && paper.authors.get.nonEmpty) {
      result.setAuthor(
        paper.authors.get
          .filter(a => a.AuthorName.isDefined)
          .map(a => {
            val author = new Author
            author.setFullname(a.AuthorName.get)
            author
          })
          .asJava
      )
    }

    val instance = result.getInstance().get(0)
    instance.setPid(pidList.asJava)
    if (paper.doi.orNull != null)
      instance.setAlternateIdentifier(
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
    instance.setUrl(paper.urls.get.asJava)
    instance.setHostedby(ModelConstants.UNKNOWN_REPOSITORY)
    instance.setCollectedfrom(MAGCollectedFrom)
    instance.setAccessright(
      accessRight(
        ModelConstants.UNKNOWN,
        ModelConstants.NOT_AVAILABLE,
        ModelConstants.DNET_ACCESS_MODES,
        ModelConstants.DNET_ACCESS_MODES
      )
    )

    if (paper.authors.orNull != null && paper.authors.get.nonEmpty)
      result.setAuthor(
        paper.authors.get
          .filter(a => a.AuthorName.orNull != null)
          .map { a =>
            val author = new Author
            author.setFullname(a.AuthorName.get)
            var authorPid = List(
              structuredProperty(
                a.AuthorId.get.toString,
                qualifier(
                  PidType.mag_id.toString,
                  PidType.mag_id.toString,
                  ModelConstants.DNET_PID_TYPES,
                  ModelConstants.DNET_PID_TYPES
                ),
                null
              )
            )
            if (a.GridId.orNull != null) {
              authorPid = authorPid ::: List(
                structuredProperty(
                  a.AuthorId.get.toString,
                  qualifier(
                    PidType.mag_id.toString,
                    PidType.mag_id.toString,
                    ModelConstants.DNET_PID_TYPES,
                    ModelConstants.DNET_PID_TYPES
                  ),
                  null
                )
              )
            }
            author.setPid(authorPid.asJava)
            author
          }
          .asJava
      )
    mapper.writeValueAsString(result)

  }

  def generateOrganization(r: Row): String = {

    val o = new Organization
    val affId = s"20|mag_________::${DHPUtils.md5(r.getAs[Long]("AffiliationId").toString)}"
    o.setId(affId)
    o.setDataInfo(MAGDataInfo)
    o.setCollectedfrom(List(MAGCollectedFrom).asJava)
    o.setLegalname(field(r.getAs[String]("DisplayName"), null))
    val gid = r.getAs[String]("GridId")
    if (gid != null) {
      o.setPid(List(
        structuredProperty(gid, qualifier(
          PidType.GRID.toString,
          PidType.GRID.toString,
          ModelConstants.DNET_PID_TYPES,
          ModelConstants.DNET_PID_TYPES
        ),
          null),
        structuredProperty(r.getAs[Long]("AffiliationId").toString, qualifier(
          PidType.mag_id.toString,
          PidType.mag_id.toString,
          ModelConstants.DNET_PID_TYPES,
          ModelConstants.DNET_PID_TYPES
        ),
          null)

      ).asJava)
    } else {
      o.setPid(List(
        structuredProperty(r.getAs[Long]("AffiliationId").toString, qualifier(
          PidType.mag_id.toString,
          PidType.mag_id.toString,
          ModelConstants.DNET_PID_TYPES,
          ModelConstants.DNET_PID_TYPES
        ),
          null)
      ).asJava)
    }
    val c = r.getAs[String]("Iso3166Code")
    if (c != null)
      o.setCountry(qualifier(c, c, "dnet:countries", "dnet:countries"))
    else
      o.setCountry(ModelConstants.UNKNOWN_COUNTRY)
    val ws = r.getAs[String]("OfficialPage")
    if (ws != null)
      o.setWebsiteurl(field(ws, null))
    val a = new AtomicAction[Organization]()
    a.setClazz(classOf[Organization])
    a.setPayload(o)
    mapper.writeValueAsString(a)
  }

  def generateAffiliationRelations(paperAffiliation: Row): List[Relation] = {

    val affId = s"20|mag_________::${DHPUtils.md5(paperAffiliation.getAs[Long]("AffiliationId").toString)}"
    val oafId = s"50|mag_________::${DHPUtils.md5(paperAffiliation.getAs[Long]("PaperId").toString)}"
    val r: Relation = new Relation
    r.setSource(oafId)
    r.setTarget(affId)
    r.setRelType(ModelConstants.RESULT_ORGANIZATION)
    r.setRelClass(ModelConstants.HAS_AUTHOR_INSTITUTION)
    r.setSubRelType(ModelConstants.AFFILIATION)
    r.setDataInfo(MAGDataInfo)
    r.setCollectedfrom(List(MAGCollectedFrom).asJava)
    val r1: Relation = new Relation
    r1.setTarget(oafId)
    r1.setSource(affId)
    r1.setRelType(ModelConstants.RESULT_ORGANIZATION)
    r1.setRelClass(ModelConstants.IS_AUTHOR_INSTITUTION_OF)
    r1.setSubRelType(ModelConstants.AFFILIATION)
    r1.setDataInfo(MAGDataInfo)
    r1.setCollectedfrom(List(MAGCollectedFrom).asJava)
    List(r, r1)

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
