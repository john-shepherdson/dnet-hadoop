package eu.dnetlib.dhp.datacite

import eu.dnetlib.dhp.schema.common.ModelConstants
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils
import eu.dnetlib.dhp.schema.oaf.{DataInfo, KeyValue}

import java.io.InputStream
import java.time.format.DateTimeFormatter
import java.util.Locale
import java.util.regex.Pattern
import scala.io.Source

/** This class represent the dataModel of the input Dataset of Datacite
  * @param doi THE DOI
  * @param timestamp timestamp of last update date
  * @param isActive the record is active or deleted
  * @param json the json native records
  */
case class DataciteType(doi: String, timestamp: Long, isActive: Boolean, json: String) {}

/*
  The following class are utility class used for the mapping from
  json datacite to OAF Shema
 */
case class RelatedIdentifierType(
  relationType: String,
  relatedIdentifier: String,
  relatedIdentifierType: String
) {}

case class NameIdentifiersType(
  nameIdentifierScheme: Option[String],
  schemeUri: Option[String],
  nameIdentifier: Option[String]
) {}

case class CreatorType(
  nameType: Option[String],
  nameIdentifiers: Option[List[NameIdentifiersType]],
  name: Option[String],
  familyName: Option[String],
  givenName: Option[String],
  affiliation: Option[List[String]]
) {}

case class TitleType(title: Option[String], titleType: Option[String], lang: Option[String]) {}

case class SubjectType(subject: Option[String], subjectScheme: Option[String]) {}

case class DescriptionType(descriptionType: Option[String], description: Option[String]) {}

case class FundingReferenceType(
  funderIdentifierType: Option[String],
  awardTitle: Option[String],
  awardUri: Option[String],
  funderName: Option[String],
  funderIdentifier: Option[String],
  awardNumber: Option[String]
) {}

case class DateType(date: Option[String], dateType: Option[String]) {}

case class OAFRelations(relation: String, inverse: String, relType: String)

class DataciteModelConstants extends Serializable {}

object DataciteModelConstants {

  val REL_TYPE_VALUE: String = "resultResult"
  val DATE_RELATION_KEY = "RelationDate"
  val DATACITE_FILTER_PATH = "/eu/dnetlib/dhp/datacite/datacite_filter"
  val DOI_CLASS = "doi"
  val SUBJ_CLASS = "keywords"
  val DATACITE_NAME = "Datacite"
  val dataInfo: DataInfo = dataciteDataInfo("0.9")

  val DATACITE_COLLECTED_FROM: KeyValue =
    OafMapperUtils.keyValue(ModelConstants.DATACITE_ID, DATACITE_NAME)

  val subRelTypeMapping: Map[String, OAFRelations] = Map(
    ModelConstants.IS_SUPPLEMENTED_BY -> OAFRelations(
      ModelConstants.IS_SUPPLEMENTED_BY,
      ModelConstants.IS_SUPPLEMENT_TO,
      ModelConstants.SUPPLEMENT
    ),
    ModelConstants.IS_SUPPLEMENT_TO -> OAFRelations(
      ModelConstants.IS_SUPPLEMENT_TO,
      ModelConstants.IS_SUPPLEMENTED_BY,
      ModelConstants.SUPPLEMENT
    ),
    ModelConstants.HAS_PART -> OAFRelations(
      ModelConstants.HAS_PART,
      ModelConstants.IS_PART_OF,
      ModelConstants.PART
    ),
    ModelConstants.IS_PART_OF -> OAFRelations(
      ModelConstants.IS_PART_OF,
      ModelConstants.HAS_PART,
      ModelConstants.PART
    ),
    ModelConstants.IS_VERSION_OF -> OAFRelations(
      ModelConstants.IS_VERSION_OF,
      ModelConstants.HAS_VERSION,
      ModelConstants.VERSION
    ),
    ModelConstants.HAS_VERSION -> OAFRelations(
      ModelConstants.HAS_VERSION,
      ModelConstants.IS_VERSION_OF,
      ModelConstants.VERSION
    ),
    ModelConstants.IS_IDENTICAL_TO -> OAFRelations(
      ModelConstants.IS_IDENTICAL_TO,
      ModelConstants.IS_IDENTICAL_TO,
      ModelConstants.RELATIONSHIP
    ),
    ModelConstants.IS_CONTINUED_BY -> OAFRelations(
      ModelConstants.IS_CONTINUED_BY,
      ModelConstants.CONTINUES,
      ModelConstants.RELATIONSHIP
    ),
    ModelConstants.CONTINUES -> OAFRelations(
      ModelConstants.CONTINUES,
      ModelConstants.IS_CONTINUED_BY,
      ModelConstants.RELATIONSHIP
    ),
    ModelConstants.IS_NEW_VERSION_OF -> OAFRelations(
      ModelConstants.IS_NEW_VERSION_OF,
      ModelConstants.IS_PREVIOUS_VERSION_OF,
      ModelConstants.VERSION
    ),
    ModelConstants.IS_PREVIOUS_VERSION_OF -> OAFRelations(
      ModelConstants.IS_PREVIOUS_VERSION_OF,
      ModelConstants.IS_NEW_VERSION_OF,
      ModelConstants.VERSION
    ),
    ModelConstants.IS_DOCUMENTED_BY -> OAFRelations(
      ModelConstants.IS_DOCUMENTED_BY,
      ModelConstants.DOCUMENTS,
      ModelConstants.RELATIONSHIP
    ),
    ModelConstants.DOCUMENTS -> OAFRelations(
      ModelConstants.DOCUMENTS,
      ModelConstants.IS_DOCUMENTED_BY,
      ModelConstants.RELATIONSHIP
    ),
    ModelConstants.IS_SOURCE_OF -> OAFRelations(
      ModelConstants.IS_SOURCE_OF,
      ModelConstants.IS_DERIVED_FROM,
      ModelConstants.VERSION
    ),
    ModelConstants.IS_DERIVED_FROM -> OAFRelations(
      ModelConstants.IS_DERIVED_FROM,
      ModelConstants.IS_SOURCE_OF,
      ModelConstants.VERSION
    ),
    ModelConstants.IS_VARIANT_FORM_OF -> OAFRelations(
      ModelConstants.IS_VARIANT_FORM_OF,
      ModelConstants.IS_DERIVED_FROM,
      ModelConstants.VERSION
    ),
    ModelConstants.IS_OBSOLETED_BY -> OAFRelations(
      ModelConstants.IS_OBSOLETED_BY,
      ModelConstants.IS_NEW_VERSION_OF,
      ModelConstants.VERSION
    ),
    ModelConstants.REVIEWS -> OAFRelations(
      ModelConstants.REVIEWS,
      ModelConstants.IS_REVIEWED_BY,
      ModelConstants.REVIEW
    ),
    ModelConstants.IS_REVIEWED_BY -> OAFRelations(
      ModelConstants.IS_REVIEWED_BY,
      ModelConstants.REVIEWS,
      ModelConstants.REVIEW
    ),
    ModelConstants.DOCUMENTS -> OAFRelations(
      ModelConstants.DOCUMENTS,
      ModelConstants.IS_DOCUMENTED_BY,
      ModelConstants.RELATIONSHIP
    ),
    ModelConstants.IS_DOCUMENTED_BY -> OAFRelations(
      ModelConstants.IS_DOCUMENTED_BY,
      ModelConstants.DOCUMENTS,
      ModelConstants.RELATIONSHIP
    ),
    ModelConstants.COMPILES -> OAFRelations(
      ModelConstants.COMPILES,
      ModelConstants.IS_COMPILED_BY,
      ModelConstants.RELATIONSHIP
    ),
    ModelConstants.IS_COMPILED_BY -> OAFRelations(
      ModelConstants.IS_COMPILED_BY,
      ModelConstants.COMPILES,
      ModelConstants.RELATIONSHIP
    )
  )

  val datacite_filter: List[String] = {
    val stream: InputStream = getClass.getResourceAsStream(DATACITE_FILTER_PATH)
    require(stream != null)
    Source.fromInputStream(stream).getLines().toList
  }

  def dataciteDataInfo(trust: String): DataInfo = OafMapperUtils.dataInfo(
    false,
    null,
    false,
    false,
    ModelConstants.PROVENANCE_ACTION_SET_QUALIFIER,
    trust
  )

  val df_en: DateTimeFormatter = DateTimeFormatter.ofPattern(
    "[MM-dd-yyyy][MM/dd/yyyy][dd-MM-yy][dd-MMM-yyyy][dd/MMM/yyyy][dd-MMM-yy][dd/MMM/yy][dd-MM-yy][dd/MM/yy][dd-MM-yyyy][dd/MM/yyyy][yyyy-MM-dd][yyyy/MM/dd]",
    Locale.ENGLISH
  )

  val df_it: DateTimeFormatter =
    DateTimeFormatter.ofPattern("[dd-MM-yyyy][dd/MM/yyyy]", Locale.ITALIAN)

  val funder_regex: List[(Pattern, String)] = List(
    (
      Pattern.compile(
        "(info:eu-repo/grantagreement/ec/h2020/)(\\d\\d\\d\\d\\d\\d)(.*)",
        Pattern.MULTILINE | Pattern.CASE_INSENSITIVE
      ),
      "40|corda__h2020::"
    ),
    (
      Pattern.compile(
        "(info:eu-repo/grantagreement/ec/fp7/)(\\d\\d\\d\\d\\d\\d)(.*)",
        Pattern.MULTILINE | Pattern.CASE_INSENSITIVE
      ),
      "40|corda_______::"
    )
  )

  val Date_regex: List[Pattern] = List(
    //Y-M-D
    Pattern.compile(
      "(18|19|20)\\d\\d([- /.])(0[1-9]|1[012])\\2(0[1-9]|[12][0-9]|3[01])",
      Pattern.MULTILINE
    ),
    //M-D-Y
    Pattern.compile(
      "((0[1-9]|1[012])|([1-9]))([- /.])(0[1-9]|[12][0-9]|3[01])([- /.])(18|19|20)?\\d\\d",
      Pattern.MULTILINE
    ),
    //D-M-Y
    Pattern.compile(
      "(?:(?:31(/|-|\\.)(?:0?[13578]|1[02]|(?:Jan|Mar|May|Jul|Aug|Oct|Dec)))\\1|(?:(?:29|30)(/|-|\\.)(?:0?[1,3-9]|1[0-2]|(?:Jan|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec))\\2))(?:(?:1[6-9]|[2-9]\\d)?\\d{2})|(?:29(/|-|\\.)(?:0?2|(?:Feb))\\3(?:(?:(?:1[6-9]|[2-9]\\d)?(?:0[48]|[2468][048]|[13579][26])|(?:(?:16|[2468][048]|[3579][26])00))))|(?:0?[1-9]|1\\d|2[0-8])(/|-|\\.)(?:(?:0?[1-9]|(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep))|(?:1[0-2]|(?:Oct|Nov|Dec)))\\4(?:(?:1[6-9]|[2-9]\\d)?\\d{2})",
      Pattern.MULTILINE
    ),
    //Y
    Pattern.compile("(19|20)\\d\\d", Pattern.MULTILINE)
  )

}
