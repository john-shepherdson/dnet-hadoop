package eu.dnetlib.dhp.datacite

import eu.dnetlib.dhp.schema.common.ModelConstants
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils
import eu.dnetlib.dhp.schema.oaf.{DataInfo, EntityDataInfo, KeyValue}

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
  val dataInfo: EntityDataInfo = dataciteDataInfo(0.9f)
  val relDataInfo = OafMapperUtils.fromEntityDataInfo(dataInfo);

  val DATACITE_COLLECTED_FROM: KeyValue =
    OafMapperUtils.keyValue(ModelConstants.DATACITE_ID, DATACITE_NAME)

  val datacite_filter: List[String] = {
    val stream: InputStream = getClass.getResourceAsStream(DATACITE_FILTER_PATH)
    require(stream != null)
    Source.fromInputStream(stream).getLines().toList
  }

  def dataciteDataInfo(trust: Float): EntityDataInfo = OafMapperUtils.dataInfo(
    false,
    false,
    trust,
    null,
    false,
    ModelConstants.PROVENANCE_ACTION_SET_QUALIFIER
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
