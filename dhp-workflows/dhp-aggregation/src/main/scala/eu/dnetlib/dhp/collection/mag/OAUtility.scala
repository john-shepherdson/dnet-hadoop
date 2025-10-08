package eu.dnetlib.dhp.collection.mag

import com.fasterxml.jackson.databind.ObjectMapper
import eu.dnetlib.dhp.collection.crossref.Crossref2Oaf
import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup
import eu.dnetlib.dhp.schema.common.ModelConstants
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils._
import eu.dnetlib.dhp.schema.oaf.utils.{OafMapperUtils, PidType}
import eu.dnetlib.dhp.schema.oaf._
import eu.dnetlib.dhp.utils.DHPUtils
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.collection.JavaConverters._

object OAUtility {

  val mapper = new ObjectMapper()
  private val MAGCollectedFrom = keyValue(ModelConstants.MAG_ID, ModelConstants.MAG_NAME)

  private val MAGDataInfo: DataInfo = {
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

  case class LocationType(
    source: Option[SourceType],
    pdf_url: Option[String],
    landing_page_url: Option[String],
    is_oa: Option[Boolean],
    is_accepted: Option[Boolean],
    is_published: Option[Boolean],
    version: Option[String],
    license: Option[String],
    doi: Option[String]
  )

  case class SourceType(
    id: Option[String],
    display_name: Option[String],
    issn_l: Option[String],
    issn: Option[Seq[String]],
    host_organization: Option[String],
    `type`: Option[String]
  )

  case class AuthorType(
    id: Option[String],
    display_name: Option[String],
    orcid: Option[String]
  )

  case class InstitutionType(
    id: Option[String],
    display_name: Option[String],
    ror: Option[String],
    country_code: Option[String]
  )

  case class AuthorShipType(
    author_position: Option[String],
    author: Option[AuthorType],
    institutions: Option[Seq[InstitutionType]],
    raw_affiliation_strings: Option[Seq[String]]
  )

  case class IdsType(
    openalex: Option[String],
    doi: Option[String],
    pmid: Option[String],
    mag: Option[String],
    pmcid: Option[String]
  )

  case class OpenAccessType(
    is_oa: Option[Boolean],
    any_repository_has_fulltext: Option[Boolean],
    oa_status: Option[String],
    oa_url: Option[String]
  )

  case class OAWorks(
    id: Option[String],
    doi: Option[String],
    doi_registration_agency: Option[String],
    display_name: Option[String],
    title: Option[String],
    publication_year: Option[String],
    publication_date: Option[String],
    language: Option[String],
    ids: IdsType,
    primary_location: Option[LocationType],
    best_oa_location: Option[LocationType],
    `type`: Option[String],
    type_crossref: Option[String],
    open_access: Option[OpenAccessType],
    updated_date: Option[String],
    created_date: Option[String],
    referenced_works: Option[Seq[String]],
    abstract_inverted_index: Option[String],
    authorships: Option[Seq[AuthorShipType]]
  )

  def convertWorksToResult(works: OAWorks, vocabularies: VocabularyGroup): String = {
    if (works.ids.mag.isEmpty || works.type_crossref.isEmpty || works.doi.isEmpty) return null
    val id = works.ids.mag.orNull
    val typology = Crossref2Oaf.getTypeQualifier(works.type_crossref.get, vocabularies)
    if (typology == null)
      return null
    val result = Crossref2Oaf.generateItemFromType(typology._2)
    if (result == null)
      return null
    val pidList = List(
      structuredProperty(
        id,
        qualifier(
          PidType.mag_id.toString,
          PidType.mag_id.toString,
          ModelConstants.DNET_PID_TYPES,
          ModelConstants.DNET_PID_TYPES
        ),
        null
      ),
      structuredProperty(
        works.doi.get,
        qualifier(
          PidType.doi.toString,
          PidType.doi.toString,
          ModelConstants.DNET_PID_TYPES,
          ModelConstants.DNET_PID_TYPES
        ),
        null
      )
    )
    result.setDataInfo(MAGDataInfo)
    result.setLastupdatetimestamp(System.currentTimeMillis())
    val instance = new Instance
    instance.setInstancetype(typology._1)
    instance.setPid(pidList.asJava)

    result.setOriginalId(pidList.map(s => s.getValue).asJava)
    result.setId(s"50|mag_________::${DHPUtils.md5(id)}")
    result.setPid(pidList.asJava)

    result.setCollectedfrom(List(MAGCollectedFrom).asJava)
    result.setOriginalId(pidList.map(s => s.getValue).asJava)
    if (works.title.isEmpty) return null
    val originalTitles = structuredProperty(works.title.get, ModelConstants.MAIN_TITLE_QUALIFIER, null)
    result.setTitle(List(originalTitles).asJava)
    if (works.publication_date.isDefined)
      result.setDateofacceptance(field(works.publication_date.get, null))
    if (works.best_oa_location.isDefined && works.best_oa_location.get.source.isDefined) {
      updateLocation(works.best_oa_location.get, result, instance)
    } else if (works.primary_location.isDefined && works.primary_location.get.source.isDefined) {
      updateLocation(works.primary_location.get, result, instance)
    }
    if (works.abstract_inverted_index.isDefined) {
      val abstractText = reconstructAbstract(works.abstract_inverted_index.get)
      result.setDescription(List(field(abstractText, null)).asJava)
    }

    if (works.authorships.isDefined && works.authorships.get.nonEmpty) {
      val authorList: List[Author] = works.authorships.get
        .filter(a => a.author.isDefined)
        .map(wa => {
          val author = wa.author.get
          if (author.display_name.isEmpty)
            return null
          val currentAuthor = new Author()
          currentAuthor.setFullname(author.display_name.get)
          if (author.orcid.isDefined) {
            currentAuthor.setPid(
              List(
                structuredProperty(
                  author.orcid.get,
                  qualifier(
                    ModelConstants.ORCID_PENDING,
                    ModelConstants.ORCID_PENDING,
                    ModelConstants.DNET_PID_TYPES,
                    ModelConstants.DNET_PID_TYPES
                  ),
                  null
                )
              ).asJava
            )

          }
          if ("first".equalsIgnoreCase(wa.author_position.orNull))
            currentAuthor.setRank(1)

          if (wa.raw_affiliation_strings.isDefined && wa.raw_affiliation_strings.get.nonEmpty)
            currentAuthor.setRawAffiliationString(wa.raw_affiliation_strings.get.asJava)
          currentAuthor
        })
        .filter(s => s != null)
        .toList
      if (authorList.nonEmpty) {
        result.setAuthor(authorList.asJava)
      }

    } else
      return null

    result.setInstance(List(instance).asJava)
    mapper.writeValueAsString(result)
  }

  private def updateLocation(location: LocationType, result: Result, instance: Instance): Unit = {
    val source = location.source.get
    val typology_source = source.`type`.orNull
    val display_name = source.display_name.orNull
    if (source.display_name.isDefined)
      result.setPublisher(field(source.display_name.get, null))
    if ("journal".equalsIgnoreCase(typology_source)) {
      val j = new Journal
      j.setName(display_name)
      if (source.issn.isDefined && source.issn.get.nonEmpty)
        j.setIssnPrinted(source.issn.get.head)
      if (source.issn_l.isDefined)
        j.setIssnOnline(source.issn_l.get)
      result match {
        case publication: Publication => publication.setJournal(j)
        case _                        =>
      }
    }
    if (location.pdf_url.isDefined) {
      val pdfUrl = location.pdf_url.get

      instance.setUrl(List(pdfUrl).asJava)

    }

    if (location.is_oa.getOrElse(false)) {
      instance.setAccessright(ModelConstants.OPEN_ACCESS_RIGHT())

    } else {
      instance.setAccessright(
        accessRight(
          ModelConstants.UNKNOWN,
          ModelConstants.NOT_AVAILABLE,
          ModelConstants.DNET_ACCESS_MODES,
          ModelConstants.DNET_ACCESS_MODES
        )
      )
    }
    if (location.license.isDefined)
      instance.setLicense(field(location.license.get, null))
    instance.setCollectedfrom(MAGCollectedFrom)
    instance.setHostedby(ModelConstants.UNKNOWN_REPOSITORY)

  }

  def extractRelations(works: OAWorks): List[Relation] = {
    if (works.ids.mag.isEmpty || works.type_crossref.isEmpty || works.doi.isEmpty) return List()
    val id = works.ids.mag.orNull
    if (id == null) return List()
    if (works.referenced_works.isEmpty) return List()
    val citationRelation =
      if (works.referenced_works.isEmpty) List()
      else
        works.referenced_works.get
          .map(r => {
            val relation = new Relation
            relation.setRelType(ModelConstants.RESULT_RESULT)
            relation.setRelClass(ModelConstants.CITES)
            relation.setSubRelType(ModelConstants.CITATION)
            relation.setCollectedfrom(List(MAGCollectedFrom).asJava)
            relation.setDataInfo(MAGDataInfo)
            relation.setSource(s"50|mag_________::${DHPUtils.md5(id)}")
            val target = r.split("/").last.replace("W", "")
            relation.setTarget(s"50|mag_________::${DHPUtils.md5(target)}")
            relation
          })
          .toList

    val affiliationRelations: List[Relation] =
      if (works.authorships.isEmpty) List()
      else {
        works.authorships.get
          .filter(a => a.institutions.isDefined)
          .flatMap(wa => {
            val author = wa.author.get
            val id = works.ids.mag.orNull
            if (id == null) return List()
            val institutions = if (wa.institutions.isDefined) wa.institutions.get else List()
            institutions
              .filter(i => i.ror.isDefined)
              .map(i => {
                val ror = i.ror.get
                val relation = new Relation
                relation.setRelType(ModelConstants.RESULT_ORGANIZATION)
                relation.setRelClass(ModelConstants.HAS_AUTHOR_INSTITUTION)
                relation.setSubRelType(ModelConstants.AFFILIATION)
                relation.setCollectedfrom(List(MAGCollectedFrom).asJava)
                relation.setDataInfo(MAGDataInfo)
                relation.setSource(s"50|mag_________::${DHPUtils.md5(id)}")
                relation.setTarget(s"20|ror_________::${DHPUtils.md5(ror)}")
                relation
              })
          })
          .toList
      }
    citationRelation ::: affiliationRelations
  }

  def reconstructAbstract(invertedIndex: String): String = {
    //deserialize invertedIndex which is a json representation of a Map<String, Array<Integer>>
    implicit val formats: DefaultFormats.type = DefaultFormats

    val result: Map[String, Array[Int]] = parse(invertedIndex).extract[Map[String, Array[Int]]]
    if (result.isEmpty) return null
    val total_values = result.values.map(i => i.max).max
    //creates an Array String of size total_values
    val resultArray = Array.fill(total_values + 1)("")
    result.foreach(item => {
      item._2.foreach(idx => {
        resultArray(idx) = item._1
      })
    })

    //creates a string from the array with space as separator
    resultArray.mkString(" ")
  }

}
