package eu.dnetlib.dhp.sx.bio.pubmed

import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup
import eu.dnetlib.dhp.schema.common.ModelConstants
import eu.dnetlib.dhp.schema.oaf.utils.{GraphCleaningFunctions, IdentifierFactory, OafMapperUtils, PidType}
import eu.dnetlib.dhp.schema.oaf._
import collection.JavaConverters._

import java.util.regex.Pattern

/**
 *
 */
object PubMedToOaf {

  val SUBJ_CLASS = "keywords"
  val urlMap = Map(
    "pmid" -> "https://pubmed.ncbi.nlm.nih.gov/",
    "doi" -> "https://dx.doi.org/"
  )
  val dataInfo: DataInfo = OafMapperUtils.dataInfo(false, null, false, false, ModelConstants.PROVENANCE_ACTION_SET_QUALIFIER, "0.9")
  val collectedFrom: KeyValue = OafMapperUtils.keyValue(ModelConstants.EUROPE_PUBMED_CENTRAL_ID, "Europe PubMed Central")


  /**
   * Cleaning the DOI Applying regex in order to
   * remove doi starting with URL
   *
   * @param doi input DOI
   * @return cleaned DOI
   */
  def cleanDoi(doi: String): String = {

    val regex = "^10.\\d{4,9}\\/[\\[\\]\\-\\<\\>._;()\\/:A-Z0-9]+$"


    val pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE)
    val matcher = pattern.matcher(doi)

    if (matcher.find) {
      return matcher.group(0)
    }
    null
  }

  /**
   *
   * Create an instance of class extends Result
   * starting from OAF instanceType value
   *
   * @param cobjQualifier OAF instance type
   * @param vocabularies  All dnet vocabularies
   * @return the correct instance
   */
  def createResult(cobjQualifier: Qualifier, vocabularies: VocabularyGroup): Result = {
    val result_typologies = getVocabularyTerm(ModelConstants.DNET_RESULT_TYPOLOGIES, vocabularies, cobjQualifier.getClassid)
    result_typologies.getClassid match {
      case "dataset" => new Dataset
      case "publication" => new Publication
      case "other" => new OtherResearchProduct
      case "software" => new Software
      case _ => null

    }
  }

  /**
   * Mapping the Pubmedjournal info into the OAF Journale
   *
   * @param j the pubmedJournal
   * @return the OAF Journal
   */
  def mapJournal(j: PMJournal): Journal = {
    if (j == null)
      return null
    val journal = new Journal

    journal.setDataInfo(dataInfo)
    journal.setName(j.getTitle)
    journal.setConferencedate(j.getDate)
    journal.setVol(j.getVolume)
    journal.setIssnPrinted(j.getIssn)
    journal.setIss(j.getIssue)
    journal


  }

  /**
   *
   * Find vocabulary term into synonyms and term in the vocabulary
   *
   * @param vocabularyName the input vocabulary name
   * @param vocabularies   all the vocabularies
   * @param term           the term to find
   * @return the cleaned term value
   */
  def getVocabularyTerm(vocabularyName: String, vocabularies: VocabularyGroup, term: String): Qualifier = {
    val a = vocabularies.getSynonymAsQualifier(vocabularyName, term)
    val b = vocabularies.getTermAsQualifier(vocabularyName, term)
    if (a == null) b else a
  }


  /**
   * Map the Pubmed Article into the OAF instance
   *
   * @param article      the pubmed articles
   * @param vocabularies the vocabularies
   * @return The OAF instance if the mapping did not fail
   */
  def convert(article: PMArticle, vocabularies: VocabularyGroup): Result = {

    if (article.getPublicationTypes == null)
      return null


    // MAP PMID into  pid with  classid = classname = pmid
    val pidList: List[StructuredProperty] = List(OafMapperUtils.structuredProperty(article.getPmid, PidType.pmid.toString, PidType.pmid.toString, ModelConstants.DNET_PID_TYPES, ModelConstants.DNET_PID_TYPES, dataInfo))
    if (pidList == null)
      return null

    // MAP //ArticleId[./@IdType="doi"]   into  alternateIdentifier with classid = classname = doi
    var alternateIdentifier: StructuredProperty = null
    if (article.getDoi != null) {
      val normalizedPid = cleanDoi(article.getDoi)
      if (normalizedPid != null)
        alternateIdentifier = OafMapperUtils.structuredProperty(normalizedPid, PidType.doi.toString, PidType.doi.toString, ModelConstants.DNET_PID_TYPES, ModelConstants.DNET_PID_TYPES, dataInfo)
    }

    // INSTANCE MAPPING
    //--------------------------------------------------------------------------------------

    // If the article contains the typology Journal Article then we apply this type
    //else We have to find a terms that match the vocabulary otherwise we discard it
    val ja = article.getPublicationTypes.asScala.find(s => "Journal Article".equalsIgnoreCase(s.getValue))
    val pubmedInstance = new Instance
    if (ja.isDefined) {
      val cojbCategory = getVocabularyTerm(ModelConstants.DNET_PUBLICATION_RESOURCE, vocabularies, ja.get.getValue)
      pubmedInstance.setInstancetype(cojbCategory)
    } else {
      val i_type = article.getPublicationTypes.asScala
        .map(s => getVocabularyTerm(ModelConstants.DNET_PUBLICATION_RESOURCE, vocabularies, s.getValue))
        .find(q => q != null)
      if (i_type.isDefined)
        pubmedInstance.setInstancetype(i_type.get)
      else
        return null
    }
    val result = createResult(pubmedInstance.getInstancetype, vocabularies)
    if (result == null)
      return result
    result.setDataInfo(dataInfo)
    pubmedInstance.setPid(pidList.asJava)
    if (alternateIdentifier != null)
      pubmedInstance.setAlternateIdentifier(List(alternateIdentifier).asJava)
    result.setInstance(List(pubmedInstance).asJava)
    pubmedInstance.getPid.asScala.filter(p => "pmid".equalsIgnoreCase(p.getQualifier.getClassid)).map(p => p.getValue)(collection.breakOut)
    //CREATE URL From pmid
    val urlLists: List[String] = pidList
      .map(s => (urlMap.getOrElse(s.getQualifier.getClassid, ""), s.getValue))
      .filter(t => t._1.nonEmpty)
      .map(t => t._1 + t._2)
    if (urlLists != null)
      pubmedInstance.setUrl(urlLists.asJava)

    //ASSIGN DateofAcceptance
    pubmedInstance.setDateofacceptance(OafMapperUtils.field(GraphCleaningFunctions.cleanDate(article.getDate), dataInfo))
    //ASSIGN COLLECTEDFROM
    pubmedInstance.setCollectedfrom(collectedFrom)
    result.setPid(pidList.asJava)

    //END INSTANCE MAPPING
    //--------------------------------------------------------------------------------------


    // JOURNAL MAPPING
    //--------------------------------------------------------------------------------------
    if (article.getJournal != null && result.isInstanceOf[Publication])
      result.asInstanceOf[Publication].setJournal(mapJournal(article.getJournal))
    result.setCollectedfrom(List(collectedFrom).asJava)
    //END JOURNAL MAPPING
    //--------------------------------------------------------------------------------------


    // RESULT MAPPING
    //--------------------------------------------------------------------------------------
    result.setDateofacceptance(OafMapperUtils.field(GraphCleaningFunctions.cleanDate(article.getDate), dataInfo))

    if (article.getTitle == null || article.getTitle.isEmpty)
      return null
    result.setTitle(List(OafMapperUtils.structuredProperty(article.getTitle, ModelConstants.MAIN_TITLE_QUALIFIER, dataInfo)).asJava)

    if (article.getDescription != null && article.getDescription.nonEmpty)
      result.setDescription(List(OafMapperUtils.field(article.getDescription, dataInfo)).asJava)

    if (article.getLanguage != null) {

      val term = vocabularies.getSynonymAsQualifier(ModelConstants.DNET_LANGUAGES, article.getLanguage)
      if (term != null)
        result.setLanguage(term)
    }


    val subjects: List[StructuredProperty] = article.getSubjects.asScala.map(s => OafMapperUtils.structuredProperty(s.getValue, SUBJ_CLASS, SUBJ_CLASS, ModelConstants.DNET_SUBJECT_TYPOLOGIES, ModelConstants.DNET_SUBJECT_TYPOLOGIES, dataInfo))(collection.breakOut)
    if (subjects != null)
      result.setSubject(subjects.asJava)


    val authors: List[Author] = article.getAuthors.asScala.zipWithIndex.map { case (a, index) =>
      val author = new Author()
      author.setName(a.getForeName)
      author.setSurname(a.getLastName)
      author.setFullname(a.getFullName)
      author.setRank(index + 1)
      author
    }(collection.breakOut)


    if (authors != null && authors.nonEmpty)
      result.setAuthor(authors.asJava)
    result.setOriginalId(pidList.map(s => s.getValue).asJava)


    result.setId(article.getPmid)


    // END RESULT MAPPING
    //--------------------------------------------------------------------------------------
    val id = IdentifierFactory.createIdentifier(result)
    if (article.getPmid.equalsIgnoreCase(id))
      return null
    result.setId(id)
    result
  }


}
