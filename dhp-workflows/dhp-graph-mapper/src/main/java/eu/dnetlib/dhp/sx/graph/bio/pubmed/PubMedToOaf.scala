package eu.dnetlib.dhp.sx.graph.bio.pubmed

import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup
import eu.dnetlib.dhp.schema.common.ModelConstants
import eu.dnetlib.dhp.schema.oaf.utils.{GraphCleaningFunctions, IdentifierFactory, OafMapperUtils, PidType}
import eu.dnetlib.dhp.schema.oaf._

import scala.collection.JavaConverters._

object PubMedToOaf {

  val SUBJ_CLASS = "keywords"
  val urlMap = Map(
    "pmid" -> "https://pubmed.ncbi.nlm.nih.gov/",
    "doi" -> "https://dx.doi.org/"
  )

  def createResult(cobjQualifier: Qualifier, vocabularies: VocabularyGroup): Result = {
    val result_typologies = getVocabularyTerm("dnet:result_typologies", vocabularies, cobjQualifier.getClassid)
    result_typologies.getClassid match {
      case "dataset" => new Dataset
      case "publication" => new Publication
      case "other" => new OtherResearchProduct
      case "software" => new Software
      case _ => null

    }
  }

  def mapJournal(j: PMJournal): Journal = {
    if (j == null)
      return null
    val journal = new Journal

    journal.setDataInfo(dataInfo)
    journal.setName(j.getTitle)
    journal.setVol(j.getVolume)
    journal.setIssnPrinted(j.getIssn)
    journal.setIss(j.getIssue)
    journal


  }


  def getVocabularyTerm(vocabularyName: String, vocabularies: VocabularyGroup, term: String): Qualifier = {
    val a = vocabularies.getSynonymAsQualifier(vocabularyName, term)
    val b = vocabularies.getTermAsQualifier(vocabularyName, term)
    if (a == null) b else a
  }

  val dataInfo: DataInfo = OafMapperUtils.dataInfo(false, null, false, false, ModelConstants.PROVENANCE_ACTION_SET_QUALIFIER, "0.9")
  val collectedFrom: KeyValue = OafMapperUtils.keyValue(ModelConstants.EUROPE_PUBMED_CENTRAL_ID, "Europe PubMed Central")

  def convert(article: PMArticle, vocabularies: VocabularyGroup): Result = {

    if (article.getPublicationTypes == null)
      return null
    val i = new Instance
    var pidList: List[StructuredProperty] = List(OafMapperUtils.structuredProperty(article.getPmid, PidType.pmid.toString, PidType.pmid.toString, ModelConstants.DNET_PID_TYPES, ModelConstants.DNET_PID_TYPES, dataInfo))
    if (pidList == null)
      return null
    if (article.getDoi != null) {
      pidList = pidList ::: List(OafMapperUtils.structuredProperty(article.getDoi, PidType.doi.toString, PidType.doi.toString, ModelConstants.DNET_PID_TYPES, ModelConstants.DNET_PID_TYPES, dataInfo))
    }

    // If the article contains the typology Journal Article then we apply this type
    //else We have to find a terms that match the vocabulary otherwise we discard it
    val ja = article.getPublicationTypes.asScala.find(s => "Journal Article".equalsIgnoreCase(s.getValue))
    if (ja.isDefined) {
      val cojbCategory = getVocabularyTerm("dnet:publication_resource", vocabularies, ja.get.getValue)
      i.setInstancetype(cojbCategory)
    } else {
      val i_type = article.getPublicationTypes.asScala
        .map(s => getVocabularyTerm("dnet:publication_resource", vocabularies, s.getValue))
        .find(q => q != null)
      if (i_type.isDefined)
        i.setInstancetype(i_type.get)
      else
        return null
    }
    val result = createResult(i.getInstancetype, vocabularies)
    if (result == null)
      return result
    result.setDataInfo(dataInfo)
    i.setPid(pidList.asJava)
    result.setInstance(List(i).asJava)


    i.getPid.asScala.filter(p => "pmid".equalsIgnoreCase(p.getQualifier.getClassid)).map(p => p.getValue)(collection breakOut)
    val urlLists: List[String] = pidList
      .map(s => (urlMap.getOrElse(s.getQualifier.getClassid, ""), s.getValue))
      .filter(t => t._1.nonEmpty)
      .map(t => t._1 + t._2)
    if (urlLists != null)
      i.setUrl(urlLists.asJava)
    i.setDateofacceptance(OafMapperUtils.field(GraphCleaningFunctions.cleanDate(article.getDate), dataInfo))
    i.setCollectedfrom(collectedFrom)
    result.setPid(pidList.asJava)
    if (article.getJournal != null && result.isInstanceOf[Publication])
      result.asInstanceOf[Publication].setJournal(mapJournal(article.getJournal))
    result.setCollectedfrom(List(collectedFrom).asJava)

    result.setDateofacceptance(OafMapperUtils.field(GraphCleaningFunctions.cleanDate(article.getDate), dataInfo))

    if (article.getTitle == null || article.getTitle.isEmpty)
      return null
    result.setTitle(List(OafMapperUtils.structuredProperty(article.getTitle, ModelConstants.MAIN_TITLE_QUALIFIER, dataInfo)).asJava)

    if (article.getDescription != null && article.getDescription.nonEmpty)
      result.setDescription(List(OafMapperUtils.field(article.getDescription, dataInfo)).asJava)

    if (article.getLanguage != null) {

      val term = vocabularies.getSynonymAsQualifier("dnet:languages", article.getLanguage)
      if (term != null)
        result.setLanguage(term)
    }


    val subjects: List[StructuredProperty] = article.getSubjects.asScala.map(s => OafMapperUtils.structuredProperty(s.getValue, SUBJ_CLASS, SUBJ_CLASS, ModelConstants.DNET_SUBJECT_TYPOLOGIES, ModelConstants.DNET_SUBJECT_TYPOLOGIES, dataInfo))(collection breakOut)
    if (subjects != null)
      result.setSubject(subjects.asJava)


    val authors: List[Author] = article.getAuthors.asScala.zipWithIndex.map { case (a, index) =>
      val author = new Author()
      author.setName(a.getForeName)
      author.setSurname(a.getLastName)
      author.setFullname(a.getFullName)
      author.setRank(index + 1)
      author
    }(collection breakOut)


    if (authors != null && authors.nonEmpty)
      result.setAuthor(authors.asJava)
    result.setOriginalId(pidList.map(s => s.getValue).asJava)


    result.setId(article.getPmid)

    val id = IdentifierFactory.createIdentifier(result)
    if (article.getPmid.equalsIgnoreCase(id))
      return null
    result.setId(id)
    result
  }


}
