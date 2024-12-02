package eu.dnetlib.dhp.sx.bio.pubmed

import org.apache.commons.lang3.StringUtils

import javax.xml.stream.XMLEventReader
import scala.collection.JavaConverters._
import scala.xml.{MetaData, NodeSeq}
import scala.xml.pull.{EvElemEnd, EvElemStart, EvText}

class PMParser2 {

  /** Extracts the value of an attribute from a MetaData object.
    * @param attrs the MetaData object
    * @param key the key of the attribute
    * @return the value of the attribute or null if the attribute is not found
    */
  private def extractAttributes(attrs: MetaData, key: String): String = {

    val res = attrs.get(key)
    if (res.isDefined) {
      val s = res.get
      if (s != null && s.nonEmpty)
        s.head.text
      else
        null
    } else null
  }

  /** Validates and formats a date given the year, month, and day as strings.
    *
    * @param year  the year as a string
    * @param month the month as a string
    * @param day   the day as a string
    * @return      the formatted date as "YYYY-MM-DD" or null if the date is invalid
    */
  private def validate_Date(year: String, month: String, day: String): String = {
    try {
      f"${year.toInt}-${month.toInt}%02d-${day.toInt}%02d"

    } catch {
      case _: Throwable => null
    }
  }

  /** Extracts the grant information from a NodeSeq object.
    *
    * @param gNode the NodeSeq object
    * @return the grant information or an empty list if the grant information is not found
    */
  private def extractGrant(gNode: NodeSeq): List[PMGrant] = {
    gNode
      .map(node => {
        val grantId = (node \ "GrantID").text
        val agency = (node \ "Agency").text
        val country = (node \ "Country").text
        new PMGrant(grantId, agency, country)
      })
      .toList
  }

  /** Extracts the journal information from a NodeSeq object.
    *
    * @param jNode the NodeSeq object
    * @return the journal information or null if the journal information is not found
    */
  private def extractJournal(jNode: NodeSeq): PMJournal = {
    val journal = new PMJournal
    journal.setTitle((jNode \ "Title").text)
    journal.setIssn((jNode \ "ISSN").text)
    journal.setVolume((jNode \ "JournalIssue" \ "Volume").text)
    journal.setIssue((jNode \ "JournalIssue" \ "Issue").text)
    if (journal.getTitle != null && StringUtils.isNotEmpty(journal.getTitle))
      journal
    else
      null
  }

  private def extractAuthors(aNode: NodeSeq): List[PMAuthor] = {
    aNode
      .map(author => {
        val a = new PMAuthor
        a.setLastName((author \ "LastName").text)
        a.setForeName((author \ "ForeName").text)
        val id = (author \ "Identifier").text
        val idType =(author \ "Identifier" \ "@Source").text

        if(id != null && id.nonEmpty && idType != null && idType.nonEmpty) {
          a.setIdentifier(new PMIdentifier(id, idType))
        }


        val affiliation = (author \ "AffiliationInfo" \ "Affiliation").text
        val affiliationId  = (author \ "AffiliationInfo" \ "Identifier").text
        val affiliationIdType = (author \ "AffiliationInfo" \ "Identifier" \ "@Source").text

        if(affiliation != null && affiliation.nonEmpty) {
          val aff = new PMAffiliation()
          aff.setName(affiliation)
          if(affiliationId != null && affiliationId.nonEmpty && affiliationIdType != null && affiliationIdType.nonEmpty) {
            aff.setIdentifier(new PMIdentifier(affiliationId, affiliationIdType))
          }
          a.setAffiliation(aff)
        }
        a
      })
      .toList
  }

  def parse(input: String): PMArticle = {
    val xml = scala.xml.XML.loadString(input)
    val article = new PMArticle

    val grantNodes = xml \ "MedlineCitation" \\ "Grant"
    article.setGrants(extractGrant(grantNodes).asJava)

    val journal = xml \ "MedlineCitation" \ "Article" \ "Journal"
    article.setJournal(extractJournal(journal))

    val authors = xml \ "MedlineCitation" \ "Article" \ "AuthorList" \ "Author"

    article.setAuthors(
      extractAuthors(authors).asJava
    )

    val pmId = xml \ "MedlineCitation" \ "PMID"

    val articleIds = xml \ "PubmedData" \ "ArticleIdList" \ "ArticleId"
    articleIds.foreach(articleId => {
      val idType = (articleId \ "@IdType").text
      val id = articleId.text
      if ("doi".equalsIgnoreCase(idType)) article.setDoi(id)
      if ("pmc".equalsIgnoreCase(idType)) article.setPmcId(id)
    })
    article.setPmid(pmId.text)

    val pubMedPubDate = xml \ "MedlineCitation" \ "DateCompleted"
    val currentDate =
      validate_Date((pubMedPubDate \ "Year").text, (pubMedPubDate \ "Month").text, (pubMedPubDate \ "Day").text)
    if (currentDate != null) article.setDate(currentDate)

    val articleTitle = xml \ "MedlineCitation" \ "Article" \ "ArticleTitle"
    article.setTitle(articleTitle.text)

    val abstractText = xml \ "MedlineCitation" \ "Article" \ "Abstract" \ "AbstractText"
    if (abstractText != null && abstractText.text != null && abstractText.text.nonEmpty)
      article.setDescription(abstractText.text.split("\n").map(s => s.trim).mkString(" ").trim)

    val language = xml \ "MedlineCitation" \ "Article" \ "Language"
    article.setLanguage(language.text)

    val subjects = xml \ "MedlineCitation" \ "MeshHeadingList" \ "MeshHeading"
    article.setSubjects(
      subjects
        .take(20)
        .map(subject => {
          val descriptorName = (subject \ "DescriptorName").text
          val ui = (subject \ "DescriptorName" \ "@UI").text
          val s = new PMSubject
          s.setValue(descriptorName)
          s.setMeshId(ui)
          s
        })
        .toList
        .asJava
    )
    val publicationTypes = xml \ "MedlineCitation" \ "Article" \ "PublicationTypeList" \ "PublicationType"
    article.setPublicationTypes(
      publicationTypes
        .map(pt => {
          val s = new PMSubject
          s.setValue(pt.text)
          s
        })
        .toList
        .asJava
    )

    article
  }

  def parse2(xml: XMLEventReader): PMArticle = {
    var currentArticle: PMArticle = null
    var currentSubject: PMSubject = null
    var currentAuthor: PMAuthor = null
    var currentJournal: PMJournal = null
    var currentGrant: PMGrant = null
    var currNode: String = null
    var currentYear = "0"
    var currentMonth = "01"
    var currentDay = "01"
    var currentArticleType: String = null

    while (xml.hasNext) {
      val ne = xml.next
      ne match {
        case EvElemStart(_, label, attrs, _) =>
          currNode = label

          label match {
            case "PubmedArticle" => currentArticle = new PMArticle
            case "Author"        => currentAuthor = new PMAuthor
            case "Journal"       => currentJournal = new PMJournal
            case "Grant"         => currentGrant = new PMGrant
            case "PublicationType" | "DescriptorName" =>
              currentSubject = new PMSubject
              currentSubject.setMeshId(extractAttributes(attrs, "UI"))
            case "ArticleId" => currentArticleType = extractAttributes(attrs, "IdType")
            case _           =>
          }
        case EvElemEnd(_, label) =>
          label match {
            case "PubmedArticle" => return currentArticle
            case "Author"        => currentArticle.getAuthors.add(currentAuthor)
            case "Journal"       => currentArticle.setJournal(currentJournal)
            case "Grant"         => currentArticle.getGrants.add(currentGrant)
            case "PubMedPubDate" =>
              if (currentArticle.getDate == null)
                currentArticle.setDate(validate_Date(currentYear, currentMonth, currentDay))
            case "PubDate"         => currentJournal.setDate(s"$currentYear-$currentMonth-$currentDay")
            case "DescriptorName"  => currentArticle.getSubjects.add(currentSubject)
            case "PublicationType" => currentArticle.getPublicationTypes.add(currentSubject)
            case _                 =>
          }
        case EvText(text) =>
          if (currNode != null && text.trim.nonEmpty)
            currNode match {
              case "ArticleTitle" => {
                if (currentArticle.getTitle == null)
                  currentArticle.setTitle(text.trim)
                else
                  currentArticle.setTitle(currentArticle.getTitle + text.trim)
              }
              case "AbstractText" => {
                if (currentArticle.getDescription == null)
                  currentArticle.setDescription(text.trim)
                else
                  currentArticle.setDescription(currentArticle.getDescription + text.trim)
              }
              case "PMID" => currentArticle.setPmid(text.trim)
              case "ArticleId" =>
                if ("doi".equalsIgnoreCase(currentArticleType)) currentArticle.setDoi(text.trim)
                if ("pmc".equalsIgnoreCase(currentArticleType)) currentArticle.setPmcId(text.trim)
              case "Language"                           => currentArticle.setLanguage(text.trim)
              case "ISSN"                               => currentJournal.setIssn(text.trim)
              case "GrantID"                            => currentGrant.setGrantID(text.trim)
              case "Agency"                             => currentGrant.setAgency(text.trim)
              case "Country"                            => if (currentGrant != null) currentGrant.setCountry(text.trim)
              case "Year"                               => currentYear = text.trim
              case "Month"                              => currentMonth = text.trim
              case "Day"                                => currentDay = text.trim
              case "Volume"                             => currentJournal.setVolume(text.trim)
              case "Issue"                              => currentJournal.setIssue(text.trim)
              case "PublicationType" | "DescriptorName" => currentSubject.setValue(text.trim)
              case "LastName" => {
                if (currentAuthor != null)
                  currentAuthor.setLastName(text.trim)
              }
              case "ForeName" =>
                if (currentAuthor != null)
                  currentAuthor.setForeName(text.trim)
              case "Title" =>
                if (currentJournal.getTitle == null)
                  currentJournal.setTitle(text.trim)
                else
                  currentJournal.setTitle(currentJournal.getTitle + text.trim)
              case _ =>

            }
        case _ =>
      }

    }
    null
  }

}
