package eu.dnetlib.dhp.sx.bio.pubmed

import scala.xml.MetaData
import scala.xml.pull.{EvElemEnd, EvElemStart, EvText, XMLEventReader}

/** @param xml
  */
class PMParser(xml: XMLEventReader) extends Iterator[PMArticle] {

  var currentArticle: PMArticle = generateNextArticle()

  override def hasNext: Boolean = currentArticle != null

  override def next(): PMArticle = {
    val tmp = currentArticle
    currentArticle = generateNextArticle()
    tmp
  }

  def extractAttributes(attrs: MetaData, key: String): String = {

    val res = attrs.get(key)
    if (res.isDefined) {
      val s = res.get
      if (s != null && s.nonEmpty)
        s.head.text
      else
        null
    } else null
  }

  def validate_Date(year: String, month: String, day: String): String = {
    try {
      f"${year.toInt}-${month.toInt}%02d-${day.toInt}%02d"

    } catch {
      case _: Throwable => null
    }
  }

  def generateNextArticle(): PMArticle = {

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
      xml.next match {
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
