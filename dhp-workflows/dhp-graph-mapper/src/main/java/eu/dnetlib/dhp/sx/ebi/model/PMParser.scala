package eu.dnetlib.dhp.sx.ebi.model
import scala.xml.pull.{EvElemEnd, EvElemStart, EvText, XMLEventReader}
class PMParser(xml:XMLEventReader) extends Iterator[PMArticle] {

  var currentArticle:PMArticle = generateNextArticle()

  override def hasNext: Boolean = currentArticle!= null

  override def next(): PMArticle = {
    val tmp = currentArticle
    currentArticle = generateNextArticle()
    tmp
  }


  def generateNextArticle():PMArticle = {

    var currentAuthor: PMAuthor = null
    var currentJournal: PMJournal = null
    var currNode: String = null
    var currentYear = "0"
    var currentMonth = "01"
    var currentDay = "01"

    while (xml.hasNext) {
      xml.next match {
        case EvElemStart(_, label, _, _) =>
          currNode = label
          label match {
            case "PubmedArticle" => currentArticle = new PMArticle
            case "Author" => currentAuthor = new PMAuthor
            case "Journal" => currentJournal = new PMJournal
            case _ =>
          }
        case EvElemEnd(_, label) =>
          label match {
            case "PubmedArticle" => return currentArticle
            case "Author" => currentArticle.getAuthors.add(currentAuthor)
            case "Journal" => currentArticle.setJournal(currentJournal)
            case "DateCompleted" => currentArticle.setDate(s"$currentYear-$currentMonth-$currentDay")
            case "PubDate" => currentJournal.setDate(s"$currentYear-$currentMonth-$currentDay")
            case _ =>
          }
        case EvText(text) =>
          if (currNode!= null && text.trim.nonEmpty)
            currNode match {
              case "ArticleTitle" => {
                if (currentArticle.getTitle==null)
                  currentArticle.setTitle(text.trim)
                else
                  currentArticle.setTitle(currentArticle.getTitle + text.trim)
              }
              case "AbstractText" => {
                if (currentArticle.getDescription==null)
                  currentArticle.setDescription(text.trim)
                else
                  currentArticle.setDescription(currentArticle.getDescription + text.trim)
              }
              case "PMID" => currentArticle.setPmid(text.trim)
              case "ISSN" => currentJournal.setIssn(text.trim)
              case "Year" => currentYear = text.trim
              case "Month" => currentMonth = text.trim
              case "Day" => currentDay = text.trim
              case "Volume" => currentJournal.setVolume( text.trim)
              case "Issue" => currentJournal.setIssue (text.trim)
              case "LastName" => {
                if (currentAuthor != null)
                  currentAuthor.setLastName(text.trim)

              }
              case "ForeName" => if (currentAuthor != null)
                currentAuthor.setForeName(text.trim)
              case "Title" =>
                if (currentJournal.getTitle==null)
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





