package eu.dnetlib.doiboost.mag


import eu.dnetlib.dhp.schema.oaf.{Instance, Journal, Publication}
import org.json4s
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import eu.dnetlib.doiboost.DoiBoostMappingUtil._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.matching.Regex


case class MagPapers(PaperId: Long, Rank: Integer, Doi: String,
                     DocType: String, PaperTitle: String, OriginalTitle: String,
                     BookTitle: String, Year: Option[Integer], Date: Option[java.sql.Timestamp], Publisher: String,
                     JournalId: Option[Long], ConferenceSeriesId: Option[Long], ConferenceInstanceId: Option[Long],
                     Volume: String, Issue: String, FirstPage: String, LastPage: String,
                     ReferenceCount: Option[Long], CitationCount: Option[Long], EstimatedCitation: Option[Long],
                     OriginalVenue: String, FamilyId: Option[Long], CreatedDate: java.sql.Timestamp) {}


case class MagPaperAbstract(PaperId: Long, IndexedAbstract: String) {}

case class MagAuthor(AuthorId: Long, Rank: Option[Int], NormalizedName: Option[String], DisplayName: Option[String], LastKnownAffiliationId: Option[Long], PaperCount: Option[Long], CitationCount: Option[Long], CreatedDate: Option[java.sql.Timestamp]) {}

case class MagAffiliation(AffiliationId: Long, Rank: Int, NormalizedName: String, DisplayName: String, GridId: String, OfficialPage: String, WikiPage: String, PaperCount: Long, CitationCount: Long, Latitude: Option[Float], Longitude: Option[Float], CreatedDate: java.sql.Timestamp) {}

case class MagPaperAuthorAffiliation(PaperId: Long, AuthorId: Long, AffiliationId: Option[Long], AuthorSequenceNumber: Int, OriginalAuthor: String, OriginalAffiliation: String) {}


case class MagAuthorAffiliation(author: MagAuthor, affiliation:String)

case class MagPaperWithAuthorList(PaperId: Long, authors: List[MagAuthorAffiliation]) {}

case class MagPaperAuthorDenormalized(PaperId: Long, author: MagAuthor, affiliation:String) {}

case class MagPaperUrl(PaperId: Long, SourceType: Option[Int], SourceUrl: Option[String], LanguageCode: Option[String]) {}

case class MagUrlInstance(SourceUrl:String){}

case class MagUrl(PaperId: Long, instances: List[MagUrlInstance])

case class MagSubject(FieldOfStudyId:Long, DisplayName:String, MainType:Option[String], Score:Float){}

case class MagFieldOfStudy(PaperId:Long, subjects:List[MagSubject]) {}

case class MagJournal(JournalId: Long, Rank: Option[Int], NormalizedName: Option[String], DisplayName: Option[String], Issn: Option[String], Publisher: Option[String], Webpage: Option[String], PaperCount: Option[Long], CitationCount: Option[Long], CreatedDate: Option[java.sql.Timestamp]) {}


case class MagConferenceInstance(ci:Long, DisplayName:Option[String], Location:Option[String], StartDate:Option[java.sql.Timestamp],   EndDate:Option[java.sql.Timestamp],  PaperId:Long){}

case object ConversionUtil {

  def extractMagIdentifier(pids:mutable.Buffer[String]) :String ={
    val magIDRegex: Regex = "^[0-9]+$".r
    val s =pids.filter(p=> magIDRegex.findAllIn(p).hasNext)

    if (s.nonEmpty)
      return s.head
    null
  }



  def addInstances(a: (Publication, MagUrl)): Publication = {
    val pub = a._1
    val urls = a._2


    val i = new Instance


    if (urls!= null) {

      val l:List[String] = urls.instances.filter(k=>k.SourceUrl.nonEmpty).map(k=>k.SourceUrl):::List(s"https://academic.microsoft.com/#/detail/${extractMagIdentifier(pub.getOriginalId.asScala)}")

      i.setUrl(l.asJava)
    }
    else
      i.setUrl(List(s"https://academic.microsoft.com/#/detail/${extractMagIdentifier(pub.getOriginalId.asScala)}").asJava)

    i.setCollectedfrom(createMAGCollectedFrom())
    pub.setInstance(List(i).asJava)
    pub
  }


  def transformPaperAbstract(input: MagPaperAbstract): MagPaperAbstract = {
    MagPaperAbstract(input.PaperId, convertInvertedIndexString(input.IndexedAbstract))
  }


  def createOAFFromJournalAuthorPaper(inputParams: ((MagPapers, MagJournal), MagPaperWithAuthorList)): Publication = {
    val paper = inputParams._1._1
    val journal = inputParams._1._2
    val authors = inputParams._2

    val pub = new Publication
    pub.setPid(List(createSP(paper.Doi.toLowerCase, "doi", PID_TYPES)).asJava)
    pub.setOriginalId(List(paper.PaperId.toString, paper.Doi.toLowerCase).asJava)

    //Set identifier as {50|60} | doiboost____::md5(DOI)
    pub.setId(generateIdentifier(pub, paper.Doi.toLowerCase))

    val mainTitles = createSP(paper.PaperTitle, "main title", "dnet:dataCite_title")
    val originalTitles = createSP(paper.OriginalTitle, "alternative title", "dnet:dataCite_title")
    pub.setTitle(List(mainTitles, originalTitles).asJava)

    pub.setSource(List(asField(paper.BookTitle)).asJava)

    val authorsOAF = authors.authors.map { f: MagAuthorAffiliation =>

      val a: eu.dnetlib.dhp.schema.oaf.Author = new eu.dnetlib.dhp.schema.oaf.Author

      a.setFullname(f.author.DisplayName.get)

      if(f.affiliation!= null)
        a.setAffiliation(List(asField(f.affiliation)).asJava)
      a.setPid(List(createSP(s"https://academic.microsoft.com/#/detail/${f.author.AuthorId}", "URL", PID_TYPES)).asJava)
      a
    }
    pub.setAuthor(authorsOAF.asJava)


    if (paper.Date != null && paper.Date.isDefined) {
      pub.setDateofacceptance(asField(paper.Date.get.toString))
    }
    pub.setPublisher(asField(paper.Publisher))


    if (journal != null && journal.DisplayName.isDefined) {
      val j = new Journal

      j.setName(journal.DisplayName.get)
      j.setSp(paper.FirstPage)
      j.setEp(paper.LastPage)
      if (journal.Publisher.isDefined)
        pub.setPublisher(asField(journal.Publisher.get))
      if (journal.Issn.isDefined)
        j.setIssnPrinted(journal.Issn.get)
      pub.setJournal(j)
    }
    pub.setCollectedfrom(List(createMAGCollectedFrom()).asJava)
    pub.setDataInfo(generateDataInfo())
    pub
  }


  def createOAF(inputParams: ((MagPapers, MagPaperWithAuthorList), MagPaperAbstract)): Publication = {

    val paper = inputParams._1._1
    val authors = inputParams._1._2
    val description = inputParams._2

    val pub = new Publication
    pub.setPid(List(createSP(paper.Doi.toLowerCase, "doi", PID_TYPES)).asJava)
    pub.setOriginalId(List(paper.PaperId.toString, paper.Doi.toLowerCase).asJava)

    //Set identifier as {50|60} | doiboost____::md5(DOI)
    pub.setId(generateIdentifier(pub, paper.Doi.toLowerCase))

    val mainTitles = createSP(paper.PaperTitle, "main title", "dnet:dataCite_title")
    val originalTitles = createSP(paper.OriginalTitle, "alternative title", "dnet:dataCite_title")
    pub.setTitle(List(mainTitles, originalTitles).asJava)

    pub.setSource(List(asField(paper.BookTitle)).asJava)


    if (description != null) {
      pub.setDescription(List(asField(description.IndexedAbstract)).asJava)
    }


    val authorsOAF = authors.authors.map { f: MagAuthorAffiliation =>

      val a: eu.dnetlib.dhp.schema.oaf.Author = new eu.dnetlib.dhp.schema.oaf.Author

      a.setFullname(f.author.DisplayName.get)

      if(f.affiliation!= null)
        a.setAffiliation(List(asField(f.affiliation)).asJava)


      a.setPid(List(createSP(s"https://academic.microsoft.com/#/detail/${f.author.AuthorId}", "URL", PID_TYPES)).asJava)

      a

    }


    if (paper.Date != null) {
      pub.setDateofacceptance(asField(paper.Date.toString))
    }

    pub.setAuthor(authorsOAF.asJava)


    pub

  }


  def convertInvertedIndexString(json_input: String): String = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json: json4s.JValue = parse(json_input)
    val idl = (json \ "IndexLength").extract[Int]
    if (idl > 0) {
      val res = Array.ofDim[String](idl)

      val iid = (json \ "InvertedIndex").extract[Map[String, List[Int]]]

      for {(k: String, v: List[Int]) <- iid} {
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
