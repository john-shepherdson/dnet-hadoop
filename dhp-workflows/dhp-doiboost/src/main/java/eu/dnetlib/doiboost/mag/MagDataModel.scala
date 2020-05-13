package eu.dnetlib.doiboost.mag


import org.json4s
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse


case class Papers(PaperId:Long, Rank:Integer, Doi:String,
                  DocType:String, PaperTitle:String, OriginalTitle:String,
                  BookTitle:String, Year:Option[Integer], Date:Option[java.sql.Timestamp], Publisher:String,
                  JournalId:Option[Long], ConferenceSeriesId:Option[Long], ConferenceInstanceId:Option[Long],
                  Volume:String, Issue:String, FirstPage:String, LastPage:String,
                  ReferenceCount:Option[Long], CitationCount:Option[Long], EstimatedCitation:Option[Long],
                  OriginalVenue:String, FamilyId:Option[Long], CreatedDate:java.sql.Timestamp) {}


case class PaperAbstract(PaperId:Long,IndexedAbstract:String) {}



case object ConversionUtil {



  def transformPaperAbstract(input:PaperAbstract) : PaperAbstract = {
    PaperAbstract(input.PaperId, convertInvertedIndexString(input.IndexedAbstract))
  }



  def convertInvertedIndexString(json_input:String) :String = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json: json4s.JValue = parse(json_input)



    val idl = (json \ "IndexLength").extract[Int]

    if (idl > 0) {
      val res = Array.ofDim[String](idl)

      val iid = (json \ "InvertedIndex").extract[Map[String, List[Int]]]

      for {(k:String,v:List[Int]) <- iid}{
        v.foreach(item => res(item) = k)
      }
      return res.mkString(" ")

    }
    ""
  }
}
