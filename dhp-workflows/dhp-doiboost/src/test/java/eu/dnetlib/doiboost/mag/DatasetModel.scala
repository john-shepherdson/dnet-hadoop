package eu.dnetlib.doiboost.mag


case class Papers(PaperId:Long, Rank:Integer, Doi:String,
                  DocType:String, PaperTitle:String, OriginalTitle:String,
                  BookTitle:String, Year:Option[Integer], Date:Option[java.sql.Timestamp], Publisher:String,
                  JournalId:Option[Long], ConferenceSeriesId:Option[Long], ConferenceInstanceId:Option[Long],
                  Volume:String, Issue:String, FirstPage:String, LastPage:String,
                  ReferenceCount:Option[Long], CitationCount:Option[Long], EstimatedCitation:Option[Long],
                  OriginalVenue:String, FamilyId:Option[Long], CreatedDate:java.sql.Timestamp) {}




