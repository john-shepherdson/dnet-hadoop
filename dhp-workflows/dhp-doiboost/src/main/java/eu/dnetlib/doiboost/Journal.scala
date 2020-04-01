package eu.dnetlib.doiboost



case class Journal(
                    JournalId: Long,
                    Rank: Int,
                    NormalizedName: String,
                    DisplayName: String,
                    Issn: String,
                    Publisher: String,
                    Webpage: String,
                    PaperCount: Long,
                    CitationCount: Long,
                    CreatedDate: String
                  )
