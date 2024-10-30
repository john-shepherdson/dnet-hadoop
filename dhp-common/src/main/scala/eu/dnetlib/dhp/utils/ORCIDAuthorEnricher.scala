package eu.dnetlib.dhp.utils

import eu.dnetlib.dhp.schema.common.ModelConstants
import eu.dnetlib.dhp.schema.oaf.{Author, StructuredProperty}
import eu.dnetlib.dhp.schema.sx.OafUtils

import java.util
import scala.beans.BeanProperty
import scala.collection.JavaConverters._
import scala.util.control.Breaks.{break, breakable}

case class OrcidAuthor(
  @BeanProperty var orcid: String,
  @BeanProperty var familyName: String,
  @BeanProperty var givenName: String,
  @BeanProperty var creditName: String,
  @BeanProperty var otherNames: java.util.List[String]
) {
  def this() = this("null", "null", "null", "null", null)
}

case class MatchedAuthors(
                           @BeanProperty var author: Author,
                           @BeanProperty var orcid: OrcidAuthor,
                           @BeanProperty var `type`: String
)

case class MatchData(
  @BeanProperty var id: String,
  @BeanProperty var graph_authors: java.util.List[Author],
  @BeanProperty var orcid_authors: java.util.List[OrcidAuthor]
) {
  def this() = this("null", null, null)
}

case class ORCIDAuthorEnricherResult(
  @BeanProperty var id: String,
  @BeanProperty var enriched_author: java.util.List[Author],
  @BeanProperty var author_matched: java.util.List[MatchedAuthors],
  @BeanProperty var author_unmatched: java.util.List[Author],
  @BeanProperty var orcid_unmatched: java.util.List[OrcidAuthor]
)

object ORCIDAuthorEnricher extends Serializable {

  def enrichOrcid(
    id: String,
    graph_authors: java.util.List[Author],
    orcid_authors: java.util.List[OrcidAuthor]
  ): ORCIDAuthorEnricherResult = {
    // Author enriching strategy:
    // 1) create a copy of graph author list in unmatched_authors
    // 2) find best match in unmatched_authors, remove it from unmatched_authors and enrich it so
    //     that the enrichment is reflected in  graph_authors (they share author instances)
    // 3) repeat (2) till the end of the list and then with different matching algorithms that have decreasing
    //    trust in their output
    // At the end unmatched_authors will contain authors not matched with any of the matching algos
    val unmatched_authors = new util.ArrayList[Author](graph_authors)

    val matches = {
      // Look after exact fullname match, reconstruct ORCID fullname as givenName + familyName
      extractAndEnrichMatches(
        unmatched_authors,
        orcid_authors,
        (author, orcid) =>
          AuthorMatchers.matchEqualsIgnoreCase(author.getFullname, orcid.givenName + " " + orcid.familyName),
        "fullName"
      ) ++
      // Look after exact reversed fullname match, reconstruct ORCID fullname as familyName + givenName
      extractAndEnrichMatches(
        unmatched_authors,
        orcid_authors,
        (author, orcid) =>
          AuthorMatchers.matchEqualsIgnoreCase(author.getFullname, orcid.familyName + " " + orcid.givenName),
        "reversedFullName"
      ) ++
      // split author names in tokens, order the tokens, then check for matches of full tokens or abbreviations
      extractAndEnrichMatches(
        unmatched_authors,
        orcid_authors,
        (author, orcid) =>
          AuthorMatchers
            .matchOrderedTokenAndAbbreviations(author.getFullname, orcid.givenName + " " + orcid.familyName),
        "orderedTokens"
      ) ++
      // look after exact matches of ORCID creditName
      extractAndEnrichMatches(
        unmatched_authors,
        orcid_authors,
        (author, orcid) => AuthorMatchers.matchEqualsIgnoreCase(author.getFullname, orcid.creditName),
        "creditName"
      ) ++
      // look after exact matches in  ORCID otherNames
      extractAndEnrichMatches(
        unmatched_authors,
        orcid_authors,
        (author, orcid) =>
          orcid.otherNames != null && AuthorMatchers.matchOtherNames(author.getFullname, orcid.otherNames.asScala),
        "otherNames"
      )
    }

    ORCIDAuthorEnricherResult(id, graph_authors, matches.asJava, unmatched_authors, orcid_authors)
  }

  private def extractAndEnrichMatches(
                                       graph_authors: java.util.List[Author],
                                       orcid_authors: java.util.List[OrcidAuthor],
                                       matchingFunc: (Author, OrcidAuthor) => Boolean,
                                       matchName: String
  ) = {
    val matched = scala.collection.mutable.ArrayBuffer.empty[MatchedAuthors]

    if (graph_authors != null && !graph_authors.isEmpty) {
      val ait = graph_authors.iterator

      while (ait.hasNext) {
        val author = ait.next()
        val oit = orcid_authors.iterator

        breakable {
          while (oit.hasNext) {
            val orcid = oit.next()

            if (matchingFunc(author, orcid)) {
              ait.remove()
              oit.remove()
              matched += MatchedAuthors(author, orcid, matchName)

              if (author.getPid == null) {
                author.setPid(new util.ArrayList[StructuredProperty]())
              }

              val orcidPID = OafUtils.createSP(orcid.orcid, ModelConstants.ORCID, ModelConstants.ORCID)
              orcidPID.setDataInfo(OafUtils.generateDataInfo())
              orcidPID.getDataInfo.setProvenanceaction(
                OafUtils.createQualifier("ORCID_ENRICHMENT", "ORCID_ENRICHMENT")
              )

              author.getPid.add(orcidPID)

              break()
            }
          }
        }
      }
    }

    matched
  }

}
