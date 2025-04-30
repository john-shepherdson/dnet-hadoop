package eu.dnetlib.dhp.utils

import eu.dnetlib.dhp.common.enrichment.Constants._
import eu.dnetlib.dhp.schema.oaf.{Author, StructuredProperty}
import eu.dnetlib.dhp.schema.sx.OafUtils
import eu.openaire.common.author.{AuthorMatch, AuthorMatcherStep, AuthorMatchers}

import java.util
import java.util.Optional
import java.util.function.{BiFunction, Predicate}
import scala.beans.BeanProperty
import scala.collection.JavaConverters._

case class OrcidAuthor(
  @BeanProperty var orcid: String,
  @BeanProperty var familyName: String,
  @BeanProperty var givenName: String,
  @BeanProperty var creditName: String,
  @BeanProperty var otherNames: java.util.List[String]
) {
  def this() = this("null", "null", "null", "null", null)
}

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
  @BeanProperty var author_matched: java.util.List[AuthorMatch[Author, OrcidAuthor]],
  @BeanProperty var author_unmatched: java.util.List[Author],
  @BeanProperty var orcid_unmatched: java.util.List[OrcidAuthor]
)

object ORCIDAuthorEnricher extends Serializable {

  def enrichOrcid(
    id: String,
    graph_authors: java.util.List[Author],
    orcid_authors: java.util.List[OrcidAuthor],
    classid: String,
    provenance: String
  ): ORCIDAuthorEnricherResult = {
    // Author enriching strategy:
    // 1) create a copy of graph author list in unmatched_authors
    // 2) find best match in unmatched_authors, remove it from unmatched_authors and enrich it so
    //     that the enrichment is reflected in  graph_authors (they share author instances).
    //     Do not match in case of ambiguity: two authors match and at least one of them has affiliation string
    // 3) repeat (2) till the end of the list and then with different matching algorithms that have decreasing
    //    trust in their output
    // At the end unmatched_authors will contain authors not matched with any of the matching algos
    val hasAffiliations = new Predicate[util.List[AuthorMatch[Author, OrcidAuthor]]] {
      override def test(t: util.List[AuthorMatch[Author, OrcidAuthor]]): Boolean = {
        val baseAffiliations = t.get(0).getBaseAuthor.getRawAffiliationString

        if (baseAffiliations == null || baseAffiliations.isEmpty)
          t.asScala.exists(m => m.getBaseAuthor.getRawAffiliationString != null && !m.getBaseAuthor.getRawAffiliationString.isEmpty)
        else
          t.asScala.exists(m =>
            m.getBaseAuthor.getRawAffiliationString == null ||
            m.getBaseAuthor.getRawAffiliationString.size() != baseAffiliations.size()
            || !baseAffiliations.containsAll(m.getBaseAuthor.getRawAffiliationString)
          )

      }
    }

    val authorFullNameExtractor = new java.util.function.Function[Author, String] {
      override def apply(author: Author): String = {
        author.getFullname
      }
    }

    val orcidFullNameExtractor = new java.util.function.Function[OrcidAuthor, String] {
      override def apply(orcid: OrcidAuthor): String = {
        orcid.givenName + " " + orcid.familyName
      }
    }

    val result = AuthorMatchers.findMatches(
      graph_authors,
      orcid_authors,
      util.Arrays.asList(
        // Look after exact fullname match, reconstruct ORCID fullname as givenName + familyName
        AuthorMatcherStep
          .stringIgnoreCaseMatcher(authorFullNameExtractor, orcidFullNameExtractor)
          .name("fullName")
          .exclusionPredicate(hasAffiliations)
          .build,
        // Look after exact reversed fullname match, reconstruct ORCID fullname as familyName + givenName
        AuthorMatcherStep
          .stringIgnoreCaseMatcher(
            authorFullNameExtractor,
            new java.util.function.Function[OrcidAuthor, String] {
              override def apply(orcid: OrcidAuthor): String = {
                orcid.familyName + " " + orcid.givenName
              }
            }
          )
          .name("reversedFullName")
          .exclusionPredicate(hasAffiliations)
          .build,
        // split author names in tokens, order the tokens, then check for matches of full tokens or abbreviations
        AuthorMatcherStep
          .abbreviationsMatcher(authorFullNameExtractor, orcidFullNameExtractor)
          .name("orderedTokens")
          .exclusionPredicate(hasAffiliations)
          .build,
        // look after exact matches of ORCID creditName
        AuthorMatcherStep
          .stringIgnoreCaseMatcher(
            authorFullNameExtractor,
            new java.util.function.Function[OrcidAuthor, String] {
              override def apply(orcid: OrcidAuthor): String = {
                orcid.creditName
              }
            }
          )
          .name("creditName")
          .exclusionPredicate(hasAffiliations)
          .build,
        // look after exact matches in  ORCID otherNames
        new AuthorMatcherStep.Builder[Author, OrcidAuthor]()
          .name("otherNames")
          .matchingFunc(new BiFunction[Author, OrcidAuthor, Optional[AuthorMatch[Author, OrcidAuthor]]] {
            override def apply(author: Author, orcid: OrcidAuthor): Optional[AuthorMatch[Author, OrcidAuthor]] = {
              if (
                orcid.otherNames != null && orcid.otherNames.asScala
                  .exists(otherName => AuthorMatchers.matchEqualsIgnoreCase(author.getFullname, otherName))
              )
                Optional.of(AuthorMatch.of(author, orcid, 1))
              else
                Optional.empty()
            }
          })
          .exclusionPredicate(hasAffiliations)
          .build()
      )
    )

    val unmatched_authors = new util.ArrayList[Author](graph_authors)
    val unmatched_orcid = new util.ArrayList[OrcidAuthor](orcid_authors)

    // enrichment
    result.asScala.foreach(m => {
      unmatched_authors.remove(m.getBaseAuthor)
      unmatched_orcid.remove(m.getEnrichingAuthor)

      // Propagate ORCID ID from ORCID record to graph author
      if (m.getBaseAuthor.getPid == null) {
        m.getBaseAuthor.setPid(new util.ArrayList[StructuredProperty]())
      }

      val orcidPID = OafUtils.createSP(m.getEnrichingAuthor.orcid, classid, classid)
      orcidPID.setDataInfo(OafUtils.generateDataInfo())
      if (provenance.equalsIgnoreCase(PROPAGATION_DATA_INFO_TYPE)) {
        orcidPID.getDataInfo.setInferenceprovenance(PROPAGATION_DATA_INFO_TYPE);
        orcidPID.getDataInfo.setInferred(true);
        orcidPID.getDataInfo.setProvenanceaction(
          OafUtils.createQualifier(
            PROPAGATION_ORCID_TO_RESULT_FROM_SEM_REL_CLASS_ID,
            PROPAGATION_ORCID_TO_RESULT_FROM_SEM_REL_CLASS_NAME
          )
        )
      } else
        orcidPID.getDataInfo.setProvenanceaction(
          OafUtils.createQualifier(provenance, provenance)
        )

      m.getBaseAuthor.getPid.add(orcidPID)
    })

    ORCIDAuthorEnricherResult(
      id,
      graph_authors,
      result,
      unmatched_authors,
      orcid_authors
    )
  }
}
