package eu.dnetlib.dhp.enrich.orcid

import eu.dnetlib.pace.util.AuthorMatchers.matchOrderedTokenAndAbbreviations
import org.junit.jupiter.api.Assertions.{assertFalse, assertTrue}
import org.junit.jupiter.api.Test

class ORCIDAuthorMatchersTest {

  @Test def testShortNames(): Unit = {
    assertTrue(matchOrderedTokenAndAbbreviations("Lasagni Mariozzi Federico", "Lasagni F. Mariozzi"))
  }

  @Test def testInvertedNames(): Unit = {
    assertTrue(matchOrderedTokenAndAbbreviations("Andrea, Paolo Marcello", "Marcello Paolo, Andrea"))
  }

  @Test def testHomonymy(): Unit = {
    assertTrue(matchOrderedTokenAndAbbreviations("Jang Myung Lee", "J Lee"))
  }

  @Test def testAmbiguousShortNames(): Unit = {
    assertFalse(matchOrderedTokenAndAbbreviations("P. Mariozzi", "M. Paolozzi"))
  }

  @Test def testNonMatches(): Unit = {
    assertFalse(matchOrderedTokenAndAbbreviations("Giovanni Paolozzi", "Francesco Paolozzi"))
    assertFalse(matchOrderedTokenAndAbbreviations("G. Paolozzi", "F. Paolozzi"))
  }

  @Test def testChineseNames(): Unit = {
    assertTrue(matchOrderedTokenAndAbbreviations("孙林 Sun Lin", "Sun Lin"))
    // assertTrue(AuthorsMatchRevised.compare("孙林 Sun Lin", "孙林")); // not yet implemented
  }

  @Test def testDocumentationNames(): Unit = {
    assertTrue(matchOrderedTokenAndAbbreviations("James C. A. Miller-Jones", "James Antony Miller-Jones"))
  }

  @Test def testDocumentationNames2(): Unit = {
    assertTrue(matchOrderedTokenAndAbbreviations("James C. A. Miller-Jones", "James Antony Miller Jones"))
  }
}
