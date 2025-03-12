package eu.dnetlib.dhp.utils


import eu.openaire.common.author.AuthorMatchers.matchOrderedTokenAndAbbreviations
import org.junit.jupiter.api.Assertions.{assertFalse, assertTrue}
import org.junit.jupiter.api.Test

class AuthorMatchersTest {

  @Test def testShortNames(): Unit = {
    assertTrue(matchOrderedTokenAndAbbreviations("Lasagni Mariozzi Federico", "Lasagni F. Mariozzi").isPresent)
  }

  @Test def testInvertedNames(): Unit = {
    assertTrue(matchOrderedTokenAndAbbreviations("Andrea, Paolo Marcello", "Marcello Paolo, Andrea").isPresent)
  }

  @Test def testHomonymy(): Unit = {
    assertTrue(matchOrderedTokenAndAbbreviations("Jang Myung Lee", "J Lee").isPresent)
  }

  @Test def testAmbiguousShortNames(): Unit = {
    assertFalse(matchOrderedTokenAndAbbreviations("P. Mariozzi", "M. Paolozzi").isPresent)
  }

  @Test def testNonMatches(): Unit = {
    assertFalse(matchOrderedTokenAndAbbreviations("Giovanni Paolozzi", "Francesco Paolozzi").isPresent)
    assertFalse(matchOrderedTokenAndAbbreviations("G. Paolozzi", "F. Paolozzi").isPresent)
  }

  @Test def testChineseNames(): Unit = {
    assertTrue(matchOrderedTokenAndAbbreviations("孙林 Sun Lin", "Sun Lin").isPresent)
    // assertTrue(AuthorsMatchRevised.compare("孙林 Sun Lin", "孙林")); // not yet implemented
  }

  @Test def testDocumentationNames(): Unit = {
    assertTrue(matchOrderedTokenAndAbbreviations("James C. A. Miller-Jones", "James Antony Miller-Jones").isPresent)
  }

  @Test def testDocumentationNames2(): Unit = {
    assertTrue(matchOrderedTokenAndAbbreviations("James C. A. Miller-Jones", "James Antony Miller Jones").isPresent)
  }
}
