package eu.dnetlib.dhp.enrich.orcid

import java.util.Locale
import java.util.regex.Pattern

object ORCIDAuthorMatchers {
  val SPLIT_REGEX = Pattern.compile("[\\s,\\.]+")

  val WORD_DIFF = 2

  def matchEqualsIgnoreCase(a1: String, a2: String): Boolean = {
    if (a1 == null || a2 == null)
      false
    else
      a1 == a2 || a1.toLowerCase(Locale.ROOT).equals(a2.toLowerCase(Locale.ROOT))
  }

  def matchOtherNames(fullName: String, otherNames: Seq[String]): Boolean = {
    if (otherNames != null) {
      otherNames.exists(matchEqualsIgnoreCase(fullName, _))
    } else {
      false
    }
  }

  def matchOrderedTokenAndAbbreviations(a1: String, a2: String): Boolean = {
    val p1: Array[String] = SPLIT_REGEX.split(a1.trim.toLowerCase(Locale.ROOT)).filter(_.nonEmpty).sorted
    val p2: Array[String] = SPLIT_REGEX.split(a2.trim.toLowerCase(Locale.ROOT)).filter(_.nonEmpty).sorted

    if (p1.length < 2 || p2.length < 2) return false
    if (Math.abs(p1.length - p2.length) > WORD_DIFF) return false // use alternative comparison algo

    var p1Idx: Int = 0
    var p2Idx: Int = 0
    var shortMatches: Int = 0
    var longMatches: Int = 0
    while (p1Idx < p1.length && p2Idx < p2.length) {
      val e1: String = p1(p1Idx)
      val c1: Char = e1.charAt(0)
      val e2: String = p2(p2Idx)
      val c2: Char = e2.charAt(0)
      if (c1 < c2) p1Idx += 1
      else if (c1 > c2) p2Idx += 1
      else {
        var res: Boolean = false
        if (e1.length != 1 && e2.length != 1) {
          res = e1 == e2
          longMatches += 1
        } else {
          res = true
          shortMatches += 1
        }
        if (res) {
          p1Idx += 1
          p2Idx += 1
        } else {
          val diff: Int = e1.compareTo(e2)
          if (diff < 0) p1Idx += 1
          else if (diff > 0) p2Idx += 1
        }
      }
    }
    longMatches > 0 && (shortMatches + longMatches) == Math.min(p1.length, p2.length)
  }
}
