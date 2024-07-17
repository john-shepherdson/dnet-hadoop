
package eu.dnetlib.pace.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.*;

public class PaceFunctionTest extends AbstractPaceFunctions {

	private final static String TEST_STRING = "Toshiba NB550D: è un netbook su piattaforma AMD Fusion⁽¹²⁾.";

	@Test
	public void normalizePidTest() {

		assertEquals("identifier", normalizePid("IdentifIer"));
		assertEquals("10.1109/tns.2015.2493347", normalizePid("10.1109/TNS.2015.2493347"));
		assertEquals("10.0001/testdoi", normalizePid("http://dx.doi.org/10.0001/testDOI"));
		assertEquals("10.0001/testdoi", normalizePid("https://dx.doi.org/10.0001/testDOI"));
	}

	@Test
	public void filterAllStopwordsTest() {

		assertEquals("universita politecnica marche", filterAllStopWords("universita politecnica delle marche"));
	}

	@Test
	public void normalizeTest() {
		assertEquals("universitat", normalize("Universität"));

		System.out.println(normalize("İstanbul Ticarət Universiteti"));
	}

	@Test
	public void cleanupTest() {
		assertEquals("istanbul ticaret universiteti", cleanup("İstanbul Ticarət Universiteti"));

		System.out.println("cleaned up     : " + cleanup(TEST_STRING));
	}

	@Test
	public void testGetNumbers() {
		System.out.println("Numbers        : " + getNumbers(TEST_STRING));
	}

	@Test
	public void testRemoveSymbols() {
		System.out.println("Without symbols: " + removeSymbols(TEST_STRING));
	}

	@Test
	public void testFixAliases() {
		System.out.println("Fixed aliases  : " + fixAliases(TEST_STRING));
	}

	@Test
	public void countryInferenceTest() {
		assertEquals("IT", countryInference("UNKNOWN", "Università di Bologna"));
		assertEquals("UK", countryInference("UK", "Università di Bologna"));
		assertEquals("IT", countryInference("UNKNOWN", "Universiteé de Naples"));
		assertEquals("UNKNOWN", countryInference("UNKNOWN", "Università del Lavoro"));
	}

	@Test
	public void cityInferenceTest() {
		assertEquals("universita city::3181928", cityInference("Università di Bologna"));
		assertEquals("university city::3170647", cityInference("University of Pisa"));
		assertEquals("universita", cityInference("Università del lavoro"));
		assertEquals("universita city::3173331 city::3169522", cityInference("Università di Modena e Reggio Emilia"));
	}

	@Test
	public void keywordInferenceTest() {
		assertEquals("key::41 turin", keywordInference("Polytechnic University of Turin"));
		assertEquals("key::41 torino", keywordInference("POLITECNICO DI TORINO"));
		assertEquals(
			"key::1 key::60 key::81 milano bergamo",
			keywordInference("Universita farmaceutica culturale di milano bergamo"));
		assertEquals("key::1 key::1 milano milano", keywordInference("universita universita milano milano"));
		assertEquals(
			"key::10 kapodistriako panepistemio athenon",
			keywordInference("Εθνικό και Καποδιστριακό Πανεπιστήμιο Αθηνών"));
	}

	@Test
	public void cityKeywordInferenceTest() {
		assertEquals("key::41 city::3165524", cityKeywordInference("Polytechnic University of Turin"));
		assertEquals("key::41 city::3165524", cityKeywordInference("POLITECNICO DI TORINO"));
		assertEquals(
			"key::1 key::60 key::81 city::3173435 city::3182164",
			cityKeywordInference("Universita farmaceutica culturale di milano bergamo"));
		assertEquals(
			"key::1 key::1 city::3173435 city::3173435", cityKeywordInference("universita universita milano milano"));
		assertEquals(
			"key::10 kapodistriako panepistemio city::264371",
			cityKeywordInference("Εθνικό και Καποδιστριακό Πανεπιστήμιο Αθηνών"));
	}

}
