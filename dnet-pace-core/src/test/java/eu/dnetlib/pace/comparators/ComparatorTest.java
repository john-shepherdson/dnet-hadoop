package eu.dnetlib.pace.comparators;

import eu.dnetlib.pace.clustering.NGramUtils;
import eu.dnetlib.pace.tree.CityMatch;
import eu.dnetlib.pace.tree.ContainsMatch;
import eu.dnetlib.pace.tree.JaroWinklerNormalizedName;
import eu.dnetlib.pace.config.DedupConfig;
import eu.dnetlib.pace.tree.KeywordMatch;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import eu.dnetlib.pace.common.AbstractPaceFunctions;

import java.util.HashMap;
import java.util.Map;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

public class ComparatorTest extends AbstractPaceFunctions {

	private Map<String, String> params;
	private DedupConfig conf;

	@Before
	public void setup() {
		params = new HashMap<>();
		params.put("weight", "1.0");
		conf = DedupConfig.load(readFromClasspath("/eu/dnetlib/pace/config/organization.current.conf", ComparatorTest.class));

	}

	@Test
	public void testCleanForSorting() {
		NGramUtils utils = new NGramUtils();
		System.out.println("utils = " + utils.cleanupForOrdering("University of Pisa"));
	}

	@Test
	public void cityMatchTest() {
		final CityMatch cityMatch = new CityMatch(params);

		//both names with no cities
		assertEquals(1.0, cityMatch.distance("Università", "Centro di ricerca", conf));

		//one of the two names with no cities
		assertEquals(-1.0, cityMatch.distance("Università di Bologna", "Centro di ricerca", conf));

		//both names with cities (same)
		assertEquals(1.0, cityMatch.distance("Universita di Bologna", "Biblioteca di Bologna", conf));

		//both names with cities (different)
		assertEquals(0.0, cityMatch.distance("Universita di Bologna", "Universita di Torino", conf));

		//particular cases
		assertEquals(1.0, cityMatch.distance("Free University of Bozen-Bolzano", "Università di Bolzano", conf));
		assertEquals(1.0, cityMatch.distance("Politechniki Warszawskiej (Warsaw University of Technology)", "Warsaw University of Technology", conf));
	}

	//    @Test
//    public void testJaroWinklerNormalizedName6() {
//
//        final JaroWinklerNormalizedName jaroWinklerNormalizedName = new JaroWinklerNormalizedName(params);
//        double result = jaroWinklerNormalizedName.distance("Fonds zur Förderung der wissenschaftlichen Forschung (Austrian Science Fund)", "Fonds zur Förderung der wissenschaftlichen Forschung", conf);
//
//        System.out.println("result = " + result);
//        assertTrue(result > 0.9);
//
//    }
// 	@Test
//	public void testJaroWinklerNormalizedName10(){
//
//		final JaroWinklerNormalizedName jaroWinklerNormalizedName = new JaroWinklerNormalizedName(params);
//
//		double result = jaroWinklerNormalizedName.distance("Firenze University Press", "University of Florence", conf);
//
//		System.out.println("result = " + result);
//	}

	@Test
	public void keywordMatchTest(){
		params.put("threshold", "0.4");

		final KeywordMatch keywordMatch = new KeywordMatch(params);

		assertEquals(1.0, keywordMatch.distance("Biblioteca dell'Universita di Bologna", "Università di Bologna", conf));
		assertEquals(1.0, keywordMatch.distance("Universita degli studi di Pisa", "Universita di Pisa", conf));
		assertEquals(1.0, keywordMatch.distance("Polytechnic University of Turin", "POLITECNICO DI TORINO", conf));
		assertEquals(1.0, keywordMatch.distance("Istanbul Commerce University", "İstanbul Ticarət Universiteti", conf));
	}

	@Test
	public void containsMatchTest(){

		params.put("string", "openorgs");
		params.put("bool", "XOR");
		params.put("caseSensitive", "false");

		final ContainsMatch containsMatch = new ContainsMatch(params);

		assertEquals(0.0, containsMatch.distance("openorgs", "openorgs", conf));
	}


}
