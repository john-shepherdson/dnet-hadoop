
package eu.dnetlib.pace.comparators;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.*;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import eu.dnetlib.pace.AbstractPaceTest;
import eu.dnetlib.pace.clustering.NGramUtils;
import eu.dnetlib.pace.config.DedupConfig;
import eu.dnetlib.pace.tree.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ComparatorTest extends AbstractPaceTest {

	private Map<String, String> params;
	private DedupConfig conf;

	@BeforeAll
	public void setup() {
		conf = DedupConfig
			.load(readFromClasspath("/eu/dnetlib/pace/config/organization.current.conf.json", ComparatorTest.class));
	}

	@BeforeEach
	public void beforeEachTest() {
		params = new HashMap<>();
		params.put("weight", "1.0");
		params.put("surname_th", "0.99");
		params.put("name_th", "0.95");
		params.put("jpath_value", "$.value");
		params.put("jpath_classid", "$.qualifier.classid");
		params.put("codeRegex", "key::\\d+");
	}

	@Test
	public void testCleanForSorting() {
		NGramUtils utils = new NGramUtils();
		System.out.println(utils.cleanupForOrdering("University of Pisa"));
	}

	@Test
	public void codeMatchTest() {
		CodeMatch codeMatch = new CodeMatch(params);

		// both names with no codes
		assertEquals(1.0, codeMatch.distance("testing1", "testing2", conf));

		// one of the two names with no codes
		assertEquals(-1.0, codeMatch.distance("testing1 key::1", "testing", conf));

		// both names with codes (same)
		assertEquals(1.0, codeMatch.distance("testing1 key::1", "testing2 key::1", conf));

		// both names with codes (different)
		assertEquals(0.0, codeMatch.distance("testing1 key::1", "testing2 key::2", conf));

		// both names with codes (1 same, 1 different)
		assertEquals(0.5, codeMatch.distance("key::1 key::2 testing1", "key::1 testing", conf));

	}

	@Test
	public void datasetVersionCodeMatchTest() {

		params.put("codeRegex", "(?=[\\w-]*[a-zA-Z])(?=[\\w-]*\\d)[\\w-]+");
		CodeMatch codeMatch = new CodeMatch(params);

		// names have different codes
		assertEquals(
			0.0,
			codeMatch
				.distance(
					"physical oceanography at ctd station june 1998 ev02a",
					"physical oceanography at ctd station june 1998 ir02", conf));

		// names have same code
		assertEquals(
			1.0,
			codeMatch
				.distance(
					"physical oceanography at ctd station june 1998 ev02a",
					"physical oceanography at ctd station june 1998 ev02a", conf));

		// code is not in both names
		assertEquals(
			-1,
			codeMatch
				.distance(
					"physical oceanography at ctd station june 1998",
					"physical oceanography at ctd station june 1998 ev02a", conf));
		assertEquals(
			1.0,
			codeMatch
				.distance(
					"physical oceanography at ctd station june 1998", "physical oceanography at ctd station june 1998",
					conf));
	}

	@Test
	public void listContainsMatchTest() {

		List<String> a = createFieldList(Arrays.asList("Article", "Publication", "ORP"), "instanceType");
		List<String> b = createFieldList(Arrays.asList("Publication", "Article", "ORP"), "instanceType");

		params.put("string", "Article");
		params.put("bool", "XOR");
		params.put("caseSensitive", "false");

		ListContainsMatch listContainsMatch = new ListContainsMatch(params);

		assertEquals(0.0, listContainsMatch.compare(a, b, conf));

		params.put("string", "Article");
		params.put("bool", "AND");
		params.put("caseSensitive", "false");

		listContainsMatch = new ListContainsMatch(params);

		assertEquals(1.0, listContainsMatch.compare(a, b, conf));
	}

	@Test
	public void stringContainsMatchTest() {

		params.put("string", "openorgs");
		params.put("aggregator", "XOR");
		params.put("caseSensitive", "false");

		StringContainsMatch stringContainsMatch = new StringContainsMatch(params);

		assertEquals(0.0, stringContainsMatch.distance("openorgs", "openorgs", conf));

		params.put("string", "openorgs");
		params.put("aggregator", "AND");
		params.put("caseSensitive", "false");

		stringContainsMatch = new StringContainsMatch(params);

		assertEquals(1.0, stringContainsMatch.distance("openorgs", "openorgs", conf));
	}

	@Test
	public void numbersMatchTest() {
		final NumbersMatch numbersMatch = new NumbersMatch(params);

		assertEquals(0.0, numbersMatch.distance("University of Rennes 2", "Universita di Rennes 7", conf));
		assertEquals(1.0, numbersMatch.distance("Universit<C9><U3> de Rennes 2", "Universita di Rennes 2", conf));
	}

	@Test
	public void romansMatchTest() {

		final RomansMatch romansMatch = new RomansMatch(params);

		assertEquals(-1.0, romansMatch.distance("University of Paris X", "Universita di Parigi", conf));
		assertEquals(0.0, romansMatch.distance("University of Paris IX", "University of Paris X", conf));
		assertEquals(1.0, romansMatch.distance("University of Paris VII", "University of Paris VII", conf));
	}

	@Test
	public void jaroWinklerLegalnameTest() {

		final JaroWinklerLegalname jaroWinklerLegalname = new JaroWinklerLegalname(params);

		double result = jaroWinklerLegalname
			.distance("AT&T (United States)", "United States key::2 key::1", conf);
		System.out.println("result = " + result);

		result = jaroWinklerLegalname.distance("NOAA - Servicio Meteorol\\u00f3gico Nacional", "NOAA - NWS", conf);
		System.out.println("result = " + result);

	}

	@Test
	public void jaroWinklerTest() {

		final JaroWinkler jaroWinkler = new JaroWinkler(params);

		double result = jaroWinkler.distance("Sofia", "Sofìa", conf);
		System.out.println("result = " + result);

		result = jaroWinkler.distance("University of Victoria Dataverse", "University of Windsor Dataverse", conf);
		System.out.println("result = " + result);

		result = jaroWinkler.distance("Victoria Dataverse", "Windsor Dataverse", conf);
		System.out.println("result = " + result);

	}

	@Test
	public void levensteinTitleTest() {

		final LevensteinTitle levensteinTitle = new LevensteinTitle(params);

		double result = levensteinTitle
			.distance(
				"Degradation of lignin β‐aryl ether units in Arabidopsis thaliana expressing LigD, LigF and LigG from Sphingomonas paucimobilis SYK‐6",
				"Degradation of lignin β-aryl ether units in <i>Arabidopsis thaliana</i> expressing <i>LigD</i>, <i>LigF</i> and <i>LigG</i> from <i>Sphingomonas paucimobilis</i> SYK-6",
				conf);

		System.out.println("result = " + result);
	}

	@Test
	public void levensteinTest() {
		final Levenstein levenstein = new Levenstein(params);

		double result = levenstein.distance("la bruzzo", "la bruzzo", conf);
		System.out.println("result = " + result);
	}

	@Test
	public void instanceTypeMatchTest() {

		final InstanceTypeMatch instanceTypeMatch = new InstanceTypeMatch(params);

		List<String> a = createFieldList(Arrays.asList("Article", "Article", "Article"), "instanceType");
		List<String> b = createFieldList(Arrays.asList("Article", "Article", "Article"), "instanceType");
		double result = instanceTypeMatch.compare(a, b, conf);

		assertEquals(1.0, result);

		List<String> c = createFieldList(
			Arrays.asList("Conference object", "Conference object", "Conference object"), "instanceType");
		result = instanceTypeMatch.compare(c, b, conf);

		assertEquals(1.0, result);

		List<String> d = createFieldList(
			Arrays.asList("Master thesis", "Master thesis", "Master thesis"), "instanceType");
		List<String> e = createFieldList(
			Arrays.asList("Bachelor thesis", "Bachelor thesis", "Bachelor thesis"), "instanceType");
		result = instanceTypeMatch.compare(d, e, conf);

		assertEquals(1.0, result);

		List<String> g = createFieldList(Arrays.asList("Software Paper", "Software Paper"), "instanceType");
		result = instanceTypeMatch.compare(e, g, conf);

		assertEquals(0.0, result);

		List<String> h = createFieldList(Arrays.asList("Other literature type", "Article"), "instanceType");
		result = instanceTypeMatch.compare(a, h, conf);

		assertEquals(1.0, result);
	}

	@Test
	public void authorsMatchTest() {

		AuthorsMatch authorsMatch = new AuthorsMatch(params);

		List<String> a = createFieldList(
			Arrays.asList("La Bruzzo, Sandro", "Atzori, Claudio", "De Bonis, Michele"), "authors");
		List<String> b = createFieldList(Arrays.asList("Atzori, C.", "La Bruzzo, S.", "De Bonis, M."), "authors");
		double result = authorsMatch.compare(a, b, conf);

		assertEquals(1.0, result);

		List<String> c = createFieldList(Arrays.asList("Manghi, Paolo"), "authors");
		List<String> d = createFieldList(Arrays.asList("Manghi, Pasquale"), "authors");
		result = authorsMatch.compare(c, d, conf);

		assertEquals(0.0, result);

		params.put("mode", "surname");
		authorsMatch = new AuthorsMatch(params);
		result = authorsMatch.compare(c, d, conf);

		assertEquals(1.0, result);

		List<String> e = createFieldList(Arrays.asList("Manghi, Paolo", "Atzori, Claudio"), "authors");
		result = authorsMatch.compare(a, e, conf);

		assertEquals(0.25, result);

		List<String> f = createFieldList(new ArrayList<>(), "authors");
		result = authorsMatch.compare(f, f, conf);
		System.out.println("result = " + result);

	}

	@Test
	public void jsonListMatch() {

		JsonListMatch jsonListMatch = new JsonListMatch(params);

		List<String> a = createFieldList(
			Arrays
				.asList(
					"{\"datainfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":null,\"inferred\":false,\"invisible\":false,\"provenanceaction\":{\"classid\":\"sysimport:actionset\",\"classname\":\"Harvested\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"0.9\"},\"qualifier\":{\"classid\":\"grid\",\"classname\":\"GRID Identifier\",\"schemeid\":\"dnet:pid_types\",\"schemename\":\"dnet:pid_types\"},\"value\":\"grid_1\"}",
					"{\"datainfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":null,\"inferred\":false,\"invisible\":false,\"provenanceaction\":{\"classid\":\"sysimport:actionset\",\"classname\":\"Harvested\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"0.9\"},\"qualifier\":{\"classid\":\"ror\",\"classname\":\"Research Organization Registry\",\"schemeid\":\"dnet:pid_types\",\"schemename\":\"dnet:pid_types\"},\"value\":\"ror_1\"}"),
			"authors");
		List<String> b = createFieldList(
			Arrays
				.asList(
					"{\"datainfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"\",\"inferred\":false,\"invisible\":false,\"provenanceaction\":{\"classid\":\"sysimport:crosswalk:repository\",\"classname\":\"Harvested\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"0.9\"},\"qualifier\":{\"classid\":\"grid\",\"classname\":\"GRID Identifier\",\"schemeid\":\"dnet:pid_types\",\"schemename\":\"dnet:pid_types\"},\"value\":\"grid_1\"}",
					"{\"datainfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"\",\"inferred\":false,\"invisible\":false,\"provenanceaction\":{\"classid\":\"sysimport:crosswalk:repository\",\"classname\":\"Harvested\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"0.9\"},\"qualifier\":{\"classid\":\"ror\",\"classname\":\"Research Organization Registry\",\"schemeid\":\"dnet:pid_types\",\"schemename\":\"dnet:pid_types\"},\"value\":\"ror_2\"}",
					"{\"datainfo\":{\"deletedbyinference\":false,\"inferenceprovenance\":\"\",\"inferred\":false,\"invisible\":false,\"provenanceaction\":{\"classid\":\"user:claim\",\"classname\":\"Linked by user\",\"schemeid\":\"dnet:provenanceActions\",\"schemename\":\"dnet:provenanceActions\"},\"trust\":\"0.9\"},\"qualifier\":{\"classid\":\"isni\",\"classname\":\"ISNI Identifier\",\"schemeid\":\"dnet:pid_types\",\"schemename\":\"dnet:pid_types\"},\"value\":\"isni_1\"}"),
			"authors");

		double result = jsonListMatch.compare(a, b, conf);

		assertEquals(0.25, result);

		params.put("mode", "count");
		jsonListMatch = new JsonListMatch(params);
		result = jsonListMatch.compare(a, b, conf);

		assertEquals(1.0, result);

		params.put("mode", "type");
		jsonListMatch = new JsonListMatch(params);
		result = jsonListMatch.compare(a, b, conf);

		assertEquals(0.5, result);

	}

	@Test
	public void domainExactMatch() {

		DomainExactMatch domainExactMatch = new DomainExactMatch(params);
		String a = url("http://www.flowrepository.org");
		String b = url("http://flowrepository.org/");

		double compare = domainExactMatch.compare(a, b, conf);
		System.out.println("compare = " + compare);

	}

	@Test
	public void cosineSimilarity() {

		CosineSimilarity cosineSimilarity = new CosineSimilarity(params);

		double[] a = new double[] {
			1, 2, 3
		};
		double[] b = new double[] {
			1, 2, 3
		};

		double compare = cosineSimilarity.compare(a, b, conf);

		System.out.println("compare = " + compare);
	}

	@Test
	public void countryMatch() {

		CountryMatch countryMatch = new CountryMatch(params);

		double result = countryMatch.distance("UNKNOWN", "UNKNOWN", conf);
		assertEquals(-1.0, result);

		result = countryMatch.distance("CL", "UNKNOWN", conf);
		assertEquals(-1.0, result);

		result = countryMatch.distance("CL", "IT", conf);
		assertEquals(0.0, result);

		result = countryMatch.distance("CL", "CL", conf);
		assertEquals(1.0, result);

	}

	@Test
	public void dateMatch() {

		DateRange dateRange = new DateRange(params);

		double result = dateRange.distance("2021-05-13", "2023-05-13", conf);
		assertEquals(1.0, result);

		result = dateRange.distance("2021-05-13", "2025-05-13", conf);
		assertEquals(0.0, result);

		result = dateRange.distance("", "2020-05-05", conf);
		assertEquals(-1.0, result);

		result = dateRange.distance("invalid date", "2021-05-02", conf);
		assertEquals(-1.0, result);
	}

	@Test
	public void titleVersionMatchTest() {

		TitleVersionMatch titleVersionMatch = new TitleVersionMatch(params);

		double result = titleVersionMatch
			.compare(
				"parp 2 regulates sirt 1 expression and whole body energy expenditure",
				"parp 2 regulates sirt 1 expression and whole body energy expenditure", conf);
		assertEquals(1.0, result);
	}

}
