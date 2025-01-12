
package eu.dnetlib.pace.clustering;

import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.mongodb.connection.Cluster;

import eu.dnetlib.pace.AbstractPaceTest;
import eu.dnetlib.pace.common.AbstractPaceFunctions;
import eu.dnetlib.pace.config.DedupConfig;

public class ClusteringFunctionTest extends AbstractPaceTest {

	private static Map<String, Object> params;
	private static DedupConfig conf;

	@BeforeAll
	public static void setUp() throws Exception {
		params = Maps.newHashMap();
		conf = DedupConfig
			.load(
				AbstractPaceFunctions
					.readFromClasspath(
						"/eu/dnetlib/pace/config/organization.current.conf.json", ClusteringFunctionTest.class));
	}

	@Test
	public void testUrlClustering() {

		final ClusteringFunction urlClustering = new UrlClustering(params);

		final String s = "http://www.test.it/path/to/resource";
		System.out.println(s);
		System.out.println(urlClustering.apply(conf, Lists.newArrayList(s)));
	}

	@Test
	public void testNgram() {
		params.put("ngramLen", "3");
		params.put("max", "8");
		params.put("maxPerToken", "2");
		params.put("minNgramLen", "1");

		final ClusteringFunction ngram = new Ngrams(params);

		final String s = "Search for the Standard Model Higgs Boson";
		System.out.println(s);
		System.out.println(ngram.apply(conf, Lists.newArrayList(s)));
	}

	@Test
	public void testNgramPairs() {
		params.put("ngramLen", "3");
		params.put("max", "2");

		final ClusteringFunction np = new NgramPairs(params);

		final String s = "Search for the Standard Model Higgs Boson";
		System.out.println(s);
		System.out.println(np.apply(conf, Lists.newArrayList(s)));
	}

	@Test
	public void testSortedNgramPairs() {
		params.put("ngramLen", "3");
		params.put("max", "2");

		final ClusteringFunction np = new SortedNgramPairs(params);

		final String s1 = "University of Pisa";
		System.out.println(s1);
		System.out.println(np.apply(conf, Lists.newArrayList(s1)));

		final String s2 = "Pisa University";
		System.out.println(s2);
		System.out.println(np.apply(conf, Lists.newArrayList(s2)));

		final String s3 = "Parco Tecnologico Agroalimentare Umbria";
		System.out.println(s3);
		System.out.println(np.apply(conf, Lists.newArrayList(s3)));

	}

	@Test
	public void testAcronym() {
		params.put("max", "4");
		params.put("minLen", "1");
		params.put("maxLen", "3");

		final ClusteringFunction acro = new Acronyms(params);

		final String s = "Search for the Standard Model Higgs Boson";
		System.out.println(s);
		System.out.println(acro.apply(conf, Lists.newArrayList(s)));
	}

	@Test
	public void testSuffixPrefix() {
		params.put("len", "3");
		params.put("max", "4");

		final ClusteringFunction sp = new SuffixPrefix(params);

		final String s = "Search for the Standard Model Higgs Boson";
		System.out.println(s);
		System.out.println(sp.apply(conf, Lists.newArrayList(s)));

		params.put("len", "3");
		params.put("max", "1");

		System.out.println(sp.apply(conf, Lists.newArrayList("Framework for general-purpose deduplication")));
	}

	@Test
	public void testWordsSuffixPrefix() {

		params.put("len", "3");
		params.put("max", "4");

		final ClusteringFunction sp = new WordsSuffixPrefix(params);

		final String s = "Search for the Standard Model Higgs Boson";
		System.out.println(s);
		System.out.println(sp.apply(conf, Lists.newArrayList(s)));
	}

	@Test
	public void testWordsStatsSuffixPrefix() {
		params.put("mod", "10");

		final ClusteringFunction sp = new WordsStatsSuffixPrefixChain(params);

		String s = "Search for the Standard Model Higgs Boson";
		System.out.println(s);
		System.out.println(sp.apply(conf, Lists.newArrayList(s)));

		s = "A Physical Education Teacher Is Like...: Examining Turkish Students  Perceptions of Physical Education Teachers Through Metaphor Analysis";
		System.out.println(s);
		System.out.println(sp.apply(conf, Lists.newArrayList(s)));

		s = "Structure of a Eukaryotic Nonribosomal Peptide Synthetase Adenylation Domain That Activates a Large Hydroxamate Amino Acid in Siderophore Biosynthesis";
		System.out.println(s);
		System.out.println(sp.apply(conf, Lists.newArrayList(s)));

		s = "Performance Evaluation";
		System.out.println(s);
		System.out.println(sp.apply(conf, Lists.newArrayList(s)));

		s = "JRC Open Power Plants Database (JRC-PPDB-OPEN)";
		System.out.println(s);
		System.out.println(sp.apply(conf, Lists.newArrayList(s)));

		s = "JRC Open Power Plants Database";
		System.out.println(s);
		System.out.println(sp.apply(conf, Lists.newArrayList(s)));

		s = "niivue/niivue: 0.21.1";
		System.out.println(s);
		System.out.println(sp.apply(conf, Lists.newArrayList(s)));

	}

	@Test
	public void testFieldValue() {

		params.put("randomLength", "5");

		final ClusteringFunction sp = new SpaceTrimmingFieldValue(params);

		final String s = "Search for the Standard Model Higgs Boson";
		System.out.println(s);
		System.out.println(sp.apply(conf, Lists.newArrayList(s)));
	}

	@Test
	public void legalnameClustering() {

		final ClusteringFunction cf = new LegalnameClustering(params);
		String s = "key::1 key::2 city::1";
		System.out.println(s);
		System.out.println(cf.apply(conf, Lists.newArrayList(s)));

		s = "key::1 key::2 city::1 city::2";
		System.out.println(s);
		System.out.println(cf.apply(conf, Lists.newArrayList(s)));
	}

	@Test
	public void testPersonClustering() {

		final ClusteringFunction cf = new PersonClustering(params);
		final String s = "Abd-Alla, Abo-el-nour N.";
		System.out.println("s = " + s);
		System.out.println(cf.apply(conf, Lists.newArrayList(s)));

		final String s1 = "Manghi, Paolo";
		System.out.println("s1 = " + s1);
		System.out.println(cf.apply(conf, Lists.newArrayList(s1)));

	}

	@Test
	public void testPersonHash() {

		final ClusteringFunction cf = new PersonHash(params);
		final String s = "Manghi, Paolo";
		System.out.println("s = " + s);
		System.out.println(cf.apply(conf, Lists.newArrayList(s)));

		final String s1 = "Manghi, P.";
		System.out.println("s = " + s1);
		System.out.println(cf.apply(conf, Lists.newArrayList(s1)));

	}

	@Test
	public void testLastNameFirstInitial() {

		final ClusteringFunction cf = new LastNameFirstInitial(params);
		final String s = "LI Yonghong";
		System.out.println("s = " + s);
		System.out.println(cf.apply(conf, Lists.newArrayList(s)));
	}

}
