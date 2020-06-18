
package eu.dnetlib.dhp.oa.graph.dump;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;

import eu.dnetlib.dhp.schema.dump.oaf.Result;

public class SplitForCommunityTest {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SparkSession spark;

	private static Path workingDir;

	private static String MOCK_IS_LOOK_UP_URL = "BASEURL:8280/is/services/isLookUp";

	private static final Logger log = LoggerFactory.getLogger(DumpJobTest.class);

	private static HashMap<String, String> map = new HashMap<>();

	static {
		map.put("egi", "EGI Federation");
		map.put("fet-fp7", "FET FP7");
		map.put("fet-h2020", "FET H2020");
		map.put("clarin", "CLARIN");
		map.put("fam", "Fisheries and Aquaculture Management");
		map.put("ni", "Neuroinformatics");
		map.put("mes", "European Marine Scinece");
		map.put("instruct", "Instruct-Eric");
		map.put("rda", "Research Data Alliance");
		map.put("elixir-gr", "ELIXIR GR");
		map.put("aginfra", "Agricultural and Food Sciences");
		map.put("dariah", "DARIAH EU");
		map.put("risis", "RISI");
		map.put("ee", "SDSN - Greece");
		map.put("oa-pg", "EC Post-Grant Open Access Pilot");
		map.put("beopen", "Transport Research");
		map.put("euromarine", "Euromarine");
		map.put("ifremer", "Ifremer");
		map.put("dh-ch", "Digital Humanities and Cultural Heritage");
		map.put("science-innovation-policy", "Science and Innovation Policy Studies");
		map.put("covid-19", "COVID-19");
		map.put("enermaps", "Energy Research");
		map.put("epos", "EPOS");

	}

//	@Mock
//	private SparkDumpCommunityProducts dumpCommunityProducts;

	// private QueryInformationSystem queryInformationSystem;

//	@Mock
//	private ISLookUpService isLookUpService;

	List<String> communityMap = Arrays
		.asList(
			"<community id=\"egi\" label=\"EGI Federation\"/>",
			"<community id=\"fet-fp7\" label=\"FET FP7\"/>",
			"<community id=\"fet-h2020\" label=\"FET H2020\"/>",
			"<community id=\"clarin\" label=\"CLARIN\"/>",
			"<community id=\"rda\" label=\"Research Data Alliance\"/>",
			"<community id=\"ee\" label=\"SDSN - Greece\"/>",
			"<community id=\"dh-ch\" label=\"Digital Humanities and Cultural Heritage\"/>",
			"<community id=\"fam\" label=\"Fisheries and Aquaculture Management\"/>",
			"<community id=\"ni\" label=\"Neuroinformatics\"/>",
			"<community id=\"mes\" label=\"European Marine Science\"/>",
			"<community id=\"instruct\" label=\"Instruct-ERIC\"/>",
			"<community id=\"elixir-gr\" label=\"ELIXIR GR\"/>",
			"<community id=\"aginfra\" label=\"Agricultural and Food Sciences\"/>",
			"<community id=\"dariah\" label=\"DARIAH EU\"/>",
			"<community id=\"risis\" label=\"RISIS\"/>",
			"<community id=\"epos\" label=\"EPOS\"/>",
			"<community id=\"beopen\" label=\"Transport Research\"/>",
			"<community id=\"euromarine\" label=\"EuroMarine\"/>",
			"<community id=\"ifremer\" label=\"Ifremer\"/>",
			"<community id=\"oa-pg\" label=\"EC Post-Grant Open Access Pilot\"/>",
			"<community id=\"science-innovation-policy\" label=\"Science and Innovation Policy Studies\"/>",
			"<community id=\"covid-19\" label=\"COVID-19\"/>",
			"<community id=\"enermaps\" label=\"Energy Research\"/>");

	private static final String XQUERY = "for $x in collection('/db/DRIVER/ContextDSResources/ContextDSResourceType') "
		+
		"  where $x//CONFIGURATION/context[./@type='community' or ./@type='ri'] " +
		"  return " +
		"<community> " +
		"{$x//CONFIGURATION/context/@id}" +
		"{$x//CONFIGURATION/context/@label}" +
		"</community>";

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files.createTempDirectory(SplitForCommunityTest.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(SplitForCommunityTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(SplitForCommunityTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

//	@BeforeEach
//	public void setUp() throws ISLookUpException {
//		lenient().when(isLookUpService.quickSearchProfile(XQUERY)).thenReturn(communityMap);
//		lenient().when(dumpCommunityProducts.getIsLookUpService(MOCK_IS_LOOK_UP_URL)).thenReturn(isLookUpService);
//
//	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	@Test
	public void test1() throws Exception {

		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/splitForCommunity")
			.getPath();

		SparkSplitForCommunity.main(new String[] {
			"-isLookUpUrl", MOCK_IS_LOOK_UP_URL,
			"-isSparkSessionManaged", Boolean.FALSE.toString(),
			"-outputPath", workingDir.toString() + "/split",
			"-sourcePath", sourcePath,
			"-communityMap", new Gson().toJson(map)
		});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Result> tmp = sc
			.textFile(workingDir.toString() + "/split/dh-ch")
			.map(item -> OBJECT_MAPPER.readValue(item, Result.class));

		org.apache.spark.sql.Dataset<Result> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(Result.class));

		Assertions.assertEquals(19, verificationDataset.count());

		Assertions
			.assertEquals(
				1, verificationDataset.filter("id = '50|dedup_wf_001::51b88f272ba9c3bb181af64e70255a80'").count());

		tmp = sc
			.textFile(workingDir.toString() + "/split/egi")
			.map(item -> OBJECT_MAPPER.readValue(item, Result.class));

		verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(Result.class));

		Assertions.assertEquals(1, verificationDataset.count());

		Assertions
			.assertEquals(
				1, verificationDataset.filter("id = '50|dedup_wf_001::e4805d005bfab0cd39a1642cbf477fdb'").count());

		tmp = sc
			.textFile(workingDir.toString() + "/split/ni")
			.map(item -> OBJECT_MAPPER.readValue(item, Result.class));

		verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(Result.class));

		Assertions.assertEquals(5, verificationDataset.count());

		Assertions
			.assertEquals(
				1, verificationDataset.filter("id = '50|datacite____::6b1e3a2fa60ed8c27317a66d6357f795'").count());

		tmp = sc
			.textFile(workingDir.toString() + "/split/science-innovation-policy")
			.map(item -> OBJECT_MAPPER.readValue(item, Result.class));

		verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(Result.class));

		Assertions.assertEquals(5, verificationDataset.count());

		Assertions
			.assertEquals(
				1, verificationDataset.filter("id = '50|dedup_wf_001::0347b1cd516fc59e41ba92e0d74e4e9f'").count());
		Assertions
			.assertEquals(
				1, verificationDataset.filter("id = '50|dedup_wf_001::1432beb6171baa5da8a85a7f99545d69'").count());
		Assertions
			.assertEquals(
				1, verificationDataset.filter("id = '50|dedup_wf_001::1c8bd19e633976e314b88ce5c3f92d69'").count());
		Assertions
			.assertEquals(
				1, verificationDataset.filter("id = '50|dedup_wf_001::51b88f272ba9c3bb181af64e70255a80'").count());

		tmp = sc
			.textFile(workingDir.toString() + "/split/fet-fp7")
			.map(item -> OBJECT_MAPPER.readValue(item, Result.class));

		Assertions.assertEquals(0, tmp.count());

		tmp = sc
			.textFile(workingDir.toString() + "/split/fet-h2020")
			.map(item -> OBJECT_MAPPER.readValue(item, Result.class));

		Assertions.assertEquals(0, tmp.count());

		tmp = sc
			.textFile(workingDir.toString() + "/split/clarin")
			.map(item -> OBJECT_MAPPER.readValue(item, Result.class));

		Assertions.assertEquals(0, tmp.count());

		tmp = sc
			.textFile(workingDir.toString() + "/split/rda")
			.map(item -> OBJECT_MAPPER.readValue(item, Result.class));

		Assertions.assertEquals(0, tmp.count());

		tmp = sc
			.textFile(workingDir.toString() + "/split/ee")
			.map(item -> OBJECT_MAPPER.readValue(item, Result.class));

		Assertions.assertEquals(0, tmp.count());

		tmp = sc
			.textFile(workingDir.toString() + "/split/fam")
			.map(item -> OBJECT_MAPPER.readValue(item, Result.class));

		Assertions.assertEquals(0, tmp.count());

		tmp = sc
			.textFile(workingDir.toString() + "/split/mes")
			.map(item -> OBJECT_MAPPER.readValue(item, Result.class));

		Assertions.assertEquals(0, tmp.count());

		tmp = sc
			.textFile(workingDir.toString() + "/split/instruct")
			.map(item -> OBJECT_MAPPER.readValue(item, Result.class));

		Assertions.assertEquals(0, tmp.count());

		tmp = sc
			.textFile(workingDir.toString() + "/split/elixir-gr")
			.map(item -> OBJECT_MAPPER.readValue(item, Result.class));

		Assertions.assertEquals(0, tmp.count());

		tmp = sc
			.textFile(workingDir.toString() + "/split/aginfra")
			.map(item -> OBJECT_MAPPER.readValue(item, Result.class));

		Assertions.assertEquals(0, tmp.count());

		tmp = sc
			.textFile(workingDir.toString() + "/split/dariah")
			.map(item -> OBJECT_MAPPER.readValue(item, Result.class));

		Assertions.assertEquals(0, tmp.count());

		tmp = sc
			.textFile(workingDir.toString() + "/split/risis")
			.map(item -> OBJECT_MAPPER.readValue(item, Result.class));

		Assertions.assertEquals(0, tmp.count());

		tmp = sc
			.textFile(workingDir.toString() + "/split/epos")
			.map(item -> OBJECT_MAPPER.readValue(item, Result.class));

		Assertions.assertEquals(0, tmp.count());

		tmp = sc
			.textFile(workingDir.toString() + "/split/beopen")
			.map(item -> OBJECT_MAPPER.readValue(item, Result.class));

		Assertions.assertEquals(0, tmp.count());

		tmp = sc
			.textFile(workingDir.toString() + "/split/euromarine")
			.map(item -> OBJECT_MAPPER.readValue(item, Result.class));

		Assertions.assertEquals(0, tmp.count());

		tmp = sc
			.textFile(workingDir.toString() + "/split/ifremer")
			.map(item -> OBJECT_MAPPER.readValue(item, Result.class));

		Assertions.assertEquals(0, tmp.count());

		tmp = sc
			.textFile(workingDir.toString() + "/split/oa-pg")
			.map(item -> OBJECT_MAPPER.readValue(item, Result.class));

		Assertions.assertEquals(0, tmp.count());

		tmp = sc
			.textFile(workingDir.toString() + "/split/covid-19")
			.map(item -> OBJECT_MAPPER.readValue(item, Result.class));

		Assertions.assertEquals(0, tmp.count());

		tmp = sc
			.textFile(workingDir.toString() + "/split/enermaps")
			.map(item -> OBJECT_MAPPER.readValue(item, Result.class));

		Assertions.assertEquals(0, tmp.count());

	}
}
