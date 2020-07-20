
package eu.dnetlib.dhp.oa.graph.dump;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import eu.dnetlib.dhp.oa.graph.dump.community.CommunityMap;
import eu.dnetlib.dhp.schema.oaf.Dataset;
import eu.dnetlib.dhp.schema.oaf.OtherResearchProduct;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Software;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

//@ExtendWith(MockitoExtension.class)
public class DumpJobTest {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SparkSession spark;

	private static Path workingDir;

	private static String MOCK_IS_LOOK_UP_URL = "BASEURL:8280/is/services/isLookUp";

	private static final Logger log = LoggerFactory.getLogger(DumpJobTest.class);

	private static CommunityMap map = new CommunityMap();

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
		map.put("enrmaps", "Energy Research");
		map.put("epos", "EPOS");

	}

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
		workingDir = Files.createTempDirectory(DumpJobTest.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(DumpJobTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(DumpJobTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	@Test
	public void testDataset() {

		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/resultDump/dataset.json")
			.getPath();
		DumpProducts dump = new DumpProducts();
		dump.run(false, sourcePath, workingDir.toString() + "/result", map, Dataset.class, false);


		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<eu.dnetlib.dhp.schema.dump.oaf.Result> tmp = sc
			.textFile(workingDir.toString() + "/result")
			.map(item -> OBJECT_MAPPER.readValue(item, eu.dnetlib.dhp.schema.dump.oaf.Result.class));

		org.apache.spark.sql.Dataset<eu.dnetlib.dhp.schema.dump.oaf.Result> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(eu.dnetlib.dhp.schema.dump.oaf.Result.class));

		Assertions.assertEquals(90, verificationDataset.count());

		Assertions
			.assertTrue(
				verificationDataset.filter("bestAccessright.code = 'c_abf2'").count() == verificationDataset
					.filter("bestAccessright.code = 'c_abf2' and bestAccessright.label = 'OPEN'")
					.count());

		Assertions
			.assertTrue(
				verificationDataset.filter("bestAccessright.code = 'c_16ec'").count() == verificationDataset
					.filter("bestAccessright.code = 'c_16ec' and bestAccessright.label = 'RESTRICTED'")
					.count());

		Assertions
			.assertTrue(
				verificationDataset.filter("bestAccessright.code = 'c_14cb'").count() == verificationDataset
					.filter("bestAccessright.code = 'c_14cb' and bestAccessright.label = 'CLOSED'")
					.count());

		Assertions
			.assertTrue(
				verificationDataset.filter("bestAccessright.code = 'c_f1cf'").count() == verificationDataset
					.filter("bestAccessright.code = 'c_f1cf' and bestAccessright.label = 'EMBARGO'")
					.count());

		Assertions.assertTrue(verificationDataset.filter("size(context) > 0").count() == 90);

		Assertions.assertTrue(verificationDataset.filter("type = 'dataset'").count() == 90);

//TODO verify value and name of the fields for vocab related value (i.e. accessright, bestaccessright)

	}

	@Test
	public void testPublication() {

		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/resultDump/publication.json")
			.getPath();
		DumpProducts dump = new DumpProducts();
		dump.run(false, sourcePath, workingDir.toString() + "/result", map, Publication.class, false);

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<eu.dnetlib.dhp.schema.dump.oaf.Result> tmp = sc
			.textFile(workingDir.toString() + "/result")
			.map(item -> OBJECT_MAPPER.readValue(item, eu.dnetlib.dhp.schema.dump.oaf.Result.class));

		org.apache.spark.sql.Dataset<eu.dnetlib.dhp.schema.dump.oaf.Result> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(eu.dnetlib.dhp.schema.dump.oaf.Result.class));

		Assertions.assertEquals(76, verificationDataset.count());
		verificationDataset.show(false);

		Assertions.assertEquals(76, verificationDataset.filter("type = 'publication'").count());

//TODO verify value and name of the fields for vocab related value (i.e. accessright, bestaccessright)

	}

	@Test
	public void testSoftware() {

		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/resultDump/software.json")
			.getPath();

		DumpProducts dump = new DumpProducts();
		dump.run(false, sourcePath, workingDir.toString() + "/result", map, Software.class, false);


		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<eu.dnetlib.dhp.schema.dump.oaf.Result> tmp = sc
			.textFile(workingDir.toString() + "/result")
			.map(item -> OBJECT_MAPPER.readValue(item, eu.dnetlib.dhp.schema.dump.oaf.Result.class));

		org.apache.spark.sql.Dataset<eu.dnetlib.dhp.schema.dump.oaf.Result> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(eu.dnetlib.dhp.schema.dump.oaf.Result.class));

		Assertions.assertEquals(6, verificationDataset.count());

		Assertions.assertEquals(6, verificationDataset.filter("type = 'software'").count());
		verificationDataset.show(false);

//TODO verify value and name of the fields for vocab related value (i.e. accessright, bestaccessright)

	}

	@Test
	public void testORP() {

		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/resultDump/orp.json")
			.getPath();

		DumpProducts dump = new DumpProducts();
		dump.run(false, sourcePath, workingDir.toString() + "/result", map, OtherResearchProduct.class, false);


		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<eu.dnetlib.dhp.schema.dump.oaf.Result> tmp = sc
			.textFile(workingDir.toString() + "/result")
			.map(item -> OBJECT_MAPPER.readValue(item, eu.dnetlib.dhp.schema.dump.oaf.Result.class));

		org.apache.spark.sql.Dataset<eu.dnetlib.dhp.schema.dump.oaf.Result> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(eu.dnetlib.dhp.schema.dump.oaf.Result.class));

		Assertions.assertEquals(3, verificationDataset.count());

		Assertions.assertEquals(3, verificationDataset.filter("type = 'other'").count());
		verificationDataset.show(false);

//TODO verify value and name of the fields for vocab related value (i.e. accessright, bestaccessright)

	}

	@Test
	public void testRecord()  {
		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/resultDump/singelRecord_pub.json")
			.getPath();

		DumpProducts dump = new DumpProducts();
		dump.run(false, sourcePath, workingDir.toString() + "/result", map, Publication.class, false);

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<eu.dnetlib.dhp.schema.dump.oaf.Result> tmp = sc
			.textFile(workingDir.toString() + "/result")
			.map(item -> OBJECT_MAPPER.readValue(item, eu.dnetlib.dhp.schema.dump.oaf.Result.class));

		org.apache.spark.sql.Dataset<eu.dnetlib.dhp.schema.dump.oaf.Result> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(eu.dnetlib.dhp.schema.dump.oaf.Result.class));

		Assertions.assertEquals(1, verificationDataset.count());
		verificationDataset.show(false);

		Assertions.assertEquals(1, verificationDataset.filter("type = 'publication'").count());

	}

}
