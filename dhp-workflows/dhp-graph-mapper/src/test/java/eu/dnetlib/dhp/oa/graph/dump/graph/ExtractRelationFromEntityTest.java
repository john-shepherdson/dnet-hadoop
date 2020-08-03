
package eu.dnetlib.dhp.oa.graph.dump.graph;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.oa.graph.dump.community.CommunityMap;
import eu.dnetlib.dhp.schema.dump.oaf.graph.Relation;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;

@Disabled
public class ExtractRelationFromEntityTest {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SparkSession spark;

	private static Path workingDir;

	private static final Logger log = LoggerFactory
		.getLogger(ExtractRelationFromEntityTest.class);

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

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files
			.createTempDirectory(ExtractRelationFromEntityTest.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(ExtractRelationFromEntityTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(ExtractRelationFromEntityTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

//	@Test
//	public void test1() {
//
//		final String sourcePath = getClass()
//			.getResource("/eu/dnetlib/dhp/oa/graph/dump/resultDump/singelRecord_pub.json")
//			.getPath();
//
//		final String communityMapPath = getClass()
//			.getResource("/eu/dnetlib/dhp/oa/graph/dump/communityMapPath/communitymap.json")
//			.getPath();
//
//		Extractor ex = new Extractor();
//		ex
//			.run(
//				false, sourcePath, workingDir.toString() + "/relation",
//				// eu.dnetlib.dhp.schema.oaf.Publication.class, communityMapPath);
//				eu.dnetlib.dhp.schema.oaf.Publication.class, map);
//
//		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
//
//		JavaRDD<Relation> tmp = sc
//			.textFile(workingDir.toString() + "/relation")
//			.map(item -> OBJECT_MAPPER.readValue(item, Relation.class));
//
//		org.apache.spark.sql.Dataset<Relation> verificationDataset = spark
//			.createDataset(tmp.rdd(), Encoders.bean(Relation.class));
//
//		Assertions
//			.assertEquals(
//				9,
//				verificationDataset.filter("source.id = '50|dedup_wf_001::15270b996fa8fd2fb5723daeab3685c3'").count());
//
//		Assertions
//			.assertEquals(
//				9,
//				verificationDataset.filter("source.id = '50|dedup_wf_001::15270b996fa8fd2fb5723daxab3685c3'").count());
//
//	}

}
