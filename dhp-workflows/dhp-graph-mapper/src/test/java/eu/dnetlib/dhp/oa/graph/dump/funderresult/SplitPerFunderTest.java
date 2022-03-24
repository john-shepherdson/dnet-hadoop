
package eu.dnetlib.dhp.oa.graph.dump.funderresult;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

//import eu.dnetlib.dhp.oa.graph.dump.funderresults.SparkDumpFunderResults2;
//import eu.dnetlib.dhp.oa.graph.dump.funderresults.SparkGetFunderList;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.oa.graph.dump.DumpJobTest;
import eu.dnetlib.dhp.oa.graph.dump.SplitForCommunityTest;
import eu.dnetlib.dhp.oa.graph.dump.community.CommunitySplit;
import eu.dnetlib.dhp.oa.graph.dump.funderresults.SparkDumpFunderResults;
import eu.dnetlib.dhp.schema.dump.oaf.community.CommunityResult;

public class SplitPerFunderTest {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SparkSession spark;

	private static Path workingDir;

	private static final Logger log = LoggerFactory.getLogger(SplitPerFunderTest.class);

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files.createTempDirectory(SplitPerFunderTest.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(SplitPerFunderTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(SplitPerFunderTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	@Test
	void test1() throws Exception {

		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/funderresource/ext")
			.getPath();


		SparkDumpFunderResults.main(new String[] {
			"-isSparkSessionManaged", Boolean.FALSE.toString(),
			"-outputPath", workingDir.toString() + "/split",
			"-sourcePath", sourcePath

		});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		// FP7 3 and H2020 3
		JavaRDD<CommunityResult> tmp = sc
			.textFile(workingDir.toString() + "/split/EC_FP7")
			.map(item -> OBJECT_MAPPER.readValue(item, CommunityResult.class));

		org.apache.spark.sql.Dataset<CommunityResult> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(CommunityResult.class));

		Assertions.assertEquals(3, verificationDataset.count());

		Assertions
			.assertEquals(
				1, verificationDataset.filter("id = '50|dedup_wf_001::0d16b1714ab3077df73893a8ea57d776'").count());

		// CIHR 2
		tmp = sc
			.textFile(workingDir.toString() + "/split/CIHR")
			.map(item -> OBJECT_MAPPER.readValue(item, CommunityResult.class));
		Assertions.assertEquals(2, tmp.count());

		// NWO 1
		tmp = sc
			.textFile(workingDir.toString() + "/split/NWO")
			.map(item -> OBJECT_MAPPER.readValue(item, CommunityResult.class));
		Assertions.assertEquals(1, tmp.count());

		// NIH 3
		tmp = sc
			.textFile(workingDir.toString() + "/split/NIH")
			.map(item -> OBJECT_MAPPER.readValue(item, CommunityResult.class));
		Assertions.assertEquals(2, tmp.count());

		// NSF 1
		tmp = sc
			.textFile(workingDir.toString() + "/split/NSF")
			.map(item -> OBJECT_MAPPER.readValue(item, CommunityResult.class));
		Assertions.assertEquals(1, tmp.count());

		// SNSF 1
		tmp = sc
			.textFile(workingDir.toString() + "/split/SNSF")
			.map(item -> OBJECT_MAPPER.readValue(item, CommunityResult.class));
		Assertions.assertEquals(1, tmp.count());

		// NHMRC 1
		tmp = sc
			.textFile(workingDir.toString() + "/split/NHMRC")
			.map(item -> OBJECT_MAPPER.readValue(item, CommunityResult.class));
		Assertions.assertEquals(1, tmp.count());

		// H2020 3
		tmp = sc
			.textFile(workingDir.toString() + "/split/EC_H2020")
			.map(item -> OBJECT_MAPPER.readValue(item, CommunityResult.class));
		Assertions.assertEquals(3, tmp.count());

		// MZOS 1
		tmp = sc
			.textFile(workingDir.toString() + "/split/MZOS")
			.map(item -> OBJECT_MAPPER.readValue(item, CommunityResult.class));
		Assertions.assertEquals(1, tmp.count());



	}


}
