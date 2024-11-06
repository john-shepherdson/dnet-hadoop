
package eu.dnetlib.dhp.resulttocommunityfromproject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.schema.oaf.Context;
import eu.dnetlib.dhp.schema.oaf.Dataset;

public class ResultToCommunityJobTest {

	private static final Logger log = LoggerFactory.getLogger(ResultToCommunityJobTest.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SparkSession spark;

	private static Path workingDir;

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files.createTempDirectory(ResultToCommunityJobTest.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(ResultToCommunityJobTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(ResultToCommunityJobTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	@Test
	void testSparkResultToCommunityFromProjectJob() throws Exception {
		final String preparedInfoPath = getClass()
			.getResource("/eu/dnetlib/dhp/resulttocommunityfromproject/preparedInfo")
			.getPath();
		SparkResultToCommunityFromProject
			.main(
				new String[] {

					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath", getClass()
						.getResource("/eu/dnetlib/dhp/resulttocommunityfromproject/sample/")
						.getPath(),

					"-outputPath", workingDir.toString() + "/",
					"-preparedInfoPath", preparedInfoPath
				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Dataset> tmp = sc
			.textFile(workingDir.toString() + "/dataset")
			.map(item -> OBJECT_MAPPER.readValue(item, Dataset.class));

		Assertions.assertEquals(10, tmp.count());
		/**
		 * {"resultId":"50|57a035e5b1ae::d5be548ca7ae489d762f893be67af52f","communityList":["aurora"]}
		 * {"resultId":"50|57a035e5b1ae::a77232ffca9115fcad51c3503dbc7e3e","communityList":["aurora"]}
		 * {"resultId":"50|57a035e5b1ae::803aaad4decab7e27cd4b52a1931b3a1","communityList":["sdsn-gr"]}
		 * {"resultId":"50|57a035e5b1ae::a02e9e4087bca50687731ae5c765b5e1","communityList":["netherlands"]}
		 */
		List<Context> context = tmp
			.filter(r -> r.getId().equals("50|57a035e5b1ae::d5be548ca7ae489d762f893be67af52f"))
			.first()
			.getContext();
		Assertions.assertTrue(context.stream().anyMatch(c -> containsResultCommunityProject(c)));

		context = tmp
			.filter(r -> r.getId().equals("50|57a035e5b1ae::a77232ffca9115fcad51c3503dbc7e3e"))
			.first()
			.getContext();
		Assertions.assertTrue(context.stream().anyMatch(c -> containsResultCommunityProject(c)));

		Assertions
			.assertEquals(
				0, tmp.filter(r -> r.getId().equals("50|57a035e5b1ae::803aaad4decab7e27cd4b52a1931b3a1")).count());

		Assertions
			.assertEquals(
				0, tmp.filter(r -> r.getId().equals("50|57a035e5b1ae::a02e9e4087bca50687731ae5c765b5e1")).count());

		Assertions
			.assertEquals(
				2, tmp.filter(r -> r.getContext().stream().anyMatch(c -> c.getId().equals("aurora"))).count());

	}

	private static boolean containsResultCommunityProject(Context c) {
		return c
			.getDataInfo()
			.stream()
			.anyMatch(di -> di.getProvenanceaction().getClassid().equals("result:community:project"));
	}
}
