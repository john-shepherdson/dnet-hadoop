
package eu.dnetlib.dhp.resulttocommunityfromsemrel;

import static org.apache.spark.sql.functions.desc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import eu.dnetlib.dhp.resulttocommunityfromorganization.ResultCommunityList;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.schema.oaf.Dataset;
import scala.collection.Seq;

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
	void testSparkResultToCommunityThroughSemRelJob() throws Exception {
		SparkResultToCommunityThroughSemRelJob
			.main(
				new String[] {
					"-isTest", Boolean.TRUE.toString(),
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath", getClass()
						.getResource("/eu/dnetlib/dhp/resulttocommunityfromsemrel/sample")
						.getPath(),
					"-hive_metastore_uris", "",
					"-resultTableName", "eu.dnetlib.dhp.schema.oaf.Dataset",
					"-outputPath", workingDir.toString() + "/dataset",
					"-preparedInfoPath", getClass()
						.getResource("/eu/dnetlib/dhp/resulttocommunityfromsemrel/preparedInfo")
						.getPath()
				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Dataset> tmp = sc
			.textFile(workingDir.toString() + "/dataset")
			.map(item -> OBJECT_MAPPER.readValue(item, Dataset.class));

		Assertions.assertEquals(10, tmp.count());
		org.apache.spark.sql.Dataset<Dataset> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(Dataset.class));

		verificationDataset.createOrReplaceTempView("dataset");

		String query = "select id, MyT.id community "
			+ "from dataset "
			+ "lateral view explode(context) c as MyT "
			+ "lateral view explode(MyT.datainfo) d as MyD "
			+ "where MyD.inferenceprovenance = 'propagation'";

		org.apache.spark.sql.Dataset<Row> resultExplodedProvenance = spark.sql(query);
		Assertions.assertEquals(5, resultExplodedProvenance.count());

		Assertions
			.assertEquals(
				0,
				resultExplodedProvenance
					.filter("id = '50|dedup_wf_001::2305908abeca9da37eaf3bddcaf81b7b'")
					.count());

		Assertions
			.assertEquals(
				1,
				resultExplodedProvenance
					.filter("id = '50|dedup_wf_001::0489ae524201eedaa775da282dce35e7'")
					.count());
		Assertions
			.assertEquals(
				"dh-ch",
				resultExplodedProvenance
					.select("community")
					.where(
						resultExplodedProvenance
							.col("id")
							.equalTo(
								"50|dedup_wf_001::0489ae524201eedaa775da282dce35e7"))
					.collectAsList()
					.get(0)
					.getString(0));

		Assertions
			.assertEquals(
				3,
				resultExplodedProvenance
					.filter("id = '50|dedup_wf_001::0a60e33b4f0986ebd9819451f2d87a28'")
					.count());
		List<Row> rowList = resultExplodedProvenance
			.select("community")
			.where(
				resultExplodedProvenance
					.col("id")
					.equalTo(
						"50|dedup_wf_001::0a60e33b4f0986ebd9819451f2d87a28"))
			.sort(desc("community"))
			.collectAsList();
		Assertions.assertEquals("mes", rowList.get(0).getString(0));
		Assertions.assertEquals("fam", rowList.get(1).getString(0));
		Assertions.assertEquals("ee", rowList.get(2).getString(0));

		Assertions
			.assertEquals(
				1,
				resultExplodedProvenance
					.filter("id = '50|dedup_wf_001::0ae02edb5598a5545d10b107fcf48dcc'")
					.count());
		Assertions
			.assertEquals(
				"aginfra",
				resultExplodedProvenance
					.select("community")
					.where(
						resultExplodedProvenance
							.col("id")
							.equalTo(
								"50|dedup_wf_001::0ae02edb5598a5545d10b107fcf48dcc"))
					.collectAsList()
					.get(0)
					.getString(0));

		query = "select id, MyT.id community "
			+ "from dataset "
			+ "lateral view explode(context) c as MyT "
			+ "lateral view explode(MyT.datainfo) d as MyD ";

		org.apache.spark.sql.Dataset<Row> resultCommunityId = spark.sql(query);

		Assertions.assertEquals(10, resultCommunityId.count());

		Assertions
			.assertEquals(
				2,
				resultCommunityId
					.filter("id = '50|dedup_wf_001::0489ae524201eedaa775da282dce35e7'")
					.count());
		rowList = resultCommunityId
			.select("community")
			.where(
				resultCommunityId
					.col("id")
					.equalTo(
						"50|dedup_wf_001::0489ae524201eedaa775da282dce35e7"))
			.sort(desc("community"))
			.collectAsList();
		Assertions.assertEquals("dh-ch", rowList.get(0).getString(0));
		Assertions.assertEquals("beopen", rowList.get(1).getString(0));

		Assertions
			.assertEquals(
				3,
				resultCommunityId
					.filter("id = '50|dedup_wf_001::0a60e33b4f0986ebd9819451f2d87a28'")
					.count());
		rowList = resultCommunityId
			.select("community")
			.where(
				resultCommunityId
					.col("id")
					.equalTo(
						"50|dedup_wf_001::0a60e33b4f0986ebd9819451f2d87a28"))
			.sort(desc("community"))
			.collectAsList();
		Assertions.assertEquals("mes", rowList.get(0).getString(0));
		Assertions.assertEquals("fam", rowList.get(1).getString(0));
		Assertions.assertEquals("ee", rowList.get(2).getString(0));

		Assertions
			.assertEquals(
				2,
				resultCommunityId
					.filter("id = '50|dedup_wf_001::0ae02edb5598a5545d10b107fcf48dcc'")
					.count());
		rowList = resultCommunityId
			.select("community")
			.where(
				resultCommunityId
					.col("id")
					.equalTo(
						"50|dedup_wf_001::0ae02edb5598a5545d10b107fcf48dcc"))
			.sort(desc("community"))
			.collectAsList();
		Assertions.assertEquals("beopen", rowList.get(0).getString(0));
		Assertions.assertEquals("aginfra", rowList.get(1).getString(0));

		Assertions
			.assertEquals(
				2,
				resultCommunityId
					.filter("id = '50|dedup_wf_001::2305908abeca9da37eaf3bddcaf81b7b'")
					.count());
		rowList = resultCommunityId
			.select("community")
			.where(
				resultCommunityId
					.col("id")
					.equalTo(
						"50|dedup_wf_001::2305908abeca9da37eaf3bddcaf81b7b"))
			.sort(desc("community"))
			.collectAsList();
		Assertions.assertEquals("euromarine", rowList.get(1).getString(0));
		Assertions.assertEquals("ni", rowList.get(0).getString(0));

		Assertions
			.assertEquals(
				1,
				resultCommunityId
					.filter("id = '50|doajarticles::8d817039a63710fcf97e30f14662c6c8'")
					.count());
		Assertions
			.assertEquals(
				"euromarine",
				resultCommunityId
					.select("community")
					.where(
						resultCommunityId
							.col("id")
							.equalTo(
								"50|doajarticles::8d817039a63710fcf97e30f14662c6c8"))
					.collectAsList()
					.get(0)
					.getString(0));
	}

	@Test
	public void prepareStep1Test() throws Exception {
		/*


		final String allowedsemrel = join(",", Arrays.stream(parser.get("allowedsemrels").split(";"))
				.map(value -> "'" + value.toLowerCase() + "'")
				.toArray(String[]::new));

		log.info("allowedSemRel: {}", new Gson().toJson(allowedsemrel));

		final String baseURL = parser.get("baseURL");
		log.info("baseURL: {}", baseURL);
		 */
		PrepareResultCommunitySetStep1
				.main(
						new String[] {
								"-isSparkSessionManaged", Boolean.FALSE.toString(),
								"-sourcePath", getClass()
								.getResource("/eu/dnetlib/dhp/resulttocommunityfromsemrel/graph")
								.getPath(),
								"-hive_metastore_uris", "",
								"-resultTableName", "eu.dnetlib.dhp.schema.oaf.Publication",
								"-outputPath", workingDir.toString() + "/preparedInfo",
								"-allowedsemrels","issupplementto;issupplementedby",
								"-baseURL","https://dev-openaire.d4science.org/openaire/community/"
						});


		org.apache.spark.sql.Dataset<ResultCommunityList> resultCommunityList = spark.read().schema(Encoders.bean(ResultCommunityList.class).schema())
				.json(workingDir.toString() + "/preparedInfo/publication")
				.as(Encoders.bean(ResultCommunityList.class));

		Assertions.assertEquals(2, resultCommunityList.count());
		Assertions.assertEquals(1,resultCommunityList.filter("resultId = '50|dedup_wf_001::06e51d2bf295531b2d2e7a1b55500783'").count());
		Assertions.assertEquals(1,resultCommunityList.filter("resultId = '50|pending_org_::82f63b2d21ae88596b9d8991780e9888'").count());

		ArrayList<String> communities = resultCommunityList
				.filter("resultId = '50|dedup_wf_001::06e51d2bf295531b2d2e7a1b55500783'")
				.first().getCommunityList();
		Assertions.assertEquals(2, communities.size());
		Assertions.assertTrue(communities.stream().anyMatch(cid -> "beopen".equals(cid)));
		Assertions.assertTrue(communities.stream().anyMatch(cid -> "dh-ch".equals(cid)));

		communities = resultCommunityList
				.filter("resultId = '50|pending_org_::82f63b2d21ae88596b9d8991780e9888'")
				.first().getCommunityList();
		Assertions.assertEquals(1, communities.size());
		Assertions.assertEquals("dh-ch", communities.get(0));
	}
}
