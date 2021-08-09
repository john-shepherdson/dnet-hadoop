
package eu.dnetlib.dhp.oa.graph.dump;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.logging.Filter;
import java.util.stream.Collectors;

import eu.dnetlib.dhp.schema.dump.oaf.community.Funder;
import eu.dnetlib.dhp.schema.dump.oaf.community.Project;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
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

import eu.dnetlib.dhp.oa.graph.dump.community.SparkUpdateProjectInfo;
import eu.dnetlib.dhp.schema.dump.oaf.Result;
import eu.dnetlib.dhp.schema.dump.oaf.community.CommunityResult;

public class UpdateProjectInfoTest {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SparkSession spark;

	private static Path workingDir;

	private static final Logger log = LoggerFactory.getLogger(eu.dnetlib.dhp.oa.graph.dump.UpdateProjectInfoTest.class);

	private static final HashMap<String, String> map = new HashMap<>();

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files
			.createTempDirectory(eu.dnetlib.dhp.oa.graph.dump.UpdateProjectInfoTest.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(eu.dnetlib.dhp.oa.graph.dump.UpdateProjectInfoTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(eu.dnetlib.dhp.oa.graph.dump.UpdateProjectInfoTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	@Test
	public void test1() throws Exception {

		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/addProjectInfo")
			.getPath();

		SparkUpdateProjectInfo.main(new String[] {
			"-isSparkSessionManaged", Boolean.FALSE.toString(),
			"-preparedInfoPath", sourcePath + "/preparedInfo",
			"-outputPath", workingDir.toString() + "/result",
			"-sourcePath", sourcePath + "/software.json"
		});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<CommunityResult> tmp = sc
			.textFile(workingDir.toString() + "/result")
			.map(item -> OBJECT_MAPPER.readValue(item, CommunityResult.class));

		org.apache.spark.sql.Dataset<CommunityResult> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(CommunityResult.class));

		verificationDataset.show(false);

		Assertions.assertEquals(6, verificationDataset.count());
		verificationDataset.createOrReplaceTempView("dataset");

		String query = "select id, MyT.code code, MyT.title title, MyT.funder.name funderName, MyT.funder.shortName funderShortName, "
			+
			"MyT.funder.jurisdiction funderJurisdiction, MyT.funder.fundingStream fundingStream "
			+ "from dataset " +
			"lateral view explode(projects) p as MyT ";

		org.apache.spark.sql.Dataset<Row> resultExplodedProvenance = spark.sql(query);

		Assertions.assertEquals(3, resultExplodedProvenance.count());
		resultExplodedProvenance.show(false);

		Assertions
			.assertEquals(
				2,
				resultExplodedProvenance.filter("id = '50|dedup_wf_001::e4805d005bfab0cd39a1642cbf477fdb'").count());

		Assertions
			.assertEquals(
				1,
				resultExplodedProvenance
					.filter("id = '50|dedup_wf_001::e4805d005bfab0cd39a1642cbf477fdb' and code = '123455'")
					.count());

		Assertions
			.assertEquals(
				1,
				resultExplodedProvenance
					.filter("id = '50|dedup_wf_001::e4805d005bfab0cd39a1642cbf477fdb' and code = '119027'")
					.count());

		Assertions
			.assertEquals(
				1,
				resultExplodedProvenance
					.filter("id = '50|dedup_wf_001::51b88f272ba9c3bb181af64e70255a80' and code = '123455'")
					.count());

		resultExplodedProvenance.show(false);
	}

	@Test
	public void testValidatedRelation() throws Exception{
		final String sourcePath = getClass()
				.getResource("/eu/dnetlib/dhp/oa/graph/dump/addProjectInfo")
				.getPath();

		SparkUpdateProjectInfo.main(new String[] {
				"-isSparkSessionManaged", Boolean.FALSE.toString(),
				"-preparedInfoPath", sourcePath + "/preparedInfoValidated",
				"-outputPath", workingDir.toString() + "/result",
				"-sourcePath", sourcePath + "/publication_extendedmodel"
		});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<CommunityResult> tmp = sc
				.textFile(workingDir.toString() + "/result")
				.map(item -> OBJECT_MAPPER.readValue(item, CommunityResult.class));

		org.apache.spark.sql.Dataset<CommunityResult> verificationDataset = spark
				.createDataset(tmp.rdd(), Encoders.bean(CommunityResult.class));

		verificationDataset.show(false);

		Assertions.assertEquals(2, verificationDataset.count());
		verificationDataset.createOrReplaceTempView("dataset");

		String query = "select id, MyT.code code, MyT.title title, MyT.funder.name funderName, MyT.funder.shortName funderShortName, "
				+
				"MyT.funder.jurisdiction funderJurisdiction, MyT.funder.fundingStream fundingStream, MyT.validated "
				+ "from dataset " +
				"lateral view explode(projects) p as MyT ";

		org.apache.spark.sql.Dataset<Row> resultExplodedProvenance = spark.sql(query);

		Assertions.assertEquals(2, resultExplodedProvenance.count());
		resultExplodedProvenance.show(false);

		Assertions
				.assertEquals(
						2,
						resultExplodedProvenance.filter("id = '50|pensoft_____::00ea4a1cd53806a97d62ea6bf268f2a2'").count());

		Assertions
				.assertEquals(
						1,
						resultExplodedProvenance
								.filter("id = '50|pensoft_____::00ea4a1cd53806a97d62ea6bf268f2a2' and code = '123455'")
								.count());

		Assertions
				.assertEquals(
						1,
						resultExplodedProvenance
								.filter("id = '50|pensoft_____::00ea4a1cd53806a97d62ea6bf268f2a2' and code = '119027'")
								.count());

		Project project = verificationDataset
		.map((MapFunction<CommunityResult, Project>) cr -> cr.getProjects().stream().filter(p -> p.getValidated() != null).collect(Collectors.toList()).get(0)
		, Encoders.bean(Project.class)).first();

		Assertions.assertTrue(project.getFunder().getName().equals("Academy of Finland"));
		Assertions.assertTrue(project.getFunder().getShortName().equals("AKA"));
		Assertions.assertTrue(project.getFunder().getJurisdiction().equals("FI"));
		Assertions.assertTrue(project.getFunder().getFundingStream() == null);
		Assertions.assertTrue(project.getValidated().getValidationDate().equals("2021-08-06"));


		project = verificationDataset
				.map((MapFunction<CommunityResult, Project>) cr -> cr.getProjects().stream().filter(p -> p.getValidated() == null).collect(Collectors.toList()).get(0)
						, Encoders.bean(Project.class)).first();


		Assertions.assertTrue(project.getFunder().getName().equals("European Commission"));
		Assertions.assertTrue(project.getFunder().getShortName().equals("EC"));
		Assertions.assertTrue(project.getFunder().getJurisdiction().equals("EU"));
		Assertions.assertTrue(project.getFunder().getFundingStream().equals("H2020"));


	}

}
