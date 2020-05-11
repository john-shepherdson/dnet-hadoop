
package eu.dnetlib.dhp.blacklist;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

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
import eu.dnetlib.dhp.schema.oaf.Relation;

public class BlackListTest {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static final ClassLoader cl = eu.dnetlib.dhp.blacklist.BlackListTest.class.getClassLoader();

	private static SparkSession spark;

	private static Path workingDir;
	private static final Logger log = LoggerFactory.getLogger(eu.dnetlib.dhp.blacklist.BlackListTest.class);

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files.createTempDirectory(eu.dnetlib.dhp.blacklist.BlackListTest.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(eu.dnetlib.dhp.blacklist.BlackListTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(BlackListTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	/*
	 * String inputPath = parser.get("sourcePath"); log.info("inputPath: {}", inputPath); final String outputPath =
	 * parser.get("outputPath"); log.info("outputPath {}: ", outputPath); final String blacklistPath =
	 * parser.get("hdfsPath"); log.info("blacklistPath {}: ", blacklistPath); final String mergesPath =
	 * parser.get("mergesPath"); log.info("mergesPath {}: ", mergesPath);
	 */
	@Test
	public void noRemoveTest() throws Exception {
		SparkRemoveBlacklistedRelationJob
			.main(
				new String[] {
					"-isSparkSessionManaged",
					Boolean.FALSE.toString(),
					"-sourcePath",
					getClass().getResource("/eu/dnetlib/dhp/blacklist/relationsNoRemoval").getPath(),
					"-outputPath",
					workingDir.toString() + "/relation",
					"-hdfsPath",
					getClass().getResource("/eu/dnetlib/dhp/blacklist/blacklist").getPath(),
					"-mergesPath",
					getClass().getResource("/eu/dnetlib/dhp/blacklist/mergesRel").getPath(),
				});

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaRDD<Relation> tmp = sc
			.textFile(workingDir.toString() + "/relation")
			.map(item -> OBJECT_MAPPER.readValue(item, Relation.class));

		Assertions.assertEquals(13, tmp.count());

	}

	@Test
	public void removeNoMergeMatchTest() throws Exception {
		SparkRemoveBlacklistedRelationJob
			.main(
				new String[] {
					"-isSparkSessionManaged",
					Boolean.FALSE.toString(),
					"-sourcePath",
					getClass().getResource("/eu/dnetlib/dhp/blacklist/relationsOneRemoval").getPath(),
					"-outputPath",
					workingDir.toString() + "/relation",
					"-hdfsPath",
					getClass().getResource("/eu/dnetlib/dhp/blacklist/blacklist").getPath(),
					"-mergesPath",
					getClass().getResource("/eu/dnetlib/dhp/blacklist/mergesRel").getPath(),
				});

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaRDD<Relation> tmp = sc
			.textFile(workingDir.toString() + "/relation")
			.map(item -> OBJECT_MAPPER.readValue(item, Relation.class));

		Assertions.assertEquals(12, tmp.count());

		org.apache.spark.sql.Dataset<eu.dnetlib.dhp.schema.oaf.Relation> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(eu.dnetlib.dhp.schema.oaf.Relation.class));

		Assertions
			.assertEquals(
				0, verificationDataset
					.filter(
						"source = '40|corda__h2020::5161f53ab205d803c36b4c888fe7deef' and " +
							"target = '20|dedup_wf_001::157af406bc653aa4d9749318b644de43'")
					.count());

		Assertions.assertEquals(0, verificationDataset.filter("relClass = 'hasParticipant'").count());
	}

	@Test
	public void removeMergeMatchTest() throws Exception {
		SparkRemoveBlacklistedRelationJob
			.main(
				new String[] {
					"-isSparkSessionManaged",
					Boolean.FALSE.toString(),
					"-sourcePath",
					getClass().getResource("/eu/dnetlib/dhp/blacklist/relationOneRemovalWithMatch").getPath(),
					"-outputPath",
					workingDir.toString() + "/relation",
					"-hdfsPath",
					getClass().getResource("/eu/dnetlib/dhp/blacklist/blacklist").getPath(),
					"-mergesPath",
					getClass().getResource("/eu/dnetlib/dhp/blacklist/mergesRelOneMerge").getPath(),
				});

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaRDD<Relation> tmp = sc
			.textFile(workingDir.toString() + "/relation")
			.map(item -> OBJECT_MAPPER.readValue(item, Relation.class));

		Assertions.assertEquals(12, tmp.count());

		org.apache.spark.sql.Dataset<eu.dnetlib.dhp.schema.oaf.Relation> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(eu.dnetlib.dhp.schema.oaf.Relation.class));

		Assertions.assertEquals(12, verificationDataset.filter("relClass = 'isProvidedBy'").count());

	}
}
