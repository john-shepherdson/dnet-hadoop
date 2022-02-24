
package eu.dnetlib.dhp.oa.graph.dump.complete;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.schema.dump.oaf.graph.Relation;

public class ExtractRelationFromEntityTest {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SparkSession spark;

	private static Path workingDir;

	private static final Logger log = LoggerFactory
		.getLogger(ExtractRelationFromEntityTest.class);

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

	@Test
	void test1() {

		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/resultDump/singelRecord_pub.json")
			.getPath();

		final String communityMapPath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/communityMapPath/communitymap.json")
			.getPath();

		Extractor ex = new Extractor();
		ex
			.run(
				false, sourcePath, workingDir.toString() + "/relation",
				// eu.dnetlib.dhp.schema.oaf.Publication.class, communityMapPath);
				eu.dnetlib.dhp.schema.oaf.Publication.class, communityMapPath);

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Relation> tmp = sc
			.textFile(workingDir.toString() + "/relation")
			.map(item -> OBJECT_MAPPER.readValue(item, Relation.class));

		org.apache.spark.sql.Dataset<Relation> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(Relation.class));

		Assertions
			.assertEquals(
				9,
				verificationDataset.filter("source.id = '50|dedup_wf_001::15270b996fa8fd2fb5723daeab3685c3'").count());

		Assertions
			.assertEquals(
				9,
				verificationDataset.filter("source.id = '50|dedup_wf_001::15270b996fa8fd2fb5723daxab3685c3'").count());

		Assertions
			.assertEquals(
				"IsRelatedTo", verificationDataset
					.filter((FilterFunction<Relation>) row -> row.getSource().getId().startsWith("00"))
					.collectAsList()
					.get(0)
					.getReltype()
					.getName());

		Assertions
			.assertEquals(
				"relationship", verificationDataset
					.filter((FilterFunction<Relation>) row -> row.getSource().getId().startsWith("00"))
					.collectAsList()
					.get(0)
					.getReltype()
					.getType());

		Assertions
			.assertEquals(
				"context", verificationDataset
					.filter((FilterFunction<Relation>) row -> row.getSource().getId().startsWith("00"))
					.collectAsList()
					.get(0)
					.getSource()
					.getType());

		Assertions
			.assertEquals(
				"result", verificationDataset
					.filter((FilterFunction<Relation>) row -> row.getSource().getId().startsWith("00"))
					.collectAsList()
					.get(0)
					.getTarget()
					.getType());
		Assertions
			.assertEquals(
				"IsRelatedTo", verificationDataset
					.filter((FilterFunction<Relation>) row -> row.getTarget().getId().startsWith("00"))
					.collectAsList()
					.get(0)
					.getReltype()
					.getName());

		Assertions
			.assertEquals(
				"relationship", verificationDataset
					.filter((FilterFunction<Relation>) row -> row.getTarget().getId().startsWith("00"))
					.collectAsList()
					.get(0)
					.getReltype()
					.getType());

		Assertions
			.assertEquals(
				"context", verificationDataset
					.filter((FilterFunction<Relation>) row -> row.getTarget().getId().startsWith("00"))
					.collectAsList()
					.get(0)
					.getTarget()
					.getType());

		Assertions
			.assertEquals(
				"result", verificationDataset
					.filter((FilterFunction<Relation>) row -> row.getTarget().getId().startsWith("00"))
					.collectAsList()
					.get(0)
					.getSource()
					.getType());
	}

}
