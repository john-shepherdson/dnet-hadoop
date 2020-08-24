
package eu.dnetlib.dhp.oa.graph.dump.pid;

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

import eu.dnetlib.dhp.schema.dump.oaf.graph.Relation;
import eu.dnetlib.dhp.schema.dump.pidgraph.Entity;

public class DumpResultRelationTest {
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SparkSession spark;

	private static Path workingDir;

	private static final Logger log = LoggerFactory.getLogger(DumpResultRelationTest.class);

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files.createTempDirectory(DumpResultRelationTest.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(DumpResultRelationTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(DumpAuthorTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	@Test
	public void testDumpResultRelationPids() throws Exception {

		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/pid/preparedInfoDR")
			.getPath();

		SparkDumpResultRelation.main(new String[] {
			"-isSparkSessionManaged", Boolean.FALSE.toString(),
			"-outputPath", workingDir.toString(),
			"-preparedInfoPath", sourcePath
		});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Relation> tmp = sc
			.textFile(workingDir.toString() + "/relation")
			.map(item -> OBJECT_MAPPER.readValue(item, Relation.class));

		org.apache.spark.sql.Dataset<Relation> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(Relation.class));

		verificationDataset.show(100, false);
		Assertions.assertEquals(32, verificationDataset.count());

		Assertions
			.assertEquals(
				1, verificationDataset
					.filter(
						"source.id = 'orcid:0000-0001-9317-9350' and " +
							"relType.name = 'sameAs' and target.id = 'mag:fakeMag'")
					.count());

		Assertions
			.assertEquals(
				4, verificationDataset
					.filter(
						"source.id = 'orcid:0000-0001-9317-9350' and " +
							"relType.name = 'isAuthorOf'")
					.count());

		Assertions
			.assertEquals(
				4, verificationDataset
					.filter(
						"target.id = 'orcid:0000-0001-9317-9350' and " +
							"relType.name = 'hasAuthor'")
					.count());

		Assertions
			.assertEquals(
				1, verificationDataset
					.filter(
						"source.id = 'orcid:0000-0001-9317-9350' and " +
							"relType.name = 'hasCoAuthor'")
					.count());

		Assertions
			.assertEquals(
				1, verificationDataset
					.filter(
						"source.id = 'orcid:0000-0001-9317-9350' and " +
							"relType.name = 'hasCoAuthor' and target.id = 'orcid:0000-0002-1114-4216'")
					.count());

		Assertions
			.assertEquals(
				2, verificationDataset
					.filter(
						"source.id = 'orcid:0000-0002-1114-4216' and " +
							"relType.name = 'hasCoAuthor'")
					.count());

		Assertions
			.assertEquals(
				1, verificationDataset
					.filter(
						"target.id = 'orcid:0000-0001-9317-9350' and " +
							"relType.name = 'hasCoAuthor' and source.id = 'orcid:0000-0002-1114-4216'")
					.count());

		Assertions
			.assertEquals(
				1, verificationDataset
					.filter(
						"target.id = 'mag:fakeMag' and " +
							"relType.name = 'hasCoAuthor' and source.id = 'orcid:0000-0002-1114-4216'")
					.count());

		Assertions.assertEquals(4, verificationDataset.filter("relType.name = 'hasOtherManifestation'").count());

//
//        Assertions.assertEquals(1, verificationDataset.filter("id = 'mag:fakemag'").count());
//
//        Assertions.assertEquals(2, verificationDataset.filter("substr(id,1,5) = 'orcid'").count());

	}

}
