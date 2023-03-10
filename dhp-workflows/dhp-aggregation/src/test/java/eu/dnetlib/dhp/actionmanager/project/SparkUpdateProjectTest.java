
package eu.dnetlib.dhp.actionmanager.project;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
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

import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.Project;

public class SparkUpdateProjectTest {
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static final ClassLoader cl = eu.dnetlib.dhp.actionmanager.project.SparkUpdateProjectTest.class
		.getClassLoader();

	private static SparkSession spark;

	private static Path workingDir;
	private static final Logger log = LoggerFactory
		.getLogger(eu.dnetlib.dhp.actionmanager.project.SparkUpdateProjectTest.class);

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files
			.createTempDirectory(eu.dnetlib.dhp.actionmanager.project.SparkUpdateProjectTest.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(eu.dnetlib.dhp.actionmanager.project.SparkUpdateProjectTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(SparkUpdateProjectTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	@Test
	void numberDistinctProgrammeTest() throws Exception {
		SparkAtomicActionJob
			.main(
				new String[] {
					"-isSparkSessionManaged",
					Boolean.FALSE.toString(),
					"-programmePath",
					getClass()
						.getResource(
							"/eu/dnetlib/dhp/actionmanager/project/prepared_h2020_programme.json.gz")
						.getPath(),
					"-projectPath",
					getClass().getResource("/eu/dnetlib/dhp/actionmanager/project/prepared_projects.json.gz").getPath(),
					"-topicPath",
					getClass().getResource("/eu/dnetlib/dhp/actionmanager/project/topics_nld.json.gz").getPath(),
					"-outputPath",
					workingDir.toString() + "/actionSet"
				});

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaRDD<Project> tmp = sc
			.sequenceFile(workingDir.toString() + "/actionSet", Text.class, Text.class)
			.map(value -> OBJECT_MAPPER.readValue(value._2().toString(), AtomicAction.class))
			.map(aa -> ((Project) aa.getPayload()));

		Assertions.assertEquals(15, tmp.count());

		Dataset<Project> verificationDataset = spark.createDataset(tmp.rdd(), Encoders.bean(Project.class));
		verificationDataset.createOrReplaceTempView("project");

		Dataset<Row> execverification = spark
			.sql(
				"SELECT id, class classification, h2020topiccode, h2020topicdescription FROM project LATERAL VIEW EXPLODE(h2020classification) c as class ");

		Assertions
			.assertEquals(
				"H2020-EU.3.4.7.",
				execverification
					.filter("id = '40|corda__h2020::2c7298913008865ba784e5c1350a0aa5'")
					.select("classification.h2020Programme.code")
					.collectAsList()
					.get(0)
					.getString(0));
		Assertions
			.assertEquals(
				"SESAR JU",
				execverification
					.filter("id = '40|corda__h2020::2c7298913008865ba784e5c1350a0aa5'")
					.select("classification.h2020Programme.description")
					.collectAsList()
					.get(0)
					.getString(0));
		Assertions
			.assertEquals(
				"Societal Challenges",
				execverification
					.filter("id = '40|corda__h2020::2c7298913008865ba784e5c1350a0aa5'")
					.select("classification.level1")
					.collectAsList()
					.get(0)
					.getString(0));
		Assertions
			.assertEquals(
				"Transport",
				execverification
					.filter("id = '40|corda__h2020::2c7298913008865ba784e5c1350a0aa5'")
					.select("classification.level2")
					.collectAsList()
					.get(0)
					.getString(0));
		Assertions
			.assertEquals(
				"SESAR JU",
				execverification
					.filter("id = '40|corda__h2020::2c7298913008865ba784e5c1350a0aa5'")
					.select("classification.level3")
					.collectAsList()
					.get(0)
					.getString(0));
		Assertions
			.assertEquals(
				"Societal challenges | Smart, Green And Integrated Transport | SESAR JU",
				execverification
					.filter("id = '40|corda__h2020::2c7298913008865ba784e5c1350a0aa5'")
					.select("classification.classification")
					.collectAsList()
					.get(0)
					.getString(0));
		Assertions
			.assertEquals(
				"SESAR-ER4-31-2019",
				execverification
					.filter("id = '40|corda__h2020::2c7298913008865ba784e5c1350a0aa5'")
					.select("h2020topiccode")
					.collectAsList()
					.get(0)
					.getString(0));
		Assertions
			.assertEquals(
				"U-space",
				execverification
					.filter("id = '40|corda__h2020::2c7298913008865ba784e5c1350a0aa5'")
					.select("h2020topicdescription")
					.collectAsList()
					.get(0)
					.getString(0));

		Assertions
			.assertEquals(
				"H2020-EU.1.3.2.",
				execverification
					.filter("id = '40|corda__h2020::1a1f235fdd06ef14790baec159aa1202'")
					.select("classification.h2020Programme.code")
					.collectAsList()
					.get(0)
					.getString(0));
		Assertions
			.assertEquals(
				"MSCA Mobility",
				execverification
					.filter("id = '40|corda__h2020::1a1f235fdd06ef14790baec159aa1202'")
					.select("classification.h2020Programme.description")
					.collectAsList()
					.get(0)
					.getString(0));
		Assertions
			.assertEquals(
				"Excellent Science",
				execverification
					.filter("id = '40|corda__h2020::1a1f235fdd06ef14790baec159aa1202'")
					.select("classification.level1")
					.collectAsList()
					.get(0)
					.getString(0));
		Assertions
			.assertEquals(
				"Marie-Sklodowska-Curie Actions",
				execverification
					.filter("id = '40|corda__h2020::1a1f235fdd06ef14790baec159aa1202'")
					.select("classification.level2")
					.collectAsList()
					.get(0)
					.getString(0));
		Assertions
			.assertEquals(
				"MSCA Mobility",
				execverification
					.filter("id = '40|corda__h2020::1a1f235fdd06ef14790baec159aa1202'")
					.select("classification.level3")
					.collectAsList()
					.get(0)
					.getString(0));
		Assertions
			.assertEquals(
				"Excellent science | Marie Skłodowska-Curie Actions | Nurturing excellence by means of cross-border and cross-sector mobility",
				execverification
					.filter("id = '40|corda__h2020::1a1f235fdd06ef14790baec159aa1202'")
					.select("classification.classification")
					.collectAsList()
					.get(0)
					.getString(0));
		Assertions
			.assertEquals(
				"MSCA-IF-2019",
				execverification
					.filter("id = '40|corda__h2020::1a1f235fdd06ef14790baec159aa1202'")
					.select("h2020topiccode")
					.collectAsList()
					.get(0)
					.getString(0));
		Assertions
			.assertEquals(
				"Individual Fellowships",
				execverification
					.filter("id = '40|corda__h2020::1a1f235fdd06ef14790baec159aa1202'")
					.select("h2020topicdescription")
					.collectAsList()
					.get(0)
					.getString(0));

		Assertions
			.assertTrue(
				execverification
					.filter("id = '40|corda__h2020::a657c271769fec90b60c1f2dbc25f4d5'")
					.select("classification.h2020Programme.code")
					.collectAsList()
					.get(0)
					.getString(0)
					.equals("H2020-EU.2.1.4.") ||
					execverification
						.filter("id = '40|corda__h2020::a657c271769fec90b60c1f2dbc25f4d5'")
						.select("classification.h2020Programme.code")
						.collectAsList()
						.get(1)
						.getString(0)
						.equals("H2020-EU.2.1.4."));

		Assertions
			.assertTrue(
				execverification
					.filter("id = '40|corda__h2020::a657c271769fec90b60c1f2dbc25f4d5'")
					.select("classification.h2020Programme.code")
					.collectAsList()
					.get(0)
					.getString(0)
					.equals("H2020-EU.3.2.6.") ||
					execverification
						.filter("id = '40|corda__h2020::a657c271769fec90b60c1f2dbc25f4d5'")
						.select("classification.h2020Programme.code")
						.collectAsList()
						.get(1)
						.getString(0)
						.equals("H2020-EU.3.2.6."));
		Assertions
			.assertEquals(
				"Biotechnology",
				execverification
					.filter(
						"id = '40|corda__h2020::a657c271769fec90b60c1f2dbc25f4d5' and classification.h2020Programme.code = 'H2020-EU.2.1.4.'")
					.select("classification.h2020Programme.description")
					.collectAsList()
					.get(0)
					.getString(0));
		Assertions
			.assertEquals(
				"Bio-based Industries Joint Technology Initiative (BBI-JTI)",
				execverification
					.filter(
						"id = '40|corda__h2020::a657c271769fec90b60c1f2dbc25f4d5' and classification.h2020Programme.code = 'H2020-EU.3.2.6.'")
					.select("classification.h2020Programme.description")
					.collectAsList()
					.get(0)
					.getString(0));
		Assertions
			.assertEquals(
				"BBI-2019-SO3-D4",
				execverification
					.filter("id = '40|corda__h2020::a657c271769fec90b60c1f2dbc25f4d5'")
					.select("h2020topiccode")
					.collectAsList()
					.get(0)
					.getString(0));
		Assertions
			.assertEquals(
				"Demonstrate bio-based pesticides and/or biostimulant agents for sustainable increase in agricultural productivity",
				execverification
					.filter("id = '40|corda__h2020::a657c271769fec90b60c1f2dbc25f4d5'")
					.select("h2020topicdescription")
					.collectAsList()
					.get(0)
					.getString(0));

	}
}
