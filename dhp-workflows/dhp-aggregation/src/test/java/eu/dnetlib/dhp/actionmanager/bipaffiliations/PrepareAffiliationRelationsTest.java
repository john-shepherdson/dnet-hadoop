
package eu.dnetlib.dhp.actionmanager.bipaffiliations;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
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
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory;
import eu.dnetlib.dhp.schema.oaf.utils.PidCleaner;

public class PrepareAffiliationRelationsTest {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SparkSession spark;

	private static Path workingDir;
	private static final String ID_PREFIX = "50|doi_________::";
	private static final Logger log = LoggerFactory.getLogger(PrepareAffiliationRelationsTest.class);

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files.createTempDirectory(PrepareAffiliationRelationsTest.class.getSimpleName());

		log.info("Using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(PrepareAffiliationRelationsTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(PrepareAffiliationRelationsTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	@Test
	void testMatch() throws Exception {

		String crossrefAffiliationRelationPathNew = getClass()
			.getResource("/eu/dnetlib/dhp/actionmanager/bipaffiliations/doi_to_ror.json")
			.getPath();

		String crossrefAffiliationRelationPath = getClass()
			.getResource("/eu/dnetlib/dhp/actionmanager/bipaffiliations/doi_to_ror_old.json")
			.getPath();

		String publisherAffiliationRelationPath = getClass()
			.getResource("/eu/dnetlib/dhp/actionmanager/bipaffiliations/publishers")
			.getPath();

		String publisherAffiliationRelationOldPath = getClass()
			.getResource("/eu/dnetlib/dhp/actionmanager/bipaffiliations/publichers_old")
			.getPath();

		String outputPath = workingDir.toString() + "/actionSet";

		PrepareAffiliationRelations
			.main(
				new String[] {
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-crossrefInputPath", crossrefAffiliationRelationPathNew,
					"-pubmedInputPath", crossrefAffiliationRelationPath,
					"-openapcInputPath", crossrefAffiliationRelationPathNew,
					"-dataciteInputPath", crossrefAffiliationRelationPathNew,
					"-webCrawlInputPath", crossrefAffiliationRelationPathNew,
					"-publisherInputPath", publisherAffiliationRelationPath,
					"-outputPath", outputPath
				});

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaRDD<Relation> tmp = sc
			.sequenceFile(outputPath, Text.class, Text.class)
			.map(value -> OBJECT_MAPPER.readValue(value._2().toString(), AtomicAction.class))
			.map(aa -> ((Relation) aa.getPayload()));

		// count the number of relations
		assertEquals(162, tmp.count());// 18 + 24  + 30 * 4 =

		Dataset<Relation> dataset = spark.createDataset(tmp.rdd(), Encoders.bean(Relation.class));
		dataset.createOrReplaceTempView("result");

		Dataset<Row> execVerification = spark
			.sql("select r.relType, r.relClass, r.source, r.target, r.dataInfo.trust from result r");

		// verify that we have equal number of bi-directional relations
		Assertions
			.assertEquals(
				81, execVerification
					.filter(
						"relClass='" + ModelConstants.HAS_AUTHOR_INSTITUTION + "'")
					.collectAsList()
					.size());

		Assertions
			.assertEquals(
				81, execVerification
					.filter(
						"relClass='" + ModelConstants.IS_AUTHOR_INSTITUTION_OF + "'")
					.collectAsList()
					.size());

		// check confidence value of a specific relation
		String sourceDOI = "10.1089/10872910260066679";

		final String sourceOpenaireId = ID_PREFIX
			+ IdentifierFactory.md5(PidCleaner.normalizePidValue("doi", sourceDOI));

		Assertions
			.assertEquals(
				"1.0", execVerification
					.filter(
						"source='" + sourceOpenaireId + "'")
					.collectAsList()
					.get(0)
					.getString(4));

		final String publisherid = ID_PREFIX
			+ IdentifierFactory.md5(PidCleaner.normalizePidValue("doi", "10.1089/10872910260066679"));
		final String rorId = "20|ror_________::" + IdentifierFactory.md5("https://ror.org/05cf8a891");

		Assertions
			.assertEquals(
				4, execVerification.filter("source = '" + publisherid + "' and target = '" + rorId + "'").count());

		Assertions
			.assertEquals(
				1, execVerification
					.filter(
						"source = '" + ID_PREFIX
							+ IdentifierFactory
								.md5(PidCleaner.normalizePidValue("doi", "10.1007/s00217-010-1268-9"))
							+ "' and target = '" + "20|ror_________::"
							+ IdentifierFactory.md5("https://ror.org/03265fv13") + "'")
					.count());

		Assertions
			.assertEquals(
				1, execVerification
					.filter(
						"source = '" + ID_PREFIX
							+ IdentifierFactory
								.md5(PidCleaner.normalizePidValue("doi", "10.1007/3-540-47984-8_14"))
							+ "' and target = '" + "20|ror_________::"
							+ IdentifierFactory.md5("https://ror.org/00a0n9e72") + "'")
					.count());

	}
}
