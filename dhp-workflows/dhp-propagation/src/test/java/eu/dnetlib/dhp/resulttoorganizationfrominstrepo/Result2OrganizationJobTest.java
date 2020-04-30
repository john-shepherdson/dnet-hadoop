
package eu.dnetlib.dhp.resulttoorganizationfrominstrepo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
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

import eu.dnetlib.dhp.schema.oaf.Relation;

public class Result2OrganizationJobTest {

	private static final Logger log = LoggerFactory.getLogger(Result2OrganizationJobTest.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static final ClassLoader cl = Result2OrganizationJobTest.class.getClassLoader();

	private static SparkSession spark;

	private static Path workingDir;

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files
			.createTempDirectory(
				SparkResultToOrganizationFromIstRepoJob2.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(SparkResultToOrganizationFromIstRepoJob2.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(SparkResultToOrganizationFromIstRepoJob2.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	/**
	 * No modifications done to the sample sets, so that no possible updates are created
	 *
	 * @throws Exception
	 */
	@Test
	public void NoUpdateTest() throws Exception {
		SparkResultToOrganizationFromIstRepoJob2
			.main(
				new String[] {
					"-isTest",
					Boolean.TRUE.toString(),
					"-isSparkSessionManaged",
					Boolean.FALSE.toString(),
					"-sourcePath",
					getClass()
						.getResource(
							"/eu/dnetlib/dhp/resulttoorganizationfrominstrepo/sample/noupdate_updatenomix")
						.getPath(),
					"-hive_metastore_uris",
					"",
					"-resultTableName",
					"eu.dnetlib.dhp.schema.oaf.Software",

					"-saveGraph",
					"true",
					"-outputPath",
					workingDir.toString() + "/relation",
					"-datasourceOrganizationPath",
					getClass()
						.getResource(
							"/eu/dnetlib/dhp/resulttoorganizationfrominstrepo/noupdate/preparedInfo/datasourceOrganization")
						.getPath(),
					"-alreadyLinkedPath",
					getClass()
						.getResource(
							"/eu/dnetlib/dhp/resulttoorganizationfrominstrepo/noupdate/preparedInfo/alreadyLinked")
						.getPath(),
				});

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaRDD<Relation> tmp = sc
			.textFile(workingDir.toString() + "/relation")
			.map(item -> OBJECT_MAPPER.readValue(item, Relation.class));

		Assertions.assertEquals(0, tmp.count());
	}

	/**
	 * Testing set with modified association between datasource and organization. Copied some hostedby collectedfrom
	 * from the software sample set. No intersection with the already linked (all the possible new relations, will
	 * became new relations)
	 *
	 * @throws Exception
	 */
	@Test
	public void UpdateNoMixTest() throws Exception {
		SparkResultToOrganizationFromIstRepoJob2
			.main(
				new String[] {
					"-isTest",
					Boolean.TRUE.toString(),
					"-isSparkSessionManaged",
					Boolean.FALSE.toString(),
					"-sourcePath",
					getClass()
						.getResource(
							"/eu/dnetlib/dhp/resulttoorganizationfrominstrepo/sample/noupdate_updatenomix")
						.getPath(),
					"-hive_metastore_uris",
					"",
					"-resultTableName",
					"eu.dnetlib.dhp.schema.oaf.Software",

					"-saveGraph",
					"true",
					"-outputPath",
					workingDir.toString() + "/relation",
					"-datasourceOrganizationPath",
					getClass()
						.getResource(
							"/eu/dnetlib/dhp/resulttoorganizationfrominstrepo/updatenomix/preparedInfo/datasourceOrganization")
						.getPath(),
					"-alreadyLinkedPath",
					getClass()
						.getResource(
							"/eu/dnetlib/dhp/resulttoorganizationfrominstrepo/updatenomix/preparedInfo/alreadyLinked")
						.getPath(),
				});

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaRDD<Relation> tmp = sc
			.textFile(workingDir.toString() + "/relation")
			.map(item -> OBJECT_MAPPER.readValue(item, Relation.class));

		Assertions.assertEquals(20, tmp.count());

		Dataset<Relation> verificationDs = spark.createDataset(tmp.rdd(), Encoders.bean(Relation.class));
		Assertions
			.assertEquals(
				8,
				verificationDs
					.filter("target = '20|dedup_wf_001::5168917a6aeeea55269daeac1af2ecd2'")
					.count());
		Assertions
			.assertEquals(
				1,
				verificationDs
					.filter("target = '20|opendoar____::124266ebc4ece2934eb80edfda3f2091'")
					.count());
		Assertions
			.assertEquals(
				1,
				verificationDs
					.filter("target = '20|opendoar____::4429502fa1936b0941f4647b69b844c8'")
					.count());

		Assertions
			.assertEquals(
				2,
				verificationDs
					.filter(
						"source = '50|dedup_wf_001::b67bc915603fc01e445f2b5888ba7218' and "
							+ "(target = '20|opendoar____::124266ebc4ece2934eb80edfda3f2091' "
							+ "or target = '20|dedup_wf_001::5168917a6aeeea55269daeac1af2ecd2')")
					.count());
	}

	@Test
	public void UpdateMixTest() throws Exception {
		SparkResultToOrganizationFromIstRepoJob2
			.main(
				new String[] {
					"-isTest",
					Boolean.TRUE.toString(),
					"-isSparkSessionManaged",
					Boolean.FALSE.toString(),
					"-sourcePath",
					getClass()
						.getResource(
							"/eu/dnetlib/dhp/resulttoorganizationfrominstrepo/sample/updatemix")
						.getPath(),
					"-hive_metastore_uris",
					"",
					"-resultTableName",
					"eu.dnetlib.dhp.schema.oaf.Software",

					"-saveGraph",
					"true",
					"-outputPath",
					workingDir.toString() + "/relation",
					"-datasourceOrganizationPath",
					getClass()
						.getResource(
							"/eu/dnetlib/dhp/resulttoorganizationfrominstrepo/updatemix/preparedInfo/datasourceOrganization")
						.getPath(),
					"-alreadyLinkedPath",
					getClass()
						.getResource(
							"/eu/dnetlib/dhp/resulttoorganizationfrominstrepo/updatemix/preparedInfo/alreadyLinked")
						.getPath(),
				});

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaRDD<Relation> tmp = sc
			.textFile(workingDir.toString() + "/relation")
			.map(item -> OBJECT_MAPPER.readValue(item, Relation.class));

		Dataset<Relation> verificationDs = spark.createDataset(tmp.rdd(), Encoders.bean(Relation.class));

		Assertions.assertEquals(8, verificationDs.count());

		Assertions
			.assertEquals(
				2,
				verificationDs
					.filter("source = '50|od_______109::f375befa62a741e9250e55bcfa88f9a6'")
					.count());
		Assertions
			.assertEquals(
				1,
				verificationDs
					.filter("source = '50|dedup_wf_001::b67bc915603fc01e445f2b5888ba7218'")
					.count());
		Assertions
			.assertEquals(
				1,
				verificationDs
					.filter("source = '50|dedup_wf_001::40ea2f24181f6ae77b866ebcbffba523'")
					.count());

		Assertions
			.assertEquals(
				1,
				verificationDs
					.filter("source = '20|wt__________::a72760363ca885e6bef165804770e00c'")
					.count());

		Assertions
			.assertEquals(
				4,
				verificationDs
					.filter(
						"relclass = 'hasAuthorInstitution' and substring(source, 1,2) = '50'")
					.count());
		Assertions
			.assertEquals(
				4,
				verificationDs
					.filter(
						"relclass = 'isAuthorInstitutionOf' and substring(source, 1,2) = '20'")
					.count());

		Assertions
			.assertEquals(
				4,
				verificationDs
					.filter(
						"relclass = 'hasAuthorInstitution' and "
							+ "substring(source, 1,2) = '50' and substring(target, 1, 2) = '20'")
					.count());
		Assertions
			.assertEquals(
				4,
				verificationDs
					.filter(
						"relclass = 'isAuthorInstitutionOf' and "
							+ "substring(source, 1,2) = '20' and substring(target, 1, 2) = '50'")
					.count());
	}
}
