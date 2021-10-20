
package eu.dnetlib.dhp.resulttoorganizationfromsemrel;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.KeyValueSet;
import eu.dnetlib.dhp.projecttoresult.SparkResultToProjectThroughSemRelJob;
import eu.dnetlib.dhp.schema.oaf.Relation;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class PrepareInfoJobTest {

	private static final Logger log = LoggerFactory.getLogger(PrepareInfoJobTest.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SparkSession spark;

	private static Path workingDir;

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files.createTempDirectory(PrepareInfoJobTest.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(PrepareInfoJobTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(PrepareInfoJobTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}


	@Test
	public  void childParentTest1()  {

		PrepareInfo.prepareChildrenParent(spark, getClass()
				.getResource(
						"/eu/dnetlib/dhp/resulttoorganizationfromsemrel/childparenttest1")
				.getPath(), workingDir.toString() + "/childParentOrg/");

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<KeyValueSet> tmp = sc
				.textFile( workingDir.toString() + "/childParentOrg/")
				.map(item -> OBJECT_MAPPER.readValue(item, KeyValueSet.class));

		Dataset<KeyValueSet> verificationDs = spark.createDataset(tmp.rdd(), Encoders.bean(KeyValueSet.class));

		Assertions.assertEquals(6, verificationDs.count());

		Assertions.assertEquals(1, verificationDs.filter("key = '20|dedup_wf_001::2899e571609779168222fdeb59cb916d'")
				.collectAsList().get(0).getValueSet().size());
		Assertions.assertEquals("20|doajarticles::2baa9032dc058d3c8ff780c426b0c19f",
				verificationDs.filter("key = '20|dedup_wf_001::2899e571609779168222fdeb59cb916d'")
						.collectAsList().get(0).getValueSet().get(0));

		Assertions.assertEquals(2, verificationDs.filter("key = '20|pippo_wf_001::2899e571609779168222fdeb59cb916d'")
				.collectAsList().get(0).getValueSet().size());
		Assertions.assertTrue(verificationDs.filter("key = '20|pippo_wf_001::2899e571609779168222fdeb59cb916d'")
				.collectAsList().get(0).getValueSet().contains("20|dedup_wf_001::2899e571609779168222fdeb59cb916d"));
		Assertions.assertTrue(verificationDs.filter("key = '20|pippo_wf_001::2899e571609779168222fdeb59cb916d'")
				.collectAsList().get(0).getValueSet().contains("20|doajarticles::2baa9032dc058d3c8ff780c426b0c19f"));

		Assertions.assertEquals(1, verificationDs.filter("key = '20|doajarticles::396262ee936f3d3e26ff0e60bea6cae0'")
				.collectAsList().get(0).getValueSet().size());
		Assertions.assertTrue(verificationDs.filter("key = '20|doajarticles::396262ee936f3d3e26ff0e60bea6cae0'")
				.collectAsList().get(0).getValueSet().contains("20|dedup_wf_001::2899e571609779168222fdeb59cb916d"));

		Assertions.assertEquals(1, verificationDs.filter("key = '20|doajarticles::2baa9032dc058d3c8ff780c426b0c19f'")
				.collectAsList().get(0).getValueSet().size());
		Assertions.assertTrue(verificationDs.filter("key = '20|doajarticles::2baa9032dc058d3c8ff780c426b0c19f'")
				.collectAsList().get(0).getValueSet().contains("20|doajarticles::03748bcb5d754c951efec9700e18a56d"));

		Assertions.assertEquals(1, verificationDs.filter("key = '20|doajarticles::1cae0b82b56ccd97c2db1f698def7074'")
				.collectAsList().get(0).getValueSet().size());
		Assertions.assertTrue(verificationDs.filter("key = '20|doajarticles::1cae0b82b56ccd97c2db1f698def7074'")
				.collectAsList().get(0).getValueSet().contains("20|openaire____::ec653e804967133b9436fdd30d3ff51d"));

		Assertions.assertEquals(1, verificationDs.filter("key = '20|doajarticles::2baa9032dc058d3c8ff780c426b0c19f'")
				.collectAsList().get(0).getValueSet().size());
		Assertions.assertTrue(verificationDs.filter("key = '20|opendoar____::a5fcb8eb25ebd6f7cd219e0fa1e6ddc1'")
				.collectAsList().get(0).getValueSet().contains("20|doajarticles::1cae0b82b56ccd97c2db1f698def7074"));

	}



	@Test
	public  void childParentTest2()  {

		PrepareInfo.prepareChildrenParent(spark, getClass()
				.getResource(
						"/eu/dnetlib/dhp/resulttoorganizationfromsemrel/childparenttest2")
				.getPath(), workingDir.toString() + "/childParentOrg/");

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<KeyValueSet> tmp = sc
				.textFile( workingDir.toString() + "/childParentOrg/")
				.map(item -> OBJECT_MAPPER.readValue(item, KeyValueSet.class));

		Dataset<KeyValueSet> verificationDs = spark.createDataset(tmp.rdd(), Encoders.bean(KeyValueSet.class));

		Assertions.assertEquals(5, verificationDs.count());

		Assertions.assertEquals(0, verificationDs.filter("key = '20|dedup_wf_001::2899e571609779168222fdeb59cb916d'").count());

		Assertions.assertEquals(1, verificationDs.filter("key = '20|pippo_wf_001::2899e571609779168222fdeb59cb916d'")
				.collectAsList().get(0).getValueSet().size());
		Assertions.assertEquals("20|doajarticles::2baa9032dc058d3c8ff780c426b0c19f",
				verificationDs.filter("key = '20|pippo_wf_001::2899e571609779168222fdeb59cb916d'")
				.collectAsList().get(0).getValueSet().get(0));

		Assertions.assertEquals(1, verificationDs.filter("key = '20|doajarticles::396262ee936f3d3e26ff0e60bea6cae0'")
				.collectAsList().get(0).getValueSet().size());
		Assertions.assertTrue(verificationDs.filter("key = '20|doajarticles::396262ee936f3d3e26ff0e60bea6cae0'")
				.collectAsList().get(0).getValueSet().contains("20|dedup_wf_001::2899e571609779168222fdeb59cb916d"));

		Assertions.assertEquals(1, verificationDs.filter("key = '20|doajarticles::2baa9032dc058d3c8ff780c426b0c19f'")
				.collectAsList().get(0).getValueSet().size());
		Assertions.assertTrue(verificationDs.filter("key = '20|doajarticles::2baa9032dc058d3c8ff780c426b0c19f'")
				.collectAsList().get(0).getValueSet().contains("20|doajarticles::03748bcb5d754c951efec9700e18a56d"));

		Assertions.assertEquals(1, verificationDs.filter("key = '20|doajarticles::1cae0b82b56ccd97c2db1f698def7074'")
				.collectAsList().get(0).getValueSet().size());
		Assertions.assertTrue(verificationDs.filter("key = '20|doajarticles::1cae0b82b56ccd97c2db1f698def7074'")
				.collectAsList().get(0).getValueSet().contains("20|openaire____::ec653e804967133b9436fdd30d3ff51d"));

		Assertions.assertEquals(1, verificationDs.filter("key = '20|doajarticles::2baa9032dc058d3c8ff780c426b0c19f'")
				.collectAsList().get(0).getValueSet().size());
		Assertions.assertTrue(verificationDs.filter("key = '20|opendoar____::a5fcb8eb25ebd6f7cd219e0fa1e6ddc1'")
				.collectAsList().get(0).getValueSet().contains("20|doajarticles::1cae0b82b56ccd97c2db1f698def7074"));

	}


	@Test
	public  void resultOrganizationTest1(){

	}

	/**
	 * All the possible updates will produce a new relation. No relations are already linked in the grpha
	 *
	 * @throws Exception
	 */
	@Test
	void UpdateTenTest() throws Exception {
		final String potentialUpdatePath = getClass()
			.getResource(
				"/eu/dnetlib/dhp/projecttoresult/preparedInfo/tenupdates/potentialUpdates")
			.getPath();
		final String alreadyLinkedPath = getClass()
			.getResource(
				"/eu/dnetlib/dhp/projecttoresult/preparedInfo/alreadyLinked")
			.getPath();
		SparkResultToProjectThroughSemRelJob
			.main(
				new String[] {
					"-isTest", Boolean.TRUE.toString(),
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-hive_metastore_uris", "",
					"-saveGraph", "true",
					"-outputPath", workingDir.toString() + "/relation",
					"-potentialUpdatePath", potentialUpdatePath,
					"-alreadyLinkedPath", alreadyLinkedPath,
				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Relation> tmp = sc
			.textFile(workingDir.toString() + "/relation")
			.map(item -> OBJECT_MAPPER.readValue(item, Relation.class));

		// got 20 new relations because "produces" and "isProducedBy" are added
		Assertions.assertEquals(10, tmp.count());

		Dataset<Relation> verificationDs = spark.createDataset(tmp.rdd(), Encoders.bean(Relation.class));

		Assertions.assertEquals(5, verificationDs.filter("relClass = 'produces'").count());
		Assertions.assertEquals(5, verificationDs.filter("relClass = 'isProducedBy'").count());

		Assertions
			.assertEquals(
				5,
				verificationDs
					.filter(
						(FilterFunction<Relation>) r -> r.getSource().startsWith("50")
							&& r.getTarget().startsWith("40")
							&& r.getRelClass().equals("isProducedBy"))
					.count());
		Assertions
			.assertEquals(
				5,
				verificationDs
					.filter(
						(FilterFunction<Relation>) r -> r.getSource().startsWith("40")
							&& r.getTarget().startsWith("50")
							&& r.getRelClass().equals("produces"))
					.count());

		verificationDs.createOrReplaceTempView("temporary");

		Assertions
			.assertEquals(
				10,
				spark
					.sql(
						"Select * from temporary where datainfo.inferenceprovenance = 'propagation'")
					.count());
	}

	/**
	 * One of the relations in the possible updates is already linked to the project in the graph. All the others are
	 * not. There will be 9 new associations leading to 18 new relations
	 *
	 * @throws Exception
	 */
	@Test
	void UpdateMixTest() throws Exception {
		final String potentialUpdatepath = getClass()
			.getResource(
				"/eu/dnetlib/dhp/projecttoresult/preparedInfo/updatesmixed/potentialUpdates")
			.getPath();
		final String alreadyLinkedPath = getClass()
			.getResource(
				"/eu/dnetlib/dhp/projecttoresult/preparedInfo/alreadyLinked")
			.getPath();
		SparkResultToProjectThroughSemRelJob
			.main(
				new String[] {
					"-isTest", Boolean.TRUE.toString(),
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-hive_metastore_uris", "",
					"-saveGraph", "true",
					"-outputPath", workingDir.toString() + "/relation",
					"-potentialUpdatePath", potentialUpdatepath,
					"-alreadyLinkedPath", alreadyLinkedPath,
				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Relation> tmp = sc
			.textFile(workingDir.toString() + "/relation")
			.map(item -> OBJECT_MAPPER.readValue(item, Relation.class));

		// JavaRDD<Relation> tmp = sc.textFile("/tmp/relation")
		// .map(item -> OBJECT_MAPPER.readValue(item, Relation.class));

		// got 20 new relations because "produces" and "isProducedBy" are added
		Assertions.assertEquals(8, tmp.count());

		Dataset<Relation> verificationDs = spark.createDataset(tmp.rdd(), Encoders.bean(Relation.class));

		Assertions.assertEquals(4, verificationDs.filter("relClass = 'produces'").count());
		Assertions.assertEquals(4, verificationDs.filter("relClass = 'isProducedBy'").count());

		Assertions
			.assertEquals(
				4,
				verificationDs
					.filter(
						(FilterFunction<Relation>) r -> r.getSource().startsWith("50")
							&& r.getTarget().startsWith("40")
							&& r.getRelClass().equals("isProducedBy"))
					.count());
		Assertions
			.assertEquals(
				4,
				verificationDs
					.filter(
						(FilterFunction<Relation>) r -> r.getSource().startsWith("40")
							&& r.getTarget().startsWith("50")
							&& r.getRelClass().equals("produces"))
					.count());

		verificationDs.createOrReplaceTempView("temporary");

		Assertions
			.assertEquals(
				8,
				spark
					.sql(
						"Select * from temporary where datainfo.inferenceprovenance = 'propagation'")
					.count());
	}
}
