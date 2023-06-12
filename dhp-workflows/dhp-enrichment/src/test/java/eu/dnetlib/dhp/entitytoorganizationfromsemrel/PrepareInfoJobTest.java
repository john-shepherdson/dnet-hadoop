
package eu.dnetlib.dhp.entitytoorganizationfromsemrel;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
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

import eu.dnetlib.dhp.KeyValueSet;
import eu.dnetlib.dhp.schema.oaf.Relation;

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
	public void childParentTest1() throws Exception {

		PrepareInfo
			.main(
				new String[] {
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-graphPath", getClass()
						.getResource(
							"/eu/dnetlib/dhp/entitytoorganizationfromsemrel/childparenttest1")
						.getPath(),
					"-hive_metastore_uris", "",
					"-leavesPath", workingDir.toString() + "/currentIteration/",
					"-resultOrgPath", workingDir.toString() + "/resultOrganization/",
					"-projectOrganizationPath", workingDir.toString() + "/projectOrganization/",
					"-childParentPath", workingDir.toString() + "/childParentOrg/",
					"-relationPath", workingDir.toString() + "/relation"

				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<KeyValueSet> tmp = sc
			.textFile(workingDir.toString() + "/childParentOrg/")
			.map(item -> OBJECT_MAPPER.readValue(item, KeyValueSet.class));

		Dataset<KeyValueSet> verificationDs = spark.createDataset(tmp.rdd(), Encoders.bean(KeyValueSet.class));

		Assertions.assertEquals(6, verificationDs.count());

		Assertions
			.assertEquals(
				1, verificationDs
					.filter("key = '20|dedup_wf_001::2899e571609779168222fdeb59cb916d'")
					.collectAsList()
					.get(0)
					.getValueSet()
					.size());
		Assertions
			.assertEquals(
				"20|doajarticles::2baa9032dc058d3c8ff780c426b0c19f",
				verificationDs
					.filter("key = '20|dedup_wf_001::2899e571609779168222fdeb59cb916d'")
					.collectAsList()
					.get(0)
					.getValueSet()
					.get(0));

		Assertions
			.assertEquals(
				2, verificationDs
					.filter("key = '20|pippo_wf_001::2899e571609779168222fdeb59cb916d'")
					.collectAsList()
					.get(0)
					.getValueSet()
					.size());
		Assertions
			.assertTrue(
				verificationDs
					.filter("key = '20|pippo_wf_001::2899e571609779168222fdeb59cb916d'")
					.collectAsList()
					.get(0)
					.getValueSet()
					.contains("20|dedup_wf_001::2899e571609779168222fdeb59cb916d"));
		Assertions
			.assertTrue(
				verificationDs
					.filter("key = '20|pippo_wf_001::2899e571609779168222fdeb59cb916d'")
					.collectAsList()
					.get(0)
					.getValueSet()
					.contains("20|doajarticles::2baa9032dc058d3c8ff780c426b0c19f"));

		Assertions
			.assertEquals(
				1, verificationDs
					.filter("key = '20|doajarticles::396262ee936f3d3e26ff0e60bea6cae0'")
					.collectAsList()
					.get(0)
					.getValueSet()
					.size());
		Assertions
			.assertTrue(
				verificationDs
					.filter("key = '20|doajarticles::396262ee936f3d3e26ff0e60bea6cae0'")
					.collectAsList()
					.get(0)
					.getValueSet()
					.contains("20|dedup_wf_001::2899e571609779168222fdeb59cb916d"));

		Assertions
			.assertEquals(
				1, verificationDs
					.filter("key = '20|doajarticles::2baa9032dc058d3c8ff780c426b0c19f'")
					.collectAsList()
					.get(0)
					.getValueSet()
					.size());
		Assertions
			.assertTrue(
				verificationDs
					.filter("key = '20|doajarticles::2baa9032dc058d3c8ff780c426b0c19f'")
					.collectAsList()
					.get(0)
					.getValueSet()
					.contains("20|doajarticles::03748bcb5d754c951efec9700e18a56d"));

		Assertions
			.assertEquals(
				1, verificationDs
					.filter("key = '20|doajarticles::1cae0b82b56ccd97c2db1f698def7074'")
					.collectAsList()
					.get(0)
					.getValueSet()
					.size());
		Assertions
			.assertTrue(
				verificationDs
					.filter("key = '20|doajarticles::1cae0b82b56ccd97c2db1f698def7074'")
					.collectAsList()
					.get(0)
					.getValueSet()
					.contains("20|openaire____::ec653e804967133b9436fdd30d3ff51d"));

		Assertions
			.assertEquals(
				1, verificationDs
					.filter("key = '20|doajarticles::2baa9032dc058d3c8ff780c426b0c19f'")
					.collectAsList()
					.get(0)
					.getValueSet()
					.size());
		Assertions
			.assertTrue(
				verificationDs
					.filter("key = '20|opendoar____::a5fcb8eb25ebd6f7cd219e0fa1e6ddc1'")
					.collectAsList()
					.get(0)
					.getValueSet()
					.contains("20|doajarticles::1cae0b82b56ccd97c2db1f698def7074"));

		verificationDs
			.foreach((ForeachFunction<KeyValueSet>) v -> System.out.println(OBJECT_MAPPER.writeValueAsString(v)));

	}

	@Test
	public void childParentTest2() throws Exception {

		PrepareInfo
			.main(
				new String[] {
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-graphPath", getClass()
						.getResource(
							"/eu/dnetlib/dhp/entitytoorganizationfromsemrel/childparenttest2")
						.getPath(),
					"-hive_metastore_uris", "",
					"-leavesPath", workingDir.toString() + "/currentIteration/",
					"-resultOrgPath", workingDir.toString() + "/resultOrganization/",
					"-projectOrganizationPath", workingDir.toString() + "/projectOrganization/",
					"-childParentPath", workingDir.toString() + "/childParentOrg/",
					"-relationPath", workingDir.toString() + "/relation"

				});
		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<KeyValueSet> tmp = sc
			.textFile(workingDir.toString() + "/childParentOrg/")
			.map(item -> OBJECT_MAPPER.readValue(item, KeyValueSet.class));

		Dataset<KeyValueSet> verificationDs = spark.createDataset(tmp.rdd(), Encoders.bean(KeyValueSet.class));

		Assertions.assertEquals(5, verificationDs.count());

		Assertions
			.assertEquals(
				0, verificationDs.filter("key = '20|dedup_wf_001::2899e571609779168222fdeb59cb916d'").count());

		Assertions
			.assertEquals(
				1, verificationDs
					.filter("key = '20|pippo_wf_001::2899e571609779168222fdeb59cb916d'")
					.collectAsList()
					.get(0)
					.getValueSet()
					.size());
		Assertions
			.assertEquals(
				"20|doajarticles::2baa9032dc058d3c8ff780c426b0c19f",
				verificationDs
					.filter("key = '20|pippo_wf_001::2899e571609779168222fdeb59cb916d'")
					.collectAsList()
					.get(0)
					.getValueSet()
					.get(0));

		Assertions
			.assertEquals(
				1, verificationDs
					.filter("key = '20|doajarticles::396262ee936f3d3e26ff0e60bea6cae0'")
					.collectAsList()
					.get(0)
					.getValueSet()
					.size());
		Assertions
			.assertTrue(
				verificationDs
					.filter("key = '20|doajarticles::396262ee936f3d3e26ff0e60bea6cae0'")
					.collectAsList()
					.get(0)
					.getValueSet()
					.contains("20|dedup_wf_001::2899e571609779168222fdeb59cb916d"));

		Assertions
			.assertEquals(
				1, verificationDs
					.filter("key = '20|doajarticles::2baa9032dc058d3c8ff780c426b0c19f'")
					.collectAsList()
					.get(0)
					.getValueSet()
					.size());
		Assertions
			.assertTrue(
				verificationDs
					.filter("key = '20|doajarticles::2baa9032dc058d3c8ff780c426b0c19f'")
					.collectAsList()
					.get(0)
					.getValueSet()
					.contains("20|doajarticles::03748bcb5d754c951efec9700e18a56d"));

		Assertions
			.assertEquals(
				1, verificationDs
					.filter("key = '20|doajarticles::1cae0b82b56ccd97c2db1f698def7074'")
					.collectAsList()
					.get(0)
					.getValueSet()
					.size());
		Assertions
			.assertTrue(
				verificationDs
					.filter("key = '20|doajarticles::1cae0b82b56ccd97c2db1f698def7074'")
					.collectAsList()
					.get(0)
					.getValueSet()
					.contains("20|openaire____::ec653e804967133b9436fdd30d3ff51d"));

		Assertions
			.assertEquals(
				1, verificationDs
					.filter("key = '20|doajarticles::2baa9032dc058d3c8ff780c426b0c19f'")
					.collectAsList()
					.get(0)
					.getValueSet()
					.size());
		Assertions
			.assertTrue(
				verificationDs
					.filter("key = '20|opendoar____::a5fcb8eb25ebd6f7cd219e0fa1e6ddc1'")
					.collectAsList()
					.get(0)
					.getValueSet()
					.contains("20|doajarticles::1cae0b82b56ccd97c2db1f698def7074"));

	}

	@Test
	public void relationTest() throws Exception {

		PrepareInfo
			.main(
				new String[] {
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-graphPath", getClass()
						.getResource(
							"/eu/dnetlib/dhp/entitytoorganizationfromsemrel/resultorganizationtest")
						.getPath(),
					"-hive_metastore_uris", "",
					"-leavesPath", workingDir.toString() + "/currentIteration/",
					"-resultOrgPath", workingDir.toString() + "/resultOrganization/",
					"-projectOrganizationPath", workingDir.toString() + "/projectOrganization/",
					"-childParentPath", workingDir.toString() + "/childParentOrg/",
					"-relationPath", workingDir.toString() + "/relation"

				});
		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Relation> tmp = sc
			.textFile(workingDir.toString() + "/relation/result")
			.map(item -> OBJECT_MAPPER.readValue(item, Relation.class));

		Dataset<Relation> verificationDs = spark.createDataset(tmp.rdd(), Encoders.bean(Relation.class));

		Assertions.assertEquals(7, verificationDs.count());

	}

	@Test
	public void relationProjectTest() throws Exception {

		PrepareInfo
			.main(
				new String[] {
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-graphPath", getClass()
						.getResource(
							"/eu/dnetlib/dhp/entitytoorganizationfromsemrel/projectorganizationtest")
						.getPath(),
					"-hive_metastore_uris", "",
					"-leavesPath", workingDir.toString() + "/currentIteration/",
					"-resultOrgPath", workingDir.toString() + "/resultOrganization/",
					"-projectOrganizationPath", workingDir.toString() + "/projectOrganization/",
					"-childParentPath", workingDir.toString() + "/childParentOrg/",
					"-relationPath", workingDir.toString() + "/relation"

				});
		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Relation> tmp = sc
			.textFile(workingDir.toString() + "/relation/project")
			.map(item -> OBJECT_MAPPER.readValue(item, Relation.class));

		Dataset<Relation> verificationDs = spark.createDataset(tmp.rdd(), Encoders.bean(Relation.class));

		Assertions.assertEquals(7, verificationDs.count());

	}

	@Test
	public void resultOrganizationTest1() throws Exception {

		PrepareInfo
			.main(
				new String[] {
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-graphPath", getClass()
						.getResource(
							"/eu/dnetlib/dhp/entitytoorganizationfromsemrel/resultorganizationtest")
						.getPath(),
					"-hive_metastore_uris", "",
					"-leavesPath", workingDir.toString() + "/currentIteration/",
					"-resultOrgPath", workingDir.toString() + "/resultOrganization/",
					"-projectOrganizationPath", workingDir.toString() + "/projectOrganization/",
					"-childParentPath", workingDir.toString() + "/childParentOrg/",
					"-relationPath", workingDir.toString() + "/relation"

				});
		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<KeyValueSet> tmp = sc
			.textFile(workingDir.toString() + "/resultOrganization/")
			.map(item -> OBJECT_MAPPER.readValue(item, KeyValueSet.class));

		Dataset<KeyValueSet> verificationDs = spark.createDataset(tmp.rdd(), Encoders.bean(KeyValueSet.class));

		Assertions.assertEquals(5, verificationDs.count());

		Assertions
			.assertEquals(
				2, verificationDs
					.filter("key = '50|doajarticles::2baa9032dc058d3c8ff780c426b0c19f'")
					.collectAsList()
					.get(0)
					.getValueSet()
					.size());
		Assertions
			.assertTrue(
				verificationDs
					.filter("key = '50|doajarticles::2baa9032dc058d3c8ff780c426b0c19f'")
					.collectAsList()
					.get(0)
					.getValueSet()
					.contains("20|dedup_wf_001::2899e571609779168222fdeb59cb916d"));
		Assertions
			.assertTrue(
				verificationDs
					.filter("key = '50|doajarticles::2baa9032dc058d3c8ff780c426b0c19f'")
					.collectAsList()
					.get(0)
					.getValueSet()
					.contains("20|pippo_wf_001::2899e571609779168222fdeb59cb916d"));

		Assertions
			.assertEquals(
				2, verificationDs
					.filter("key = '50|dedup_wf_001::2899e571609779168222fdeb59cb916d'")
					.collectAsList()
					.get(0)
					.getValueSet()
					.size());
		Assertions
			.assertTrue(
				verificationDs
					.filter("key = '50|dedup_wf_001::2899e571609779168222fdeb59cb916d'")
					.collectAsList()
					.get(0)
					.getValueSet()
					.contains("20|doajarticles::396262ee936f3d3e26ff0e60bea6cae0"));
		Assertions
			.assertTrue(
				verificationDs
					.filter("key = '50|dedup_wf_001::2899e571609779168222fdeb59cb916d'")
					.collectAsList()
					.get(0)
					.getValueSet()
					.contains("20|pippo_wf_001::2899e571609779168222fdeb59cb916d"));

		Assertions
			.assertEquals(
				1, verificationDs
					.filter("key = '50|doajarticles::03748bcb5d754c951efec9700e18a56d'")
					.collectAsList()
					.get(0)
					.getValueSet()
					.size());
		Assertions
			.assertTrue(
				verificationDs
					.filter("key = '50|doajarticles::03748bcb5d754c951efec9700e18a56d'")
					.collectAsList()
					.get(0)
					.getValueSet()
					.contains("20|doajarticles::2baa9032dc058d3c8ff780c426b0c19f"));

		Assertions
			.assertEquals(
				1, verificationDs
					.filter("key = '50|openaire____::ec653e804967133b9436fdd30d3ff51d'")
					.collectAsList()
					.get(0)
					.getValueSet()
					.size());
		Assertions
			.assertTrue(
				verificationDs
					.filter("key = '50|openaire____::ec653e804967133b9436fdd30d3ff51d'")
					.collectAsList()
					.get(0)
					.getValueSet()
					.contains("20|doajarticles::1cae0b82b56ccd97c2db1f698def7074"));

		Assertions
			.assertEquals(
				1, verificationDs
					.filter("key = '50|doajarticles::1cae0b82b56ccd97c2db1f698def7074'")
					.collectAsList()
					.get(0)
					.getValueSet()
					.size());
		Assertions
			.assertTrue(
				verificationDs
					.filter("key = '50|doajarticles::1cae0b82b56ccd97c2db1f698def7074'")
					.collectAsList()
					.get(0)
					.getValueSet()
					.contains("20|opendoar____::a5fcb8eb25ebd6f7cd219e0fa1e6ddc1"));

		verificationDs
			.foreach((ForeachFunction<KeyValueSet>) v -> System.out.println(OBJECT_MAPPER.writeValueAsString(v)));

	}

	@Test
	public void projectOrganizationTest1() throws Exception {

		PrepareInfo
			.main(
				new String[] {
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-graphPath", getClass()
						.getResource(
							"/eu/dnetlib/dhp/entitytoorganizationfromsemrel/projectorganizationtest")
						.getPath(),
					"-hive_metastore_uris", "",
					"-leavesPath", workingDir.toString() + "/currentIteration/",
					"-resultOrgPath", workingDir.toString() + "/resultOrganization/",
					"-projectOrganizationPath", workingDir.toString() + "/projectOrganization/",
					"-childParentPath", workingDir.toString() + "/childParentOrg/",
					"-relationPath", workingDir.toString() + "/relation"

				});
		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<KeyValueSet> tmp = sc
			.textFile(workingDir.toString() + "/projectOrganization/")
			.map(item -> OBJECT_MAPPER.readValue(item, KeyValueSet.class));

		Dataset<KeyValueSet> verificationDs = spark.createDataset(tmp.rdd(), Encoders.bean(KeyValueSet.class));

		Assertions.assertEquals(5, verificationDs.count());

		Assertions
			.assertEquals(
				2, verificationDs
					.filter("key = '40|doajarticles::2baa9032dc058d3c8ff780c426b0c19f'")
					.collectAsList()
					.get(0)
					.getValueSet()
					.size());
		Assertions
			.assertTrue(
				verificationDs
					.filter("key = '40|doajarticles::2baa9032dc058d3c8ff780c426b0c19f'")
					.collectAsList()
					.get(0)
					.getValueSet()
					.contains("20|dedup_wf_001::2899e571609779168222fdeb59cb916d"));
		Assertions
			.assertTrue(
				verificationDs
					.filter("key = '40|doajarticles::2baa9032dc058d3c8ff780c426b0c19f'")
					.collectAsList()
					.get(0)
					.getValueSet()
					.contains("20|pippo_wf_001::2899e571609779168222fdeb59cb916d"));

		Assertions
			.assertEquals(
				2, verificationDs
					.filter("key = '40|dedup_wf_001::2899e571609779168222fdeb59cb916d'")
					.collectAsList()
					.get(0)
					.getValueSet()
					.size());
		Assertions
			.assertTrue(
				verificationDs
					.filter("key = '40|dedup_wf_001::2899e571609779168222fdeb59cb916d'")
					.collectAsList()
					.get(0)
					.getValueSet()
					.contains("20|doajarticles::396262ee936f3d3e26ff0e60bea6cae0"));
		Assertions
			.assertTrue(
				verificationDs
					.filter("key = '40|dedup_wf_001::2899e571609779168222fdeb59cb916d'")
					.collectAsList()
					.get(0)
					.getValueSet()
					.contains("20|pippo_wf_001::2899e571609779168222fdeb59cb916d"));

		Assertions
			.assertEquals(
				1, verificationDs
					.filter("key = '40|doajarticles::03748bcb5d754c951efec9700e18a56d'")
					.collectAsList()
					.get(0)
					.getValueSet()
					.size());
		Assertions
			.assertTrue(
				verificationDs
					.filter("key = '40|doajarticles::03748bcb5d754c951efec9700e18a56d'")
					.collectAsList()
					.get(0)
					.getValueSet()
					.contains("20|doajarticles::2baa9032dc058d3c8ff780c426b0c19f"));

		Assertions
			.assertEquals(
				1, verificationDs
					.filter("key = '40|openaire____::ec653e804967133b9436fdd30d3ff51d'")
					.collectAsList()
					.get(0)
					.getValueSet()
					.size());
		Assertions
			.assertTrue(
				verificationDs
					.filter("key = '40|openaire____::ec653e804967133b9436fdd30d3ff51d'")
					.collectAsList()
					.get(0)
					.getValueSet()
					.contains("20|doajarticles::1cae0b82b56ccd97c2db1f698def7074"));

		Assertions
			.assertEquals(
				1, verificationDs
					.filter("key = '40|doajarticles::1cae0b82b56ccd97c2db1f698def7074'")
					.collectAsList()
					.get(0)
					.getValueSet()
					.size());
		Assertions
			.assertTrue(
				verificationDs
					.filter("key = '40|doajarticles::1cae0b82b56ccd97c2db1f698def7074'")
					.collectAsList()
					.get(0)
					.getValueSet()
					.contains("20|opendoar____::a5fcb8eb25ebd6f7cd219e0fa1e6ddc1"));

		verificationDs
			.foreach((ForeachFunction<KeyValueSet>) v -> System.out.println(OBJECT_MAPPER.writeValueAsString(v)));

	}

	@Test
	public void foundLeavesTest1() throws Exception {

		PrepareInfo
			.main(
				new String[] {
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-graphPath", getClass()
						.getResource(
							"/eu/dnetlib/dhp/entitytoorganizationfromsemrel/resultorganizationtest")
						.getPath(),
					"-hive_metastore_uris", "",
					"-leavesPath", workingDir.toString() + "/currentIteration/",
					"-resultOrgPath", workingDir.toString() + "/resultOrganization/",
					"-projectOrganizationPath", workingDir.toString() + "/projectOrganization/",
					"-childParentPath", workingDir.toString() + "/childParentOrg/",
					"-relationPath", workingDir.toString() + "/relation"

				});
		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<String> tmp = sc
			.textFile(workingDir.toString() + "/currentIteration/")
			.map(item -> OBJECT_MAPPER.readValue(item, String.class));

		Assertions.assertEquals(0, tmp.count());

	}

	@Test
	public void foundLeavesTest2() throws Exception {
		PrepareInfo
			.main(
				new String[] {
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-graphPath", getClass()
						.getResource(
							"/eu/dnetlib/dhp/entitytoorganizationfromsemrel/childparenttest1")
						.getPath(),
					"-hive_metastore_uris", "",
					"-leavesPath", workingDir.toString() + "/currentIteration/",
					"-resultOrgPath", workingDir.toString() + "/resultOrganization/",
					"-projectOrganizationPath", workingDir.toString() + "/projectOrganization/",
					"-childParentPath", workingDir.toString() + "/childParentOrg/",
					"-relationPath", workingDir.toString() + "/relation"

				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Leaves> tmp = sc
			.textFile(workingDir.toString() + "/currentIteration/")
			.map(item -> OBJECT_MAPPER.readValue(item, Leaves.class));

		Dataset<Leaves> verificationDs = spark.createDataset(tmp.rdd(), Encoders.bean(Leaves.class));

		Assertions.assertEquals(3, verificationDs.count());

		Assertions
			.assertEquals(
				1, verificationDs
					.filter("value = '20|doajarticles::396262ee936f3d3e26ff0e60bea6cae0'")
					.count());

		Assertions
			.assertEquals(
				1, verificationDs
					.filter("value = '20|opendoar____::a5fcb8eb25ebd6f7cd219e0fa1e6ddc1'")
					.count());

		Assertions
			.assertEquals(
				1, verificationDs
					.filter("value = '20|pippo_wf_001::2899e571609779168222fdeb59cb916d'")
					.count());

		verificationDs.foreach((ForeachFunction<Leaves>) l -> System.out.println(OBJECT_MAPPER.writeValueAsString(l)));

	}

}
