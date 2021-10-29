
package eu.dnetlib.dhp.resulttoorganizationfromsemrel;

import static eu.dnetlib.dhp.PropagationConstant.isSparkSessionManaged;
import static eu.dnetlib.dhp.PropagationConstant.readPath;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.KeyValueSet;
import eu.dnetlib.dhp.PropagationConstant;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Relation;

public class SparkJobTest {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SparkSession spark;

	private static Path workingDir;

	private static final Logger log = LoggerFactory.getLogger(SparkJobTest.class);

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files.createTempDirectory(StepActionsTest.class.getSimpleName());
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
	public void completeExecution() throws Exception {

		final String graphPath = getClass()
			.getResource("/eu/dnetlib/dhp/resulttoorganizationfromsemrel/execstep")
			.getPath();
		final String leavesPath = getClass()
			.getResource(
				"/eu/dnetlib/dhp/resulttoorganizationfromsemrel/execstep/currentIteration/")
			.getPath();
		final String childParentPath = getClass()
			.getResource(
				"/eu/dnetlib/dhp/resulttoorganizationfromsemrel/execstep/childParentOrg/")
			.getPath();

		final String resultOrgPath = getClass()
			.getResource(
				"/eu/dnetlib/dhp/resulttoorganizationfromsemrel/execstep/resultOrganization/")
			.getPath();

		readPath(spark, leavesPath, Leaves.class)
			.write()
			.option("compression", "gzip")
			.json(workingDir.toString() + "/leavesInput");

		readPath(spark, resultOrgPath, KeyValueSet.class)
			.write()
			.option("compression", "gzip")
			.json(workingDir.toString() + "/orgsInput");

		SparkResultToOrganizationFromSemRel

			.main(
				new String[] {
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-graphPath", graphPath,
					"-hive_metastore_uris", "",
					"-outputPath", workingDir.toString() + "/relation",
					"-leavesPath", workingDir.toString() + "/leavesInput",
					"-resultOrgPath", workingDir.toString() + "/orgsInput",
					"-childParentPath", childParentPath,
					"-workingDir", workingDir.toString()
				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Relation> tmp = sc
			.textFile(workingDir.toString() + "/relation")
			.map(item -> OBJECT_MAPPER.readValue(item, Relation.class));

		Assertions.assertEquals(18, tmp.count());
		tmp.foreach(r -> Assertions.assertEquals(ModelConstants.AFFILIATION, r.getSubRelType()));
		tmp.foreach(r -> Assertions.assertEquals(ModelConstants.RESULT_ORGANIZATION, r.getRelType()));
		tmp
			.foreach(
				r -> Assertions
					.assertEquals(
						PropagationConstant.PROPAGATION_DATA_INFO_TYPE, r.getDataInfo().getInferenceprovenance()));
		tmp
			.foreach(
				r -> Assertions
					.assertEquals(
						PropagationConstant.PROPAGATION_RELATION_RESULT_ORGANIZATION_SEM_REL_CLASS_ID,
						r.getDataInfo().getProvenanceaction().getClassid()));
		tmp
			.foreach(
				r -> Assertions
					.assertEquals(
						PropagationConstant.PROPAGATION_RELATION_RESULT_ORGANIZATION_SEM_REL_CLASS_NAME,
						r.getDataInfo().getProvenanceaction().getClassname()));
		tmp
			.foreach(
				r -> Assertions
					.assertEquals(
						"0.85",
						r.getDataInfo().getTrust()));

		Assertions.assertEquals(9, tmp.filter(r -> r.getSource().substring(0, 3).equals("50|")).count());
		tmp
			.filter(r -> r.getSource().substring(0, 3).equals("50|"))
			.foreach(r -> Assertions.assertEquals(ModelConstants.HAS_AUTHOR_INSTITUTION, r.getRelClass()));
		Assertions
			.assertEquals(
				2, tmp.filter(r -> r.getSource().equals("50|doajarticles::1cae0b82b56ccd97c2db1f698def7074")).count());
		Assertions
			.assertEquals(
				3, tmp.filter(r -> r.getSource().equals("50|dedup_wf_001::2899e571609779168222fdeb59cb916d")).count());
		Assertions
			.assertEquals(
				2, tmp.filter(r -> r.getSource().equals("50|doajarticles::2baa9032dc058d3c8ff780c426b0c19f")).count());
		Assertions
			.assertEquals(
				1, tmp.filter(r -> r.getSource().equals("50|openaire____::ec653e804967133b9436fdd30d3ff51d")).count());
		Assertions
			.assertEquals(
				1, tmp.filter(r -> r.getSource().equals("50|doajarticles::03748bcb5d754c951efec9700e18a56d")).count());

		Assertions.assertEquals(9, tmp.filter(r -> r.getSource().substring(0, 3).equals("20|")).count());
		tmp
			.filter(r -> r.getSource().substring(0, 3).equals("20|"))
			.foreach(r -> Assertions.assertEquals(ModelConstants.IS_AUTHOR_INSTITUTION_OF, r.getRelClass()));
		Assertions
			.assertEquals(
				1, tmp.filter(r -> r.getSource().equals("20|doajarticles::1cae0b82b56ccd97c2db1f698def7074")).count());
		Assertions
			.assertEquals(
				1, tmp.filter(r -> r.getSource().equals("20|dedup_wf_001::2899e571609779168222fdeb59cb916d")).count());
		Assertions
			.assertEquals(
				2, tmp.filter(r -> r.getSource().equals("20|doajarticles::2baa9032dc058d3c8ff780c426b0c19f")).count());
		Assertions
			.assertEquals(
				2, tmp.filter(r -> r.getSource().equals("20|openaire____::ec653e804967133b9436fdd30d3ff51d")).count());
		Assertions
			.assertEquals(
				3, tmp.filter(r -> r.getSource().equals("20|doajarticles::03748bcb5d754c951efec9700e18a56d")).count());

		Assertions
			.assertTrue(
				tmp
					.filter(r -> r.getSource().equals("50|doajarticles::1cae0b82b56ccd97c2db1f698def7074"))
					.map(r -> r.getTarget())
					.collect()
					.contains("20|doajarticles::1cae0b82b56ccd97c2db1f698def7074"));
		Assertions
			.assertTrue(
				tmp
					.filter(r -> r.getSource().equals("50|doajarticles::1cae0b82b56ccd97c2db1f698def7074"))
					.map(r -> r.getTarget())
					.collect()
					.contains("20|openaire____::ec653e804967133b9436fdd30d3ff51d"));

		Assertions
			.assertTrue(
				tmp
					.filter(r -> r.getSource().equals("50|doajarticles::2baa9032dc058d3c8ff780c426b0c19f"))
					.map(r -> r.getTarget())
					.collect()
					.contains("20|doajarticles::2baa9032dc058d3c8ff780c426b0c19f"));
		Assertions
			.assertTrue(
				tmp
					.filter(r -> r.getSource().equals("50|doajarticles::2baa9032dc058d3c8ff780c426b0c19f"))
					.map(r -> r.getTarget())
					.collect()
					.contains("20|doajarticles::03748bcb5d754c951efec9700e18a56d"));

		Assertions
			.assertTrue(
				tmp
					.filter(r -> r.getSource().equals("50|dedup_wf_001::2899e571609779168222fdeb59cb916d"))
					.map(r -> r.getTarget())
					.collect()
					.contains("20|dedup_wf_001::2899e571609779168222fdeb59cb916d"));
		Assertions
			.assertTrue(
				tmp
					.filter(r -> r.getSource().equals("50|dedup_wf_001::2899e571609779168222fdeb59cb916d"))
					.map(r -> r.getTarget())
					.collect()
					.contains("20|doajarticles::2baa9032dc058d3c8ff780c426b0c19f"));
		Assertions
			.assertTrue(
				tmp
					.filter(r -> r.getSource().equals("50|dedup_wf_001::2899e571609779168222fdeb59cb916d"))
					.map(r -> r.getTarget())
					.collect()
					.contains("20|doajarticles::03748bcb5d754c951efec9700e18a56d"));

		Assertions
			.assertTrue(
				tmp
					.filter(r -> r.getSource().equals("50|openaire____::ec653e804967133b9436fdd30d3ff51d"))
					.map(r -> r.getTarget())
					.collect()
					.contains("20|openaire____::ec653e804967133b9436fdd30d3ff51d"));

		Assertions
			.assertTrue(
				tmp
					.filter(r -> r.getSource().equals("50|doajarticles::03748bcb5d754c951efec9700e18a56d"))
					.map(r -> r.getTarget())
					.collect()
					.contains("20|doajarticles::03748bcb5d754c951efec9700e18a56d"));

		Assertions
			.assertTrue(
				tmp
					.filter(r -> r.getSource().equals("20|openaire____::ec653e804967133b9436fdd30d3ff51d"))
					.map(r -> r.getTarget())
					.collect()
					.contains("50|doajarticles::1cae0b82b56ccd97c2db1f698def7074"));
		Assertions
			.assertTrue(
				tmp
					.filter(r -> r.getSource().equals("20|openaire____::ec653e804967133b9436fdd30d3ff51d"))
					.map(r -> r.getTarget())
					.collect()
					.contains("50|openaire____::ec653e804967133b9436fdd30d3ff51d"));

		Assertions
			.assertTrue(
				tmp
					.filter(r -> r.getSource().equals("20|doajarticles::2baa9032dc058d3c8ff780c426b0c19f"))
					.map(r -> r.getTarget())
					.collect()
					.contains("50|dedup_wf_001::2899e571609779168222fdeb59cb916d"));
		Assertions
			.assertTrue(
				tmp
					.filter(r -> r.getSource().equals("20|doajarticles::2baa9032dc058d3c8ff780c426b0c19f"))
					.map(r -> r.getTarget())
					.collect()
					.contains("50|doajarticles::2baa9032dc058d3c8ff780c426b0c19f"));

		Assertions
			.assertTrue(
				tmp
					.filter(r -> r.getSource().equals("20|doajarticles::03748bcb5d754c951efec9700e18a56d"))
					.map(r -> r.getTarget())
					.collect()
					.contains("50|dedup_wf_001::2899e571609779168222fdeb59cb916d"));
		Assertions
			.assertTrue(
				tmp
					.filter(r -> r.getSource().equals("20|doajarticles::03748bcb5d754c951efec9700e18a56d"))
					.map(r -> r.getTarget())
					.collect()
					.contains("50|doajarticles::2baa9032dc058d3c8ff780c426b0c19f"));
		Assertions
			.assertTrue(
				tmp
					.filter(r -> r.getSource().equals("20|doajarticles::03748bcb5d754c951efec9700e18a56d"))
					.map(r -> r.getTarget())
					.collect()
					.contains("50|doajarticles::03748bcb5d754c951efec9700e18a56d"));

		Assertions
			.assertTrue(
				tmp
					.filter(r -> r.getSource().equals("20|dedup_wf_001::2899e571609779168222fdeb59cb916d"))
					.map(r -> r.getTarget())
					.collect()
					.contains("50|dedup_wf_001::2899e571609779168222fdeb59cb916d"));

		Assertions
			.assertTrue(
				tmp
					.filter(r -> r.getSource().equals("20|doajarticles::1cae0b82b56ccd97c2db1f698def7074"))
					.map(r -> r.getTarget())
					.collect()
					.contains("50|doajarticles::1cae0b82b56ccd97c2db1f698def7074"));
	}

}
