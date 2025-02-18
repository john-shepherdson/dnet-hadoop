
package eu.dnetlib.dhp.entitytoorganizationfromsemrel;

import static eu.dnetlib.dhp.PropagationConstant.readPath;
import static eu.dnetlib.dhp.common.enrichment.Constants.PROPAGATION_DATA_INFO_TYPE;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
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

	@AfterEach
	public void afterEach() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	@Test
	public void completeResultExecution() throws Exception {

		final String graphPath = getClass()
			.getResource("/eu/dnetlib/dhp/entitytoorganizationfromsemrel/execstep/graph")
			.getPath();
		final String leavesPath = getClass()
			.getResource(
				"/eu/dnetlib/dhp/entitytoorganizationfromsemrel/execstep/currentIteration/")
			.getPath();
		final String childParentPath = getClass()
			.getResource(
				"/eu/dnetlib/dhp/entitytoorganizationfromsemrel/execstep/childParentOrg/")
			.getPath();

		final String resultOrgPath = getClass()
			.getResource(
				"/eu/dnetlib/dhp/entitytoorganizationfromsemrel/execstep/resultOrganization/")
			.getPath();

		final String projectOrgPath = getClass()
			.getResource(
				"/eu/dnetlib/dhp/entitytoorganizationfromsemrel/execstep/projectOrganization/")
			.getPath();

		readPath(spark, leavesPath, Leaves.class)
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(workingDir.toString() + "/leavesInput");

		readPath(spark, resultOrgPath, KeyValueSet.class)
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(workingDir.toString() + "/orgsInput");

		readPath(spark, projectOrgPath, KeyValueSet.class)
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(workingDir.toString() + "/projectInput");

		SparkEntityToOrganizationFromSemRel

			.main(
				new String[] {
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-relationPath", graphPath,
					"-hive_metastore_uris", "",
					"-outputPath", workingDir.toString() + "/finalrelation",
					"-leavesPath", workingDir.toString() + "/leavesInput",
					"-resultOrgPath", workingDir.toString() + "/orgsInput",
					"-projectOrganizationPath", workingDir.toString() + "/projectInput",
					"-childParentPath", childParentPath,
					"-workingDir", workingDir.toString()
				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Relation> temp = sc
			.textFile(workingDir.toString() + "/finalrelation")
			.map(item -> OBJECT_MAPPER.readValue(item, Relation.class));

		Assertions.assertEquals(36, temp.count());

		JavaRDD<Relation> result = temp.filter(r -> r.getSource().startsWith("50|") || r.getTarget().startsWith("50|"));
		Assertions.assertEquals(18, result.count());
		result.foreach(r -> Assertions.assertEquals(ModelConstants.AFFILIATION, r.getSubRelType()));
		result.foreach(r -> Assertions.assertEquals(ModelConstants.RESULT_ORGANIZATION, r.getRelType()));
		result
			.foreach(
				r -> Assertions
					.assertEquals(
						PROPAGATION_DATA_INFO_TYPE, r.getDataInfo().getInferenceprovenance()));
		result
			.foreach(
				r -> Assertions
					.assertEquals(
						PropagationConstant.PROPAGATION_RELATION_RESULT_ORGANIZATION_SEM_REL_CLASS_ID,
						r.getDataInfo().getProvenanceaction().getClassid()));
		result
			.foreach(
				r -> Assertions
					.assertEquals(
						PropagationConstant.PROPAGATION_RELATION_RESULT_ORGANIZATION_SEM_REL_CLASS_NAME,
						r.getDataInfo().getProvenanceaction().getClassname()));
		result
			.foreach(
				r -> Assertions
					.assertEquals(
						"0.85",
						r.getDataInfo().getTrust()));

		Assertions.assertEquals(9, result.filter(r -> r.getSource().substring(0, 3).equals("50|")).count());
		result
			.filter(r -> r.getSource().substring(0, 3).equals("50|"))
			.foreach(r -> Assertions.assertEquals(ModelConstants.HAS_AUTHOR_INSTITUTION, r.getRelClass()));
		Assertions
			.assertEquals(
				2,
				result.filter(r -> r.getSource().equals("50|doajarticles::1cae0b82b56ccd97c2db1f698def7074")).count());
		Assertions
			.assertEquals(
				3,
				result.filter(r -> r.getSource().equals("50|dedup_wf_001::2899e571609779168222fdeb59cb916d")).count());
		Assertions
			.assertEquals(
				2,
				result.filter(r -> r.getSource().equals("50|doajarticles::2baa9032dc058d3c8ff780c426b0c19f")).count());
		Assertions
			.assertEquals(
				1,
				result.filter(r -> r.getSource().equals("50|openaire____::ec653e804967133b9436fdd30d3ff51d")).count());
		Assertions
			.assertEquals(
				1,
				result.filter(r -> r.getSource().equals("50|doajarticles::03748bcb5d754c951efec9700e18a56d")).count());

		Assertions.assertEquals(9, result.filter(r -> r.getSource().substring(0, 3).equals("20|")).count());
		result
			.filter(r -> r.getSource().substring(0, 3).equals("20|"))
			.foreach(r -> Assertions.assertEquals(ModelConstants.IS_AUTHOR_INSTITUTION_OF, r.getRelClass()));
		Assertions
			.assertEquals(
				1,
				result.filter(r -> r.getSource().equals("20|doajarticles::1cae0b82b56ccd97c2db1f698def7074")).count());
		Assertions
			.assertEquals(
				1,
				result.filter(r -> r.getSource().equals("20|dedup_wf_001::2899e571609779168222fdeb59cb916d")).count());
		Assertions
			.assertEquals(
				2,
				result.filter(r -> r.getSource().equals("20|doajarticles::2baa9032dc058d3c8ff780c426b0c19f")).count());
		Assertions
			.assertEquals(
				2,
				result.filter(r -> r.getSource().equals("20|openaire____::ec653e804967133b9436fdd30d3ff51d")).count());
		Assertions
			.assertEquals(
				3,
				result.filter(r -> r.getSource().equals("20|doajarticles::03748bcb5d754c951efec9700e18a56d")).count());

		Assertions
			.assertTrue(
				result
					.filter(r -> r.getSource().equals("50|doajarticles::1cae0b82b56ccd97c2db1f698def7074"))
					.map(r -> r.getTarget())
					.collect()
					.contains("20|doajarticles::1cae0b82b56ccd97c2db1f698def7074"));
		Assertions
			.assertTrue(
				result
					.filter(r -> r.getSource().equals("50|doajarticles::1cae0b82b56ccd97c2db1f698def7074"))
					.map(r -> r.getTarget())
					.collect()
					.contains("20|openaire____::ec653e804967133b9436fdd30d3ff51d"));

		Assertions
			.assertTrue(
				result
					.filter(r -> r.getSource().equals("50|doajarticles::2baa9032dc058d3c8ff780c426b0c19f"))
					.map(r -> r.getTarget())
					.collect()
					.contains("20|doajarticles::2baa9032dc058d3c8ff780c426b0c19f"));
		Assertions
			.assertTrue(
				result
					.filter(r -> r.getSource().equals("50|doajarticles::2baa9032dc058d3c8ff780c426b0c19f"))
					.map(r -> r.getTarget())
					.collect()
					.contains("20|doajarticles::03748bcb5d754c951efec9700e18a56d"));

		Assertions
			.assertTrue(
				result
					.filter(r -> r.getSource().equals("50|dedup_wf_001::2899e571609779168222fdeb59cb916d"))
					.map(r -> r.getTarget())
					.collect()
					.contains("20|dedup_wf_001::2899e571609779168222fdeb59cb916d"));
		Assertions
			.assertTrue(
				result
					.filter(r -> r.getSource().equals("50|dedup_wf_001::2899e571609779168222fdeb59cb916d"))
					.map(r -> r.getTarget())
					.collect()
					.contains("20|doajarticles::2baa9032dc058d3c8ff780c426b0c19f"));
		Assertions
			.assertTrue(
				result
					.filter(r -> r.getSource().equals("50|dedup_wf_001::2899e571609779168222fdeb59cb916d"))
					.map(r -> r.getTarget())
					.collect()
					.contains("20|doajarticles::03748bcb5d754c951efec9700e18a56d"));

		Assertions
			.assertTrue(
				result
					.filter(r -> r.getSource().equals("50|openaire____::ec653e804967133b9436fdd30d3ff51d"))
					.map(r -> r.getTarget())
					.collect()
					.contains("20|openaire____::ec653e804967133b9436fdd30d3ff51d"));

		Assertions
			.assertTrue(
				result
					.filter(r -> r.getSource().equals("50|doajarticles::03748bcb5d754c951efec9700e18a56d"))
					.map(r -> r.getTarget())
					.collect()
					.contains("20|doajarticles::03748bcb5d754c951efec9700e18a56d"));

		Assertions
			.assertTrue(
				result
					.filter(r -> r.getSource().equals("20|openaire____::ec653e804967133b9436fdd30d3ff51d"))
					.map(r -> r.getTarget())
					.collect()
					.contains("50|doajarticles::1cae0b82b56ccd97c2db1f698def7074"));
		Assertions
			.assertTrue(
				result
					.filter(r -> r.getSource().equals("20|openaire____::ec653e804967133b9436fdd30d3ff51d"))
					.map(r -> r.getTarget())
					.collect()
					.contains("50|openaire____::ec653e804967133b9436fdd30d3ff51d"));

		Assertions
			.assertTrue(
				result
					.filter(r -> r.getSource().equals("20|doajarticles::2baa9032dc058d3c8ff780c426b0c19f"))
					.map(r -> r.getTarget())
					.collect()
					.contains("50|dedup_wf_001::2899e571609779168222fdeb59cb916d"));
		Assertions
			.assertTrue(
				result
					.filter(r -> r.getSource().equals("20|doajarticles::2baa9032dc058d3c8ff780c426b0c19f"))
					.map(r -> r.getTarget())
					.collect()
					.contains("50|doajarticles::2baa9032dc058d3c8ff780c426b0c19f"));

		Assertions
			.assertTrue(
				result
					.filter(r -> r.getSource().equals("20|doajarticles::03748bcb5d754c951efec9700e18a56d"))
					.map(r -> r.getTarget())
					.collect()
					.contains("50|dedup_wf_001::2899e571609779168222fdeb59cb916d"));
		Assertions
			.assertTrue(
				result
					.filter(r -> r.getSource().equals("20|doajarticles::03748bcb5d754c951efec9700e18a56d"))
					.map(r -> r.getTarget())
					.collect()
					.contains("50|doajarticles::2baa9032dc058d3c8ff780c426b0c19f"));
		Assertions
			.assertTrue(
				result
					.filter(r -> r.getSource().equals("20|doajarticles::03748bcb5d754c951efec9700e18a56d"))
					.map(r -> r.getTarget())
					.collect()
					.contains("50|doajarticles::03748bcb5d754c951efec9700e18a56d"));

		Assertions
			.assertTrue(
				result
					.filter(r -> r.getSource().equals("20|dedup_wf_001::2899e571609779168222fdeb59cb916d"))
					.map(r -> r.getTarget())
					.collect()
					.contains("50|dedup_wf_001::2899e571609779168222fdeb59cb916d"));

		Assertions
			.assertTrue(
				result
					.filter(r -> r.getSource().equals("20|doajarticles::1cae0b82b56ccd97c2db1f698def7074"))
					.map(r -> r.getTarget())
					.collect()
					.contains("50|doajarticles::1cae0b82b56ccd97c2db1f698def7074"));
	}

	@Test
	public void completeProjectExecution() throws Exception {

		final String graphPath = getClass()
			.getResource("/eu/dnetlib/dhp/entitytoorganizationfromsemrel/execstep/graph")
			.getPath();
		final String leavesPath = getClass()
			.getResource(
				"/eu/dnetlib/dhp/entitytoorganizationfromsemrel/execstep/currentIteration/")
			.getPath();
		final String childParentPath = getClass()
			.getResource(
				"/eu/dnetlib/dhp/entitytoorganizationfromsemrel/execstep/childParentOrg/")
			.getPath();

		final String resultOrgPath = getClass()
			.getResource(
				"/eu/dnetlib/dhp/entitytoorganizationfromsemrel/execstep/resultOrganization/")
			.getPath();

		final String projectOrgPath = getClass()
			.getResource(
				"/eu/dnetlib/dhp/entitytoorganizationfromsemrel/execstep/projectOrganization/")
			.getPath();

		readPath(spark, leavesPath, Leaves.class)
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(workingDir.toString() + "/leavesInput");

		readPath(spark, resultOrgPath, KeyValueSet.class)
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(workingDir.toString() + "/orgsInput");

		readPath(spark, projectOrgPath, KeyValueSet.class)
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(workingDir.toString() + "/projectInput");

		SparkEntityToOrganizationFromSemRel

			.main(
				new String[] {
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-relationPath", graphPath,
					"-hive_metastore_uris", "",
					"-outputPath", workingDir.toString() + "/finalrelation",
					"-leavesPath", workingDir.toString() + "/leavesInput",
					"-resultOrgPath", workingDir.toString() + "/orgsInput",
					"-projectOrganizationPath", workingDir.toString() + "/projectInput",
					"-childParentPath", childParentPath,
					"-workingDir", workingDir.toString()
				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Relation> temp = sc
			.textFile(workingDir.toString() + "/finalrelation")
			.map(item -> OBJECT_MAPPER.readValue(item, Relation.class));

		Assertions.assertEquals(36, temp.count());

		JavaRDD<Relation> project = temp
			.filter(r -> r.getSource().startsWith("40|") || r.getTarget().startsWith("40|"));
		Assertions.assertEquals(18, project.count());

		project.foreach(r -> Assertions.assertEquals(ModelConstants.PARTICIPATION, r.getSubRelType()));
		project.foreach(r -> Assertions.assertEquals(ModelConstants.PROJECT_ORGANIZATION, r.getRelType()));
		project
			.foreach(
				r -> Assertions
					.assertEquals(
						PROPAGATION_DATA_INFO_TYPE, r.getDataInfo().getInferenceprovenance()));
		project
			.foreach(
				r -> Assertions
					.assertEquals(
						PropagationConstant.PROPAGATION_RELATION_PROJECT_ORGANIZATION_SEM_REL_CLASS_ID,
						r.getDataInfo().getProvenanceaction().getClassid()));
		project
			.foreach(
				r -> Assertions
					.assertEquals(
						PropagationConstant.PROPAGATION_RELATION_PROJECT_ORGANIZATION_SEM_REL_CLASS_NAME,
						r.getDataInfo().getProvenanceaction().getClassname()));
		project
			.foreach(
				r -> Assertions
					.assertEquals(
						"0.85",
						r.getDataInfo().getTrust()));

		Assertions.assertEquals(9, project.filter(r -> r.getSource().substring(0, 3).equals("40|")).count());
		project
			.filter(r -> r.getSource().substring(0, 3).equals("40|"))
			.foreach(r -> Assertions.assertEquals(ModelConstants.HAS_PARTICIPANT, r.getRelClass()));
		Assertions
			.assertEquals(
				2,
				project.filter(r -> r.getSource().equals("40|doajarticles::1cae0b82b56ccd97c2db1f698def7074")).count());
		Assertions
			.assertEquals(
				3,
				project.filter(r -> r.getSource().equals("40|dedup_wf_001::2899e571609779168222fdeb59cb916d")).count());
		Assertions
			.assertEquals(
				2,
				project.filter(r -> r.getSource().equals("40|doajarticles::2baa9032dc058d3c8ff780c426b0c19f")).count());
		Assertions
			.assertEquals(
				1,
				project.filter(r -> r.getSource().equals("40|openaire____::ec653e804967133b9436fdd30d3ff51d")).count());
		Assertions
			.assertEquals(
				1,
				project.filter(r -> r.getSource().equals("40|doajarticles::03748bcb5d754c951efec9700e18a56d")).count());

		Assertions.assertEquals(9, project.filter(r -> r.getSource().substring(0, 3).equals("20|")).count());
		project
			.filter(r -> r.getSource().substring(0, 3).equals("20|"))
			.foreach(r -> Assertions.assertEquals(ModelConstants.IS_PARTICIPANT, r.getRelClass()));
		Assertions
			.assertEquals(
				1,
				project.filter(r -> r.getSource().equals("20|doajarticles::1cae0b82b56ccd97c2db1f698def7074")).count());
		Assertions
			.assertEquals(
				1,
				project.filter(r -> r.getSource().equals("20|dedup_wf_001::2899e571609779168222fdeb59cb916d")).count());
		Assertions
			.assertEquals(
				2,
				project.filter(r -> r.getSource().equals("20|doajarticles::2baa9032dc058d3c8ff780c426b0c19f")).count());
		Assertions
			.assertEquals(
				2,
				project.filter(r -> r.getSource().equals("20|openaire____::ec653e804967133b9436fdd30d3ff51d")).count());
		Assertions
			.assertEquals(
				3,
				project.filter(r -> r.getSource().equals("20|doajarticles::03748bcb5d754c951efec9700e18a56d")).count());

		Assertions
			.assertTrue(
				project
					.filter(r -> r.getSource().equals("40|doajarticles::1cae0b82b56ccd97c2db1f698def7074"))
					.map(r -> r.getTarget())
					.collect()
					.contains("20|doajarticles::1cae0b82b56ccd97c2db1f698def7074"));
		Assertions
			.assertTrue(
				project
					.filter(r -> r.getSource().equals("40|doajarticles::1cae0b82b56ccd97c2db1f698def7074"))
					.map(r -> r.getTarget())
					.collect()
					.contains("20|openaire____::ec653e804967133b9436fdd30d3ff51d"));

		Assertions
			.assertTrue(
				project
					.filter(r -> r.getSource().equals("40|doajarticles::2baa9032dc058d3c8ff780c426b0c19f"))
					.map(r -> r.getTarget())
					.collect()
					.contains("20|doajarticles::2baa9032dc058d3c8ff780c426b0c19f"));
		Assertions
			.assertTrue(
				project
					.filter(r -> r.getSource().equals("40|doajarticles::2baa9032dc058d3c8ff780c426b0c19f"))
					.map(r -> r.getTarget())
					.collect()
					.contains("20|doajarticles::03748bcb5d754c951efec9700e18a56d"));

		Assertions
			.assertTrue(
				project
					.filter(r -> r.getSource().equals("40|dedup_wf_001::2899e571609779168222fdeb59cb916d"))
					.map(r -> r.getTarget())
					.collect()
					.contains("20|dedup_wf_001::2899e571609779168222fdeb59cb916d"));
		Assertions
			.assertTrue(
				project
					.filter(r -> r.getSource().equals("40|dedup_wf_001::2899e571609779168222fdeb59cb916d"))
					.map(r -> r.getTarget())
					.collect()
					.contains("20|doajarticles::2baa9032dc058d3c8ff780c426b0c19f"));
		Assertions
			.assertTrue(
				project
					.filter(r -> r.getSource().equals("40|dedup_wf_001::2899e571609779168222fdeb59cb916d"))
					.map(r -> r.getTarget())
					.collect()
					.contains("20|doajarticles::03748bcb5d754c951efec9700e18a56d"));

		Assertions
			.assertTrue(
				project
					.filter(r -> r.getSource().equals("40|openaire____::ec653e804967133b9436fdd30d3ff51d"))
					.map(r -> r.getTarget())
					.collect()
					.contains("20|openaire____::ec653e804967133b9436fdd30d3ff51d"));

		Assertions
			.assertTrue(
				project
					.filter(r -> r.getSource().equals("40|doajarticles::03748bcb5d754c951efec9700e18a56d"))
					.map(r -> r.getTarget())
					.collect()
					.contains("20|doajarticles::03748bcb5d754c951efec9700e18a56d"));

		Assertions
			.assertTrue(
				project
					.filter(r -> r.getSource().equals("20|openaire____::ec653e804967133b9436fdd30d3ff51d"))
					.map(r -> r.getTarget())
					.collect()
					.contains("40|doajarticles::1cae0b82b56ccd97c2db1f698def7074"));
		Assertions
			.assertTrue(
				project
					.filter(r -> r.getSource().equals("20|openaire____::ec653e804967133b9436fdd30d3ff51d"))
					.map(r -> r.getTarget())
					.collect()
					.contains("40|openaire____::ec653e804967133b9436fdd30d3ff51d"));

		Assertions
			.assertTrue(
				project
					.filter(r -> r.getSource().equals("20|doajarticles::2baa9032dc058d3c8ff780c426b0c19f"))
					.map(r -> r.getTarget())
					.collect()
					.contains("40|dedup_wf_001::2899e571609779168222fdeb59cb916d"));
		Assertions
			.assertTrue(
				project
					.filter(r -> r.getSource().equals("20|doajarticles::2baa9032dc058d3c8ff780c426b0c19f"))
					.map(r -> r.getTarget())
					.collect()
					.contains("40|doajarticles::2baa9032dc058d3c8ff780c426b0c19f"));

		Assertions
			.assertTrue(
				project
					.filter(r -> r.getSource().equals("20|doajarticles::03748bcb5d754c951efec9700e18a56d"))
					.map(r -> r.getTarget())
					.collect()
					.contains("40|dedup_wf_001::2899e571609779168222fdeb59cb916d"));
		Assertions
			.assertTrue(
				project
					.filter(r -> r.getSource().equals("20|doajarticles::03748bcb5d754c951efec9700e18a56d"))
					.map(r -> r.getTarget())
					.collect()
					.contains("40|doajarticles::2baa9032dc058d3c8ff780c426b0c19f"));
		Assertions
			.assertTrue(
				project
					.filter(r -> r.getSource().equals("20|doajarticles::03748bcb5d754c951efec9700e18a56d"))
					.map(r -> r.getTarget())
					.collect()
					.contains("40|doajarticles::03748bcb5d754c951efec9700e18a56d"));

		Assertions
			.assertTrue(
				project
					.filter(r -> r.getSource().equals("20|dedup_wf_001::2899e571609779168222fdeb59cb916d"))
					.map(r -> r.getTarget())
					.collect()
					.contains("40|dedup_wf_001::2899e571609779168222fdeb59cb916d"));

		Assertions
			.assertTrue(
				project
					.filter(r -> r.getSource().equals("20|doajarticles::1cae0b82b56ccd97c2db1f698def7074"))
					.map(r -> r.getTarget())
					.collect()
					.contains("40|doajarticles::1cae0b82b56ccd97c2db1f698def7074"));
	}

	@Test
	public void singleIterationExecution() throws Exception {

		final String graphPath = getClass()
			.getResource("/eu/dnetlib/dhp/entitytoorganizationfromsemrel/execstep/graph")
			.getPath();
		final String leavesPath = getClass()
			.getResource(
				"/eu/dnetlib/dhp/entitytoorganizationfromsemrel/execstep/currentIteration/")
			.getPath();
		final String childParentPath = getClass()
			.getResource(
				"/eu/dnetlib/dhp/entitytoorganizationfromsemrel/execstep/childParentOrg/")
			.getPath();

		final String resultOrgPath = getClass()
			.getResource(
				"/eu/dnetlib/dhp/entitytoorganizationfromsemrel/execstep/resultOrganization/")
			.getPath();

		final String projectOrgPath = getClass()
			.getResource(
				"/eu/dnetlib/dhp/entitytoorganizationfromsemrel/execstep/projectOrganization/")
			.getPath();

		readPath(spark, leavesPath, Leaves.class)
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(workingDir.toString() + "/leavesInput");

		readPath(spark, resultOrgPath, KeyValueSet.class)
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(workingDir.toString() + "/orgsInput");

		readPath(spark, projectOrgPath, KeyValueSet.class)
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(workingDir.toString() + "/projectInput");

		SparkEntityToOrganizationFromSemRel

			.main(
				new String[] {
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-relationPath", graphPath,
					"-hive_metastore_uris", "",
					"-outputPath", workingDir.toString() + "/finalrelation",
					"-leavesPath", workingDir.toString() + "/leavesInput",
					"-resultOrgPath", workingDir.toString() + "/orgsInput",
					"-projectOrganizationPath", workingDir.toString() + "/projectInput",
					"-childParentPath", childParentPath,
					"-workingDir", workingDir.toString(),
					"-iterations", "1"
				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Relation> temp = sc
			.textFile(workingDir.toString() + "/finalrelation")
			.map(item -> OBJECT_MAPPER.readValue(item, Relation.class));

		Assertions.assertEquals(16, temp.count());

		Assertions.assertEquals(4, temp.filter(r -> r.getSource().startsWith("50|")).count());
		Assertions.assertEquals(4, temp.filter(r -> r.getTarget().startsWith("50|")).count());
		Assertions.assertEquals(4, temp.filter(r -> r.getSource().startsWith("40|")).count());
		Assertions.assertEquals(4, temp.filter(r -> r.getTarget().startsWith("40|")).count());
		Assertions.assertEquals(8, temp.filter(r -> r.getSource().startsWith("20|")).count());
		Assertions.assertEquals(8, temp.filter(r -> r.getSource().startsWith("20|")).count());

//		JavaRDD<Relation> result = temp.filter(r -> r.getSource().startsWith("50|") || r.getTarget().startsWith("50|"));
//		Assertions.assertEquals(18, result.count());
//		result.foreach(r -> Assertions.assertEquals(ModelConstants.AFFILIATION, r.getSubRelType()));
//		result.foreach(r -> Assertions.assertEquals(ModelConstants.RESULT_ORGANIZATION, r.getRelType()));
//		result
//				.foreach(
//						r -> Assertions
//								.assertEquals(
//										PropagationConstant.PROPAGATION_DATA_INFO_TYPE, r.getDataInfo().getInferenceprovenance()));
//		result
//				.foreach(
//						r -> Assertions
//								.assertEquals(
//										PropagationConstant.PROPAGATION_RELATION_RESULT_ORGANIZATION_SEM_REL_CLASS_ID,
//										r.getDataInfo().getProvenanceaction().getClassid()));
//		result
//				.foreach(
//						r -> Assertions
//								.assertEquals(
//										PropagationConstant.PROPAGATION_RELATION_RESULT_ORGANIZATION_SEM_REL_CLASS_NAME,
//										r.getDataInfo().getProvenanceaction().getClassname()));
//		result
//				.foreach(
//						r -> Assertions
//								.assertEquals(
//										"0.85",
//										r.getDataInfo().getTrust()));
//
//		Assertions.assertEquals(9, result.filter(r -> r.getSource().substring(0, 3).equals("50|")).count());
//		result
//				.filter(r -> r.getSource().substring(0, 3).equals("50|"))
//				.foreach(r -> Assertions.assertEquals(ModelConstants.HAS_AUTHOR_INSTITUTION, r.getRelClass()));
//		Assertions
//				.assertEquals(
//						2, result.filter(r -> r.getSource().equals("50|doajarticles::1cae0b82b56ccd97c2db1f698def7074")).count());
//		Assertions
//				.assertEquals(
//						3, result.filter(r -> r.getSource().equals("50|dedup_wf_001::2899e571609779168222fdeb59cb916d")).count());
//		Assertions
//				.assertEquals(
//						2, result.filter(r -> r.getSource().equals("50|doajarticles::2baa9032dc058d3c8ff780c426b0c19f")).count());
//		Assertions
//				.assertEquals(
//						1, result.filter(r -> r.getSource().equals("50|openaire____::ec653e804967133b9436fdd30d3ff51d")).count());
//		Assertions
//				.assertEquals(
//						1, result.filter(r -> r.getSource().equals("50|doajarticles::03748bcb5d754c951efec9700e18a56d")).count());
//
//		Assertions.assertEquals(9, result.filter(r -> r.getSource().substring(0, 3).equals("20|")).count());
//		result
//				.filter(r -> r.getSource().substring(0, 3).equals("20|"))
//				.foreach(r -> Assertions.assertEquals(ModelConstants.IS_AUTHOR_INSTITUTION_OF, r.getRelClass()));
//		Assertions
//				.assertEquals(
//						1, result.filter(r -> r.getSource().equals("20|doajarticles::1cae0b82b56ccd97c2db1f698def7074")).count());
//		Assertions
//				.assertEquals(
//						1, result.filter(r -> r.getSource().equals("20|dedup_wf_001::2899e571609779168222fdeb59cb916d")).count());
//		Assertions
//				.assertEquals(
//						2, result.filter(r -> r.getSource().equals("20|doajarticles::2baa9032dc058d3c8ff780c426b0c19f")).count());
//		Assertions
//				.assertEquals(
//						2, result.filter(r -> r.getSource().equals("20|openaire____::ec653e804967133b9436fdd30d3ff51d")).count());
//		Assertions
//				.assertEquals(
//						3, result.filter(r -> r.getSource().equals("20|doajarticles::03748bcb5d754c951efec9700e18a56d")).count());
//
//		Assertions
//				.assertTrue(
//						result
//								.filter(r -> r.getSource().equals("50|doajarticles::1cae0b82b56ccd97c2db1f698def7074"))
//								.map(r -> r.getTarget())
//								.collect()
//								.contains("20|doajarticles::1cae0b82b56ccd97c2db1f698def7074"));
//		Assertions
//				.assertTrue(
//						result
//								.filter(r -> r.getSource().equals("50|doajarticles::1cae0b82b56ccd97c2db1f698def7074"))
//								.map(r -> r.getTarget())
//								.collect()
//								.contains("20|openaire____::ec653e804967133b9436fdd30d3ff51d"));
//
//		Assertions
//				.assertTrue(
//						result
//								.filter(r -> r.getSource().equals("50|doajarticles::2baa9032dc058d3c8ff780c426b0c19f"))
//								.map(r -> r.getTarget())
//								.collect()
//								.contains("20|doajarticles::2baa9032dc058d3c8ff780c426b0c19f"));
//		Assertions
//				.assertTrue(
//						result
//								.filter(r -> r.getSource().equals("50|doajarticles::2baa9032dc058d3c8ff780c426b0c19f"))
//								.map(r -> r.getTarget())
//								.collect()
//								.contains("20|doajarticles::03748bcb5d754c951efec9700e18a56d"));
//
//		Assertions
//				.assertTrue(
//						result
//								.filter(r -> r.getSource().equals("50|dedup_wf_001::2899e571609779168222fdeb59cb916d"))
//								.map(r -> r.getTarget())
//								.collect()
//								.contains("20|dedup_wf_001::2899e571609779168222fdeb59cb916d"));
//		Assertions
//				.assertTrue(
//						result
//								.filter(r -> r.getSource().equals("50|dedup_wf_001::2899e571609779168222fdeb59cb916d"))
//								.map(r -> r.getTarget())
//								.collect()
//								.contains("20|doajarticles::2baa9032dc058d3c8ff780c426b0c19f"));
//		Assertions
//				.assertTrue(
//						result
//								.filter(r -> r.getSource().equals("50|dedup_wf_001::2899e571609779168222fdeb59cb916d"))
//								.map(r -> r.getTarget())
//								.collect()
//								.contains("20|doajarticles::03748bcb5d754c951efec9700e18a56d"));
//
//		Assertions
//				.assertTrue(
//						result
//								.filter(r -> r.getSource().equals("50|openaire____::ec653e804967133b9436fdd30d3ff51d"))
//								.map(r -> r.getTarget())
//								.collect()
//								.contains("20|openaire____::ec653e804967133b9436fdd30d3ff51d"));
//
//		Assertions
//				.assertTrue(
//						result
//								.filter(r -> r.getSource().equals("50|doajarticles::03748bcb5d754c951efec9700e18a56d"))
//								.map(r -> r.getTarget())
//								.collect()
//								.contains("20|doajarticles::03748bcb5d754c951efec9700e18a56d"));
//
//		Assertions
//				.assertTrue(
//						result
//								.filter(r -> r.getSource().equals("20|openaire____::ec653e804967133b9436fdd30d3ff51d"))
//								.map(r -> r.getTarget())
//								.collect()
//								.contains("50|doajarticles::1cae0b82b56ccd97c2db1f698def7074"));
//		Assertions
//				.assertTrue(
//						result
//								.filter(r -> r.getSource().equals("20|openaire____::ec653e804967133b9436fdd30d3ff51d"))
//								.map(r -> r.getTarget())
//								.collect()
//								.contains("50|openaire____::ec653e804967133b9436fdd30d3ff51d"));
//
//		Assertions
//				.assertTrue(
//						result
//								.filter(r -> r.getSource().equals("20|doajarticles::2baa9032dc058d3c8ff780c426b0c19f"))
//								.map(r -> r.getTarget())
//								.collect()
//								.contains("50|dedup_wf_001::2899e571609779168222fdeb59cb916d"));
//		Assertions
//				.assertTrue(
//						result
//								.filter(r -> r.getSource().equals("20|doajarticles::2baa9032dc058d3c8ff780c426b0c19f"))
//								.map(r -> r.getTarget())
//								.collect()
//								.contains("50|doajarticles::2baa9032dc058d3c8ff780c426b0c19f"));
//
//		Assertions
//				.assertTrue(
//						result
//								.filter(r -> r.getSource().equals("20|doajarticles::03748bcb5d754c951efec9700e18a56d"))
//								.map(r -> r.getTarget())
//								.collect()
//								.contains("50|dedup_wf_001::2899e571609779168222fdeb59cb916d"));
//		Assertions
//				.assertTrue(
//						result
//								.filter(r -> r.getSource().equals("20|doajarticles::03748bcb5d754c951efec9700e18a56d"))
//								.map(r -> r.getTarget())
//								.collect()
//								.contains("50|doajarticles::2baa9032dc058d3c8ff780c426b0c19f"));
//		Assertions
//				.assertTrue(
//						result
//								.filter(r -> r.getSource().equals("20|doajarticles::03748bcb5d754c951efec9700e18a56d"))
//								.map(r -> r.getTarget())
//								.collect()
//								.contains("50|doajarticles::03748bcb5d754c951efec9700e18a56d"));
//
//		Assertions
//				.assertTrue(
//						result
//								.filter(r -> r.getSource().equals("20|dedup_wf_001::2899e571609779168222fdeb59cb916d"))
//								.map(r -> r.getTarget())
//								.collect()
//								.contains("50|dedup_wf_001::2899e571609779168222fdeb59cb916d"));
//
//		Assertions
//				.assertTrue(
//						result
//								.filter(r -> r.getSource().equals("20|doajarticles::1cae0b82b56ccd97c2db1f698def7074"))
//								.map(r -> r.getTarget())
//								.collect()
//								.contains("50|doajarticles::1cae0b82b56ccd97c2db1f698def7074"));
	}
}
