
package eu.dnetlib.dhp.entitytoorganizationfromsemrel;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
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
import eu.dnetlib.dhp.PropagationConstant;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Relation;

public class StepActionsTest {

	private static final Logger log = LoggerFactory.getLogger(StepActionsTest.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SparkSession spark;

	private static Path workingDir;

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
	public void execStepTest() {

		StepActions
			.execStep(
				spark, getClass()
					.getResource(
						"/eu/dnetlib/dhp/entitytoorganizationfromsemrel/execstep/graph/result")
					.getPath(),
				workingDir.toString() + "/newRelationPath",
				getClass()
					.getResource(
						"/eu/dnetlib/dhp/entitytoorganizationfromsemrel/execstep/currentIteration/")
					.getPath(),
				getClass()
					.getResource(
						"/eu/dnetlib/dhp/entitytoorganizationfromsemrel/execstep/childParentOrg/")
					.getPath(),
				getClass()
					.getResource(
						"/eu/dnetlib/dhp/entitytoorganizationfromsemrel/execstep/resultOrganization/")
					.getPath(),
				ModelConstants.HAS_AUTHOR_INSTITUTION);

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Relation> tmp = sc
			.textFile(workingDir.toString() + "/newRelationPath")
			.map(item -> OBJECT_MAPPER.readValue(item, Relation.class));

		Assertions.assertEquals(4, tmp.count());

		Dataset<Relation> verificationDs = spark.createDataset(tmp.rdd(), Encoders.bean(Relation.class));

		verificationDs
			.foreach(
				(ForeachFunction<Relation>) r -> Assertions
					.assertEquals("propagation", r.getDataInfo().getInferenceprovenance()));

		verificationDs
			.foreach((ForeachFunction<Relation>) r -> Assertions.assertEquals("0.85", r.getDataInfo().getTrust()));

		verificationDs
			.foreach((ForeachFunction<Relation>) r -> Assertions.assertEquals("50|", r.getSource().substring(0, 3)));

		verificationDs
			.foreach((ForeachFunction<Relation>) r -> Assertions.assertEquals("20|", r.getTarget().substring(0, 3)));

		verificationDs
			.foreach(
				(ForeachFunction<Relation>) r -> Assertions
					.assertEquals(ModelConstants.HAS_AUTHOR_INSTITUTION, r.getRelClass()));

		verificationDs
			.foreach(
				(ForeachFunction<Relation>) r -> Assertions
					.assertEquals(ModelConstants.RESULT_ORGANIZATION, r.getRelType()));

		verificationDs
			.foreach(
				(ForeachFunction<Relation>) r -> Assertions
					.assertEquals(ModelConstants.AFFILIATION, r.getSubRelType()));

		verificationDs
			.foreach(
				(ForeachFunction<Relation>) r -> Assertions
					.assertEquals(
						PropagationConstant.PROPAGATION_RELATION_RESULT_ORGANIZATION_SEM_REL_CLASS_ID,
						r.getDataInfo().getProvenanceaction().getClassid()));

		verificationDs
			.foreach(
				(ForeachFunction<Relation>) r -> Assertions
					.assertEquals(
						PropagationConstant.PROPAGATION_RELATION_RESULT_ORGANIZATION_SEM_REL_CLASS_NAME,
						r.getDataInfo().getProvenanceaction().getClassname()));

		verificationDs
			.filter(
				(FilterFunction<Relation>) r -> r
					.getSource()
					.equals("50|doajarticles::1cae0b82b56ccd97c2db1f698def7074"))
			.foreach(
				(ForeachFunction<Relation>) r -> Assertions
					.assertEquals("20|doajarticles::1cae0b82b56ccd97c2db1f698def7074", r.getTarget()));

		verificationDs
			.filter(
				(FilterFunction<Relation>) r -> r
					.getSource()
					.equals("50|doajarticles::2baa9032dc058d3c8ff780c426b0c19f"))
			.foreach(
				(ForeachFunction<Relation>) r -> Assertions
					.assertEquals("20|doajarticles::2baa9032dc058d3c8ff780c426b0c19f", r.getTarget()));

		Assertions
			.assertEquals(
				2,
				verificationDs
					.filter(
						(FilterFunction<Relation>) r -> r
							.getSource()
							.equals("50|dedup_wf_001::2899e571609779168222fdeb59cb916d"))
					.count());

		Assertions
			.assertEquals(
				1,
				verificationDs
					.filter(
						(FilterFunction<Relation>) r -> r
							.getSource()
							.equals("50|dedup_wf_001::2899e571609779168222fdeb59cb916d") &&
							r.getTarget().equals("20|doajarticles::2baa9032dc058d3c8ff780c426b0c19f"))
					.count());

		Assertions
			.assertEquals(
				1,
				verificationDs
					.filter(
						(FilterFunction<Relation>) r -> r
							.getSource()
							.equals("50|dedup_wf_001::2899e571609779168222fdeb59cb916d") &&
							r.getTarget().equals("20|dedup_wf_001::2899e571609779168222fdeb59cb916d"))
					.count());

		tmp.foreach(r -> System.out.println(OBJECT_MAPPER.writeValueAsString(r)));
	}

	@Test
	public void prepareForNextStepLeavesTest() {

		StepActions
			.prepareForNextStep(
				spark,
				getClass()
					.getResource(
						"/eu/dnetlib/dhp/entitytoorganizationfromsemrel/execstep/relsforiteration1/")
					.getPath(),
				getClass()
					.getResource(
						"/eu/dnetlib/dhp/entitytoorganizationfromsemrel/execstep/resultOrganization/")
					.getPath(),
				getClass()
					.getResource(
						"/eu/dnetlib/dhp/entitytoorganizationfromsemrel/execstep/currentIteration/")
					.getPath(),
				getClass()
					.getResource(
						"/eu/dnetlib/dhp/entitytoorganizationfromsemrel/execstep/childParentOrg/")
					.getPath(),
				workingDir.toString() + "/tempLeaves", workingDir.toString() + "/tempOrgs");

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Leaves> tmp = sc
			.textFile(workingDir.toString() + "/tempLeaves")
			.map(item -> OBJECT_MAPPER.readValue(item, Leaves.class));

		Assertions.assertEquals(3, tmp.count());

		Assertions
			.assertEquals(
				1, tmp.filter(l -> l.getValue().equals("20|doajarticles::2baa9032dc058d3c8ff780c426b0c19f")).count());

		Assertions
			.assertEquals(
				1, tmp.filter(l -> l.getValue().equals("20|dedup_wf_001::2899e571609779168222fdeb59cb916d")).count());

		Assertions
			.assertEquals(
				1, tmp.filter(l -> l.getValue().equals("20|doajarticles::1cae0b82b56ccd97c2db1f698def7074")).count());

	}

	@Test
	public void prepareFonNextStepOrgTest() {
		StepActions
			.prepareForNextStep(
				spark,
				getClass()
					.getResource(
						"/eu/dnetlib/dhp/entitytoorganizationfromsemrel/execstep/relsforiteration1/")
					.getPath(),
				getClass()
					.getResource(
						"/eu/dnetlib/dhp/entitytoorganizationfromsemrel/execstep/resultOrganization/")
					.getPath(),
				getClass()
					.getResource(
						"/eu/dnetlib/dhp/entitytoorganizationfromsemrel/execstep/currentIteration/")
					.getPath(),
				getClass()
					.getResource(
						"/eu/dnetlib/dhp/entitytoorganizationfromsemrel/execstep/childParentOrg/")
					.getPath(),
				workingDir.toString() + "/tempLeaves", workingDir.toString() + "/tempOrgs");

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<KeyValueSet> tmp = sc
			.textFile(workingDir.toString() + "/tempOrgs")
			.map(item -> OBJECT_MAPPER.readValue(item, KeyValueSet.class));

		Assertions.assertEquals(5, tmp.count());

		Assertions
			.assertEquals(
				1, tmp
					.filter(kv -> kv.getKey().equals("50|openaire____::ec653e804967133b9436fdd30d3ff51d"))
					.collect()
					.get(0)
					.getValueSet()
					.size());
		Assertions
			.assertEquals(
				"20|doajarticles::1cae0b82b56ccd97c2db1f698def7074",
				tmp
					.filter(kv -> kv.getKey().equals("50|openaire____::ec653e804967133b9436fdd30d3ff51d"))
					.collect()
					.get(0)
					.getValueSet()
					.get(0));

		Assertions
			.assertEquals(
				1, tmp
					.filter(kv -> kv.getKey().equals("50|doajarticles::03748bcb5d754c951efec9700e18a56d"))
					.collect()
					.get(0)
					.getValueSet()
					.size());
		Assertions
			.assertEquals(
				"20|doajarticles::2baa9032dc058d3c8ff780c426b0c19f",
				tmp
					.filter(kv -> kv.getKey().equals("50|doajarticles::03748bcb5d754c951efec9700e18a56d"))
					.collect()
					.get(0)
					.getValueSet()
					.get(0));

		Assertions
			.assertEquals(
				4, tmp
					.filter(kv -> kv.getKey().equals("50|dedup_wf_001::2899e571609779168222fdeb59cb916d"))
					.collect()
					.get(0)
					.getValueSet()
					.size());
		Assertions
			.assertTrue(
				tmp
					.filter(kv -> kv.getKey().equals("50|dedup_wf_001::2899e571609779168222fdeb59cb916d"))
					.collect()
					.get(0)
					.getValueSet()
					.contains("20|dedup_wf_001::2899e571609779168222fdeb59cb916d"));
		Assertions
			.assertTrue(
				tmp
					.filter(kv -> kv.getKey().equals("50|dedup_wf_001::2899e571609779168222fdeb59cb916d"))
					.collect()
					.get(0)
					.getValueSet()
					.contains("20|doajarticles::396262ee936f3d3e26ff0e60bea6cae0"));
		Assertions
			.assertTrue(
				tmp
					.filter(kv -> kv.getKey().equals("50|dedup_wf_001::2899e571609779168222fdeb59cb916d"))
					.collect()
					.get(0)
					.getValueSet()
					.contains("20|pippo_wf_001::2899e571609779168222fdeb59cb916d"));
		Assertions
			.assertTrue(
				tmp
					.filter(kv -> kv.getKey().equals("50|dedup_wf_001::2899e571609779168222fdeb59cb916d"))
					.collect()
					.get(0)
					.getValueSet()
					.contains("20|doajarticles::2baa9032dc058d3c8ff780c426b0c19f"));

		Assertions
			.assertEquals(
				2, tmp
					.filter(kv -> kv.getKey().equals("50|doajarticles::1cae0b82b56ccd97c2db1f698def7074"))
					.collect()
					.get(0)
					.getValueSet()
					.size());
		Assertions
			.assertTrue(
				tmp
					.filter(kv -> kv.getKey().equals("50|doajarticles::1cae0b82b56ccd97c2db1f698def7074"))
					.collect()
					.get(0)
					.getValueSet()
					.contains("20|opendoar____::a5fcb8eb25ebd6f7cd219e0fa1e6ddc1"));
		Assertions
			.assertTrue(
				tmp
					.filter(kv -> kv.getKey().equals("50|doajarticles::1cae0b82b56ccd97c2db1f698def7074"))
					.collect()
					.get(0)
					.getValueSet()
					.contains("20|doajarticles::1cae0b82b56ccd97c2db1f698def7074"));

		Assertions
			.assertEquals(
				3, tmp
					.filter(kv -> kv.getKey().equals("50|doajarticles::2baa9032dc058d3c8ff780c426b0c19f"))
					.collect()
					.get(0)
					.getValueSet()
					.size());
		Assertions
			.assertTrue(
				tmp
					.filter(kv -> kv.getKey().equals("50|doajarticles::2baa9032dc058d3c8ff780c426b0c19f"))
					.collect()
					.get(0)
					.getValueSet()
					.contains("20|dedup_wf_001::2899e571609779168222fdeb59cb916d"));
		Assertions
			.assertTrue(
				tmp
					.filter(kv -> kv.getKey().equals("50|doajarticles::2baa9032dc058d3c8ff780c426b0c19f"))
					.collect()
					.get(0)
					.getValueSet()
					.contains("20|pippo_wf_001::2899e571609779168222fdeb59cb916d"));
		Assertions
			.assertTrue(
				tmp
					.filter(kv -> kv.getKey().equals("50|doajarticles::2baa9032dc058d3c8ff780c426b0c19f"))
					.collect()
					.get(0)
					.getValueSet()
					.contains("20|doajarticles::2baa9032dc058d3c8ff780c426b0c19f"));

	}
}
