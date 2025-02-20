
package eu.dnetlib.dhp.person;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.utils.DHPUtils;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.enrich.relsfrompublisherenricheddata.EnrichExternalDataWithGraphORCID;
import eu.dnetlib.dhp.schema.oaf.Relation;

public class EnrichPublisherAndCreatePersonRelationsTest {
	private static final Logger log = LoggerFactory.getLogger(EnrichPublisherAndCreatePersonRelationsTest.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SparkSession spark;

	private static Path workingDir;

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files.createTempDirectory(EnrichPublisherAndCreatePersonRelationsTest.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(EnrichPublisherAndCreatePersonRelationsTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(EnrichPublisherAndCreatePersonRelationsTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	@Test
	void testNewRelationNoMergeNoDedup() throws Exception {
		final String sourcePathPubs = getClass()
			.getResource("/eu/dnetlib/dhp/person/testNewRelationNoMergeNoDedup/graph/publication")
			.getPath();
		final String sourcePathRels = getClass()
				.getResource("/eu/dnetlib/dhp/person/testNewRelationNoMergeNoDedup/graph/relation")
				.getPath();
		final String publisherPath = getClass()
			.getResource("/eu/dnetlib/dhp/person/testNewRelationNoMergeNoDedup/publisher/")
			.getPath();

		spark.read().json(sourcePathPubs).write().json(workingDir.toString() + "/graph/publication");
		spark.read().json(sourcePathRels).write().json(workingDir.toString() + "/graph/relation");
		spark.read().json(publisherPath).write().json(workingDir.toString() + "/publisher");

		EnrichExternalDataWithGraphORCID.main(new String[] {

			"--orcidPath", workingDir.toString() + "/graph",
			"--targetPath", workingDir.toString() + "/graph",
			"--graphPath", workingDir.toString() + "/publisher",
			"--workingDir", workingDir.toString() + "/working",
			"--master", "yarn",
				"--matchingSource","graph"
		});

		//Anthony R Burrell arricchito con l'orcid' (0000-0001-8255-3618) dal grafo ha
		//{"Provenance":"AffRo","PID":"ROR","Value":"https:\/\/ror.org\/029m7xn54","Confidence":1,"Status":"active"},{"Provenance":"AffRo","PID":"OpenOrgs","Value":"0000002097","Confidence":1,"Status":"active"}

		org.apache.spark.sql.Dataset<Relation> relations = spark.read().schema(Encoders.bean(Relation.class).schema())
				.json(workingDir.toString() + "/graph/relation")
				.as(Encoders.bean(Relation.class));

		Assertions.assertEquals(19, relations.count());
		Assertions.assertEquals(1, relations.filter((FilterFunction<Relation>) r -> r.getSubRelType().equalsIgnoreCase("authorship")).count());
		Relation relation = relations.filter((FilterFunction<Relation>) r -> r.getSubRelType().equalsIgnoreCase("authorship")).first();
		Assertions.assertEquals("30|orcid_______::" + DHPUtils.md5("0000-0001-8255-3618"), relation.getSource());
		Assertions.assertEquals("50|doi_________::" + DHPUtils.md5("10.11646/phytotaxa.379.3.5"), relation.getTarget());

		Assertions.assertEquals(1, relation.getProperties().size());
		Assertions.assertEquals("declared_affiliation", relation.getProperties().get(0).getKey());
		Assertions.assertEquals("https://ror.org/029m7xn54", relation.getProperties().get(0).getValue());
		Assertions.assertEquals(1,
				Double.parseDouble(relation.getProperties().get(0).getDataInfo().getTrust()));

		Assertions.assertEquals(0, relations.filter((FilterFunction<Relation>) r -> r.getRelClass().equalsIgnoreCase(ModelConstants.PERSON_PERSON_HASCOAUTHORED)).count());

	}

	//this one has merges relations (enriched a result deduplicated) no other authorship relations insist on the result
	@Test
	void testNewRelationMergeNoDedup() throws Exception {
		final String sourcePathPubs = getClass()
				.getResource("/eu/dnetlib/dhp/person/testNewRelationMergeNoDedup/graph/publication")
				.getPath();
		final String sourcePathRels = getClass()
				.getResource("/eu/dnetlib/dhp/person/testNewRelationMergeNoDedup/graph/relation")
				.getPath();
		final String publisherPath = getClass()
				.getResource("/eu/dnetlib/dhp/person/testNewRelationMergeNoDedup/publisher/")
				.getPath();

		spark.read().json(sourcePathPubs).write().json(workingDir.toString() + "/graph/publication");
		spark.read().json(sourcePathRels).write().json(workingDir.toString() + "/graph/relation");
		spark.read().json(publisherPath).write().json(workingDir.toString() + "/publisher");

		EnrichExternalDataWithGraphORCID.main(new String[] {

				"--orcidPath", workingDir.toString() + "/graph",
				"--targetPath", workingDir.toString() + "/graph",
				"--graphPath", workingDir.toString() + "/publisher",
				"--workingDir", workingDir.toString() + "/working",
				"--master", "yarn",
				"--matchingSource","graph"
		});

		//Anthony R Burrell arricchito con l'orcid' (0000-0001-8255-3618) dal grafo ha
		//{"Provenance":"AffRo","PID":"ROR","Value":"https:\/\/ror.org\/029m7xn54","Confidence":1,"Status":"active"},{"Provenance":"AffRo","PID":"OpenOrgs","Value":"0000002097","Confidence":1,"Status":"active"}

		org.apache.spark.sql.Dataset<Relation> relations = spark.read().schema(Encoders.bean(Relation.class).schema())
				.json(workingDir.toString() + "/graph/relation")
				.as(Encoders.bean(Relation.class));

		Assertions.assertEquals(18, relations.count());
		Assertions.assertEquals(1, relations.filter((FilterFunction<Relation>) r -> r.getSubRelType().equalsIgnoreCase("authorship")).count());
		Relation relation = relations.filter((FilterFunction<Relation>) r -> r.getSubRelType().equalsIgnoreCase("authorship")).first();
		Assertions.assertEquals("30|orcid_______::" + DHPUtils.md5("0000-0001-8255-3618"), relation.getSource());
		Assertions.assertEquals("50|doi_________::" + DHPUtils.md5("10.11646/phytotaxa.379.3.5"), relation.getTarget());

		Assertions.assertEquals(2, relation.getProperties().size());
		relation.getProperties().forEach(p -> Assertions.assertEquals("declared_affiliation", p.getKey()));
		Assertions.assertTrue(relation.getProperties().stream().anyMatch(p -> p.getValue().equals("https://ror.org/029m7xn54")));
		Assertions.assertTrue(relation.getProperties().stream().anyMatch(p -> p.getValue().equals("https://ror.org/029m7fake")));
		Assertions.assertEquals(1,
				Double.parseDouble(relation.getProperties().stream().filter(p -> p.getDataInfo()!= null).findFirst().get().getDataInfo().getTrust()));

		Assertions.assertEquals(0, relations.filter((FilterFunction<Relation>) r -> r.getRelClass().equalsIgnoreCase(ModelConstants.PERSON_PERSON_HASCOAUTHORED)).count());
	}

	//this one has merges relations (enriched a result deduplicated) other authorship relations insist on the result
	//extend the properties
	@Test
	void testNewRelationNoMergeDedup() throws Exception {
		final String sourcePathPubs = getClass()
				.getResource("/eu/dnetlib/dhp/person/testNewRelationNoMergeDedup/graph/publication")
				.getPath();
		final String sourcePathRels = getClass()
				.getResource("/eu/dnetlib/dhp/person/testNewRelationNoMergeDedup/graph/relation")
				.getPath();
		final String publisherPath = getClass()
				.getResource("/eu/dnetlib/dhp/person/testNewRelationNoMergeDedup/publisher/")
				.getPath();

		spark.read().json(sourcePathPubs).write().json(workingDir.toString() + "/graph/publication");
		spark.read().json(sourcePathRels).write().json(workingDir.toString() + "/graph/relation");
		spark.read().json(publisherPath).write().json(workingDir.toString() + "/publisher");

		EnrichExternalDataWithGraphORCID.main(new String[] {

				"--orcidPath", workingDir.toString() + "/graph",
				"--targetPath", workingDir.toString() + "/graph",
				"--graphPath", workingDir.toString() + "/publisher",
				"--workingDir", workingDir.toString() + "/working",
				"--master", "yarn",
				"--matchingSource","graph"
		});

		//Anthony R Burrell arricchito con l'orcid' (0000-0001-8255-3618) dal grafo ha
		//{"Provenance":"AffRo","PID":"ROR","Value":"https:\/\/ror.org\/029m7xn54","Confidence":1,"Status":"active"},{"Provenance":"AffRo","PID":"OpenOrgs","Value":"0000002097","Confidence":1,"Status":"active"}

		org.apache.spark.sql.Dataset<Relation> relations = spark.read().schema(Encoders.bean(Relation.class).schema())
				.json(workingDir.toString() + "/graph/relation")
				.as(Encoders.bean(Relation.class));

		Assertions.assertEquals(19, relations.count());
		Assertions.assertEquals(1, relations.filter((FilterFunction<Relation>) r -> r.getSubRelType().equalsIgnoreCase("authorship")).count());
		Relation relation = relations.filter((FilterFunction<Relation>) r -> r.getSubRelType().equalsIgnoreCase("authorship")).first();
		Assertions.assertEquals("30|orcid_______::" + DHPUtils.md5("0000-0001-8255-3618"), relation.getSource());
		Assertions.assertEquals("50|doi_________::" + DHPUtils.md5("10.11646/phytotaxa.379.3.5"), relation.getTarget());

		Assertions.assertEquals(1, relation.getProperties().size());
		Assertions.assertEquals("declared_affiliation", relation.getProperties().get(0).getKey());
		Assertions.assertEquals("https://ror.org/029m7xn54", relation.getProperties().get(0).getValue());
		Assertions.assertEquals(1,
				Double.parseDouble(relation.getProperties().get(0).getDataInfo().getTrust()));

		Assertions.assertEquals(0, relations.filter((FilterFunction<Relation>) r -> r.getRelClass().equalsIgnoreCase(ModelConstants.PERSON_PERSON_HASCOAUTHORED)).count());
	}

	@Test
	void testNewRelationMergeDedup() throws Exception {
		final String sourcePathPubs = getClass()
				.getResource("/eu/dnetlib/dhp/person/testNewRelationMergeDedup/graph/publication")
				.getPath();
		final String sourcePathRels = getClass()
				.getResource("/eu/dnetlib/dhp/person/testNewRelationMergeDedup/graph/relation")
				.getPath();
		final String publisherPath = getClass()
				.getResource("/eu/dnetlib/dhp/person/testNewRelationMergeDedup/publisher/")
				.getPath();

		spark.read().json(sourcePathPubs).write().json(workingDir.toString() + "/graph/publication");
		spark.read().json(sourcePathRels).write().json(workingDir.toString() + "/graph/relation");
		spark.read().json(publisherPath).write().json(workingDir.toString() + "/publisher");

		EnrichExternalDataWithGraphORCID.main(new String[] {

				"--orcidPath", workingDir.toString() + "/graph",
				"--targetPath", workingDir.toString() + "/graph",
				"--graphPath", workingDir.toString() + "/publisher",
				"--workingDir", workingDir.toString() + "/working",
				"--master", "yarn",
				"--matchingSource","graph"
		});

		//Anthony R Burrell arricchito con l'orcid' (0000-0001-8255-3618) dal grafo ha
		//{"Provenance":"AffRo","PID":"ROR","Value":"https:\/\/ror.org\/029m7xn54","Confidence":1,"Status":"active"},{"Provenance":"AffRo","PID":"OpenOrgs","Value":"0000002097","Confidence":1,"Status":"active"}

		org.apache.spark.sql.Dataset<Relation> relations = spark.read().schema(Encoders.bean(Relation.class).schema())
				.json(workingDir.toString() + "/graph/relation")
				.as(Encoders.bean(Relation.class));

		Assertions.assertEquals(18, relations.count());
		Assertions.assertEquals(1, relations.filter((FilterFunction<Relation>) r -> r.getSubRelType().equalsIgnoreCase("authorship")).count());
		Relation relation = relations.filter((FilterFunction<Relation>) r -> r.getSubRelType().equalsIgnoreCase("authorship")).first();
		Assertions.assertEquals("30|orcid_______::" + DHPUtils.md5("0000-0001-8255-3618"), relation.getSource());
		Assertions.assertEquals("50|doi_________::" + DHPUtils.md5("10.11646/phytotaxa.379.3.5"), relation.getTarget());

		Assertions.assertEquals(2, relation.getProperties().size());
		relation.getProperties().forEach(p -> Assertions.assertEquals("declared_affiliation", p.getKey()));
		Assertions.assertTrue(relation.getProperties().stream().anyMatch(p -> p.getValue().equals("https://ror.org/029m7xn54")));
		Assertions.assertTrue(relation.getProperties().stream().anyMatch(p -> p.getValue().equals("https://ror.org/029m7fake")));
		Assertions.assertEquals(1,
				Double.parseDouble(relation.getProperties().stream().filter(p -> p.getDataInfo()!= null).findFirst().get().getDataInfo().getTrust()));

		Assertions.assertEquals(0, relations.filter((FilterFunction<Relation>) r -> r.getRelClass().equalsIgnoreCase(ModelConstants.PERSON_PERSON_HASCOAUTHORED)).count());

	}

	//this one creates also new coauthorship relationships
	@Test
	void testNewRelationCoAuthorship() throws Exception {
		final String sourcePathPubs = getClass()
				.getResource("/eu/dnetlib/dhp/person/testNewAuthorshiRelations/graph/publication")
				.getPath();
		final String sourcePathRels = getClass()
				.getResource("/eu/dnetlib/dhp/person/testNewAuthorshiRelations/graph/relation")
				.getPath();
		final String publisherPath = getClass()
				.getResource("/eu/dnetlib/dhp/person/testNewAuthorshiRelations/publisher/")
				.getPath();

		spark.read().json(sourcePathPubs).write().json(workingDir.toString() + "/graph/publication");
		spark.read().json(sourcePathRels).write().json(workingDir.toString() + "/graph/relation");
		spark.read().json(publisherPath).write().json(workingDir.toString() + "/publisher");

		EnrichExternalDataWithGraphORCID.main(new String[] {

				"--orcidPath", workingDir.toString() + "/graph",
				"--targetPath", workingDir.toString() + "/graph",
				"--graphPath", workingDir.toString() + "/publisher",
				"--workingDir", workingDir.toString() + "/working",
				"--master", "yarn",
				"--matchingSource","graph"
		});

		//Anthony R Burrell arricchito con l'orcid' (0000-0001-8255-3618) dal grafo ha
		//{"Provenance":"AffRo","PID":"ROR","Value":"https:\/\/ror.org\/029m7xn54","Confidence":1,"Status":"active"},{"Provenance":"AffRo","PID":"OpenOrgs","Value":"0000002097","Confidence":1,"Status":"active"}

		org.apache.spark.sql.Dataset<Relation> relations = spark.read().schema(Encoders.bean(Relation.class).schema())
				.json(workingDir.toString() + "/graph/relation")
				.as(Encoders.bean(Relation.class));

		Assertions.assertEquals(20, relations.count());
		Assertions.assertEquals(1, relations.filter((FilterFunction<Relation>) r -> r.getSubRelType().equalsIgnoreCase("authorship")).count());
		Relation relation = relations.filter((FilterFunction<Relation>) r -> r.getSubRelType().equalsIgnoreCase("authorship")).first();
		Assertions.assertEquals("30|orcid_______::" + DHPUtils.md5("0000-0001-8255-3618"), relation.getSource());
		Assertions.assertEquals("50|doi_________::" + DHPUtils.md5("10.11646/phytotaxa.379.3.5"), relation.getTarget());

		Assertions.assertEquals(2, relation.getProperties().size());
		relation.getProperties().forEach(p -> Assertions.assertEquals("declared_affiliation", p.getKey()));
		Assertions.assertTrue(relation.getProperties().stream().anyMatch(p -> p.getValue().equals("https://ror.org/029m7xn54")));
		Assertions.assertTrue(relation.getProperties().stream().anyMatch(p -> p.getValue().equals("https://ror.org/029m7fake")));
		Assertions.assertEquals(1,
				Double.parseDouble(relation.getProperties().stream().filter(p -> p.getDataInfo()!= null).findFirst().get().getDataInfo().getTrust()));

		Assertions.assertEquals(2, relations.filter((FilterFunction<Relation>) r -> r.getRelClass().equalsIgnoreCase(ModelConstants.PERSON_PERSON_HASCOAUTHORED)).count());

	}

}
