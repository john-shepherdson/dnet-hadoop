
package eu.dnetlib.dhp.actionmanager.opencitations;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.utils.CleaningFunctions;
import eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory;

public class CreateOpenCitationsASTest {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SparkSession spark;

	private static Path workingDir;
	private static final Logger log = LoggerFactory
		.getLogger(CreateOpenCitationsASTest.class);

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files
			.createTempDirectory(CreateOpenCitationsASTest.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(CreateOpenCitationsASTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(CreateOpenCitationsASTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	@Test
	void testNumberofRelations() throws Exception {

		String inputPath = getClass()
			.getResource(
				"/eu/dnetlib/dhp/actionmanager/opencitations/COCI")
			.getPath();

		CreateActionSetSparkJob
			.main(
				new String[] {
					"-isSparkSessionManaged",
					Boolean.FALSE.toString(),
					"-shouldDuplicateRels",
					Boolean.TRUE.toString(),
					"-inputPath",
					inputPath,
					"-outputPath",
					workingDir.toString() + "/actionSet1"
				});

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaRDD<Relation> tmp = sc
			.sequenceFile(workingDir.toString() + "/actionSet1", Text.class, Text.class)
			.map(value -> OBJECT_MAPPER.readValue(value._2().toString(), AtomicAction.class))
			.map(aa -> ((Relation) aa.getPayload()));

		assertEquals(31, tmp.count());

		// tmp.foreach(r -> System.out.println(OBJECT_MAPPER.writeValueAsString(r)));

	}

	@Test
	void testNumberofRelations2() throws Exception {

		String inputPath = getClass()
			.getResource(
				"/eu/dnetlib/dhp/actionmanager/opencitations/COCI")
			.getPath();

		CreateActionSetSparkJob
			.main(
				new String[] {
					"-isSparkSessionManaged",
					Boolean.FALSE.toString(),
					"-inputPath",
					inputPath,
					"-outputPath",
					workingDir.toString() + "/actionSet2"
				});

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaRDD<Relation> tmp = sc
			.sequenceFile(workingDir.toString() + "/actionSet2", Text.class, Text.class)
			.map(value -> OBJECT_MAPPER.readValue(value._2().toString(), AtomicAction.class))
			.map(aa -> ((Relation) aa.getPayload()));

		assertEquals(23, tmp.count());
	}

	@Test
	void testRelationsCollectedFrom() throws Exception {

		String inputPath = getClass()
			.getResource(
				"/eu/dnetlib/dhp/actionmanager/opencitations/COCI")
			.getPath();

		CreateActionSetSparkJob
			.main(
				new String[] {
					"-isSparkSessionManaged",
					Boolean.FALSE.toString(),
					"-inputPath",
					inputPath,
					"-outputPath",
					workingDir.toString() + "/actionSet3"
				});

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaRDD<Relation> tmp = sc
			.sequenceFile(workingDir.toString() + "/actionSet3", Text.class, Text.class)
			.map(value -> OBJECT_MAPPER.readValue(value._2().toString(), AtomicAction.class))
			.map(aa -> ((Relation) aa.getPayload()));

		tmp.foreach(r -> {
			assertEquals(ModelConstants.OPENOCITATIONS_NAME, r.getProvenance().get(0).getCollectedfrom().getValue());
			assertEquals(ModelConstants.OPENOCITATIONS_ID, r.getProvenance().get(0).getCollectedfrom().getKey());
		});

	}

	@Test
	void testRelationsDataInfo() throws Exception {

		String inputPath = getClass()
			.getResource(
				"/eu/dnetlib/dhp/actionmanager/opencitations/COCI")
			.getPath();

		CreateActionSetSparkJob
			.main(
				new String[] {
					"-isSparkSessionManaged",
					Boolean.FALSE.toString(),
					"-inputPath",
					inputPath,
					"-outputPath",
					workingDir.toString() + "/actionSet4"
				});

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaRDD<Relation> tmp = sc
			.sequenceFile(workingDir.toString() + "/actionSet4", Text.class, Text.class)
			.map(value -> OBJECT_MAPPER.readValue(value._2().toString(), AtomicAction.class))
			.map(aa -> ((Relation) aa.getPayload()));

		tmp.foreach(r -> {
			final DataInfo dataInfo = r.getProvenance().get(0).getDataInfo();
			assertEquals(false, dataInfo.getInferred());
			assertEquals(0.91f, dataInfo.getTrust());
			assertEquals(
				CreateActionSetSparkJob.OPENCITATIONS_CLASSID, dataInfo.getProvenanceaction().getClassid());
			assertEquals(
				CreateActionSetSparkJob.OPENCITATIONS_CLASSNAME, dataInfo.getProvenanceaction().getClassname());
			assertEquals(ModelConstants.DNET_PROVENANCE_ACTIONS, dataInfo.getProvenanceaction().getSchemeid());
		});

	}

	@Test
	void testRelationsSemantics() throws Exception {

		String inputPath = getClass()
			.getResource(
				"/eu/dnetlib/dhp/actionmanager/opencitations/COCI")
			.getPath();

		CreateActionSetSparkJob
			.main(
				new String[] {
					"-isSparkSessionManaged",
					Boolean.FALSE.toString(),
					"-inputPath",
					inputPath,
					"-outputPath",
					workingDir.toString() + "/actionSet5"
				});

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaRDD<Relation> tmp = sc
			.sequenceFile(workingDir.toString() + "/actionSet5", Text.class, Text.class)
			.map(value -> OBJECT_MAPPER.readValue(value._2().toString(), AtomicAction.class))
			.map(aa -> ((Relation) aa.getPayload()));

		tmp.foreach(r -> {
			assertEquals("citation", r.getSubRelType());
			assertEquals("resultResult", r.getRelType());
		});
		assertEquals(23, tmp.count());
		assertEquals(23, tmp.filter(r -> r.getRelClass().equals("Cites")).count());
	}

	@Test
	void testRelationsSourceTargetPrefix() throws Exception {

		String inputPath = getClass()
			.getResource(
				"/eu/dnetlib/dhp/actionmanager/opencitations/COCI")
			.getPath();

		CreateActionSetSparkJob
			.main(
				new String[] {
					"-isSparkSessionManaged",
					Boolean.FALSE.toString(),
					"-inputPath",
					inputPath,
					"-outputPath",
					workingDir.toString() + "/actionSet6"
				});

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaRDD<Relation> tmp = sc
			.sequenceFile(workingDir.toString() + "/actionSet6", Text.class, Text.class)
			.map(value -> OBJECT_MAPPER.readValue(value._2().toString(), AtomicAction.class))
			.map(aa -> ((Relation) aa.getPayload()));

		tmp.foreach(r -> {
			assertEquals("50|doi_________::", r.getSource().substring(0, 17));
			assertEquals("50|doi_________::", r.getTarget().substring(0, 17));
		});

	}

	@Test
	void testRelationsSourceTargetCouple() throws Exception {
		final String doi1 = "50|doi_________::"
			+ ModelSupport.md5(CleaningFunctions.normalizePidValue("doi", "10.1007/s10854-015-3684-x"));
		final String doi2 = "50|doi_________::"
			+ ModelSupport.md5(CleaningFunctions.normalizePidValue("doi", "10.1111/j.1551-2916.2008.02408.x"));
		final String doi3 = "50|doi_________::"
			+ ModelSupport.md5(CleaningFunctions.normalizePidValue("doi", "10.1007/s10854-014-2114-9"));
		final String doi4 = "50|doi_________::"
			+ ModelSupport.md5(CleaningFunctions.normalizePidValue("doi", "10.1016/j.ceramint.2013.09.069"));
		final String doi5 = "50|doi_________::"
			+ ModelSupport.md5(CleaningFunctions.normalizePidValue("doi", "10.1007/s10854-009-9913-4"));
		final String doi6 = "50|doi_________::"
			+ ModelSupport.md5(CleaningFunctions.normalizePidValue("doi", "10.1016/0038-1098(72)90370-5"));

		String inputPath = getClass()
			.getResource(
				"/eu/dnetlib/dhp/actionmanager/opencitations/COCI")
			.getPath();

		CreateActionSetSparkJob
			.main(
				new String[] {
					"-isSparkSessionManaged",
					Boolean.FALSE.toString(),
					"-inputPath",
					inputPath,
					"-outputPath",
					workingDir.toString() + "/actionSet7"
				});

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaRDD<Relation> tmp = sc
			.sequenceFile(workingDir.toString() + "/actionSet7", Text.class, Text.class)
			.map(value -> OBJECT_MAPPER.readValue(value._2().toString(), AtomicAction.class))
			.map(aa -> ((Relation) aa.getPayload()));

		JavaRDD<Relation> check = tmp.filter(r -> r.getSource().equals(doi1) || r.getTarget().equals(doi1));

		assertEquals(5, check.count());

		check.foreach(r -> {
			if (r.getSource().equals(doi2) || r.getSource().equals(doi3) || r.getSource().equals(doi4) ||
				r.getSource().equals(doi5) || r.getSource().equals(doi6)) {
				assertEquals(Relation.RELCLASS.IsCitedBy, r.getRelClass());
				assertEquals(doi1, r.getTarget());
			}
		});

		assertEquals(5, check.filter(r -> r.getSource().equals(doi1)).count());
		check.filter(r -> r.getSource().equals(doi1)).foreach(r -> assertEquals(Relation.RELCLASS.Cites, r.getRelClass()));

	}
}
