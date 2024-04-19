
package eu.dnetlib.dhp.actionmanager.webcrawl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.actionmanager.opencitations.CreateActionSetSparkJob;
import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.utils.CleaningFunctions;
import eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory;
import eu.dnetlib.dhp.schema.oaf.utils.PidCleaner;
import eu.dnetlib.dhp.schema.oaf.utils.PidType;

public class CreateASTest {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SparkSession spark;

	private static Path workingDir;
	private static final Logger log = LoggerFactory
		.getLogger(CreateASTest.class);

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files
			.createTempDirectory(CreateASTest.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(CreateASTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(CreateASTest.class.getSimpleName())
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
				"/eu/dnetlib/dhp/actionmanager/webcrawl/")
			.getPath();

		CreateActionSetFromWebEntries
			.main(
				new String[] {
					"-isSparkSessionManaged",
					Boolean.FALSE.toString(),
					"-sourcePath",
					inputPath,
					"-outputPath",
					workingDir.toString() + "/actionSet1"
				});

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaRDD<Relation> tmp = sc
			.sequenceFile(workingDir.toString() + "/actionSet1", Text.class, Text.class)
			.map(value -> OBJECT_MAPPER.readValue(value._2().toString(), AtomicAction.class))
			.map(aa -> ((Relation) aa.getPayload()));

		Assertions.assertEquals(64, tmp.count());

	}

//	https://ror.org/04c6bry31 "openalex":"https://openalex.org/W2115261608",
//			"doi":"https://doi.org/10.1056/nejmoa0908721",
//			"mag":"2115261608",
//			"pmid":"https://pubmed.ncbi.nlm.nih.gov/20375404"
//
//
//	https://ror.org/03bea9k73 "openalex":"https://openalex.org/W2157622195",
//			"doi":"https://doi.org/10.1016/s0140-6736(10)60834-3",
//			"mag":"2157622195",
//			"pmid":"https://pubmed.ncbi.nlm.nih.gov/20561675"
//
//
//	https://ror.org/008te2062    "openalex":"https://openalex.org/W2104948944",
//			"doi":"https://doi.org/10.1056/nejmoa0909494",
//			"mag":"2104948944",
//			"pmid":"https://pubmed.ncbi.nlm.nih.gov/20089952"
//
//
//
//	https://ror.org/05m7pjf47 , https://ror.org/02tyrky19    "openalex":"https://openalex.org/W2071754162",
//			"doi":"https://doi.org/10.1371/journal.pone.0009672",
//			"mag":"2071754162",
//			"pmid":"https://pubmed.ncbi.nlm.nih.gov/20300637",
//			"pmcid":"https://www.ncbi.nlm.nih.gov/pmc/articles/2837382"
//
//
//
//	https://ror.org/05m7pjf47    "openalex":"https://openalex.org/W2144543496",
//			"doi":"https://doi.org/10.1086/649858",
//			"mag":"2144543496",
//			"pmid":"https://pubmed.ncbi.nlm.nih.gov/20047480",
//			"pmcid":"https://www.ncbi.nlm.nih.gov/pmc/articles/5826644"
//
//
//	https://ror.org/04q107642", "openalex":"https://openalex.org/W2115169717",
//			"doi":"https://doi.org/10.1016/s0140-6736(09)61965-6",
//			"mag":"2115169717",
//			"pmid":"https://pubmed.ncbi.nlm.nih.gov/20167359"
//
//
//	https://ror.org/03265fv13 "openalex":"https://openalex.org/W2119378720",
//			"doi":"https://doi.org/10.1038/nnano.2010.15",
//			"mag":"2119378720",
//			"pmid":"https://pubmed.ncbi.nlm.nih.gov/20173755"
//
//
//	https://ror.org/02tyrky19   "openalex":"https://openalex.org/W2140206763",
//			"doi":"https://doi.org/10.1038/nature08900",
//			"mag":"2140206763",
//			"pmid":"https://pubmed.ncbi.nlm.nih.gov/20200518",
//			"pmcid":"https://www.ncbi.nlm.nih.gov/pmc/articles/2862165"
//
//
//
//	https://ror.org/05m7pjf47 https://ror.org/02tyrky19    "openalex":"https://openalex.org/W2110374888",
//			"doi":"https://doi.org/10.1038/nature09146",
//			"mag":"2110374888",
//			"pmid":"https://pubmed.ncbi.nlm.nih.gov/20531469",
//			"pmcid":"https://www.ncbi.nlm.nih.gov/pmc/articles/3021798"
	@Test
	void testRelations() throws Exception {

//		 , "doi":"https://doi.org/10.1126/science.1188021", "pmid":"https://pubmed.ncbi.nlm.nih.gov/20448178", https://www.ncbi.nlm.nih.gov/pmc/articles/5100745

		String inputPath = getClass()
			.getResource(
				"/eu/dnetlib/dhp/actionmanager/webcrawl/")
			.getPath();

		CreateActionSetFromWebEntries
			.main(
				new String[] {
					"-isSparkSessionManaged",
					Boolean.FALSE.toString(),
					"-sourcePath",
					inputPath,
					"-outputPath",
					workingDir.toString() + "/actionSet1"
				});

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaRDD<Relation> tmp = sc
			.sequenceFile(workingDir.toString() + "/actionSet1", Text.class, Text.class)
			.map(value -> OBJECT_MAPPER.readValue(value._2().toString(), AtomicAction.class))
			.map(aa -> ((Relation) aa.getPayload()));

		tmp.foreach(r -> System.out.println(new ObjectMapper().writeValueAsString(r)));

		Assertions
			.assertEquals(
				1, tmp
					.filter(
						r -> r
							.getSource()
							.equals(
								"50|doi_________::" + IdentifierFactory
									.md5(
										PidCleaner
											.normalizePidValue(PidType.doi.toString(), "10.1098/rstl.1684.0023"))))
					.count());

		Assertions
			.assertEquals(
				1, tmp
					.filter(
						r -> r
							.getTarget()
							.equals(
								"50|doi_________::" + IdentifierFactory
									.md5(
										PidCleaner
											.normalizePidValue(PidType.doi.toString(), "10.1098/rstl.1684.0023"))))
					.count());

		Assertions
			.assertEquals(
				1, tmp
					.filter(
						r -> r
							.getSource()
							.equals(
								"20|ror_________::" + IdentifierFactory
									.md5(
										PidCleaner
											.normalizePidValue("ROR", "https://ror.org/03argrj65"))))
					.count());

		Assertions
			.assertEquals(
				1, tmp
					.filter(
						r -> r
							.getTarget()
							.equals(
								"20|ror_________::" + IdentifierFactory
									.md5(
										PidCleaner
											.normalizePidValue("ROR", "https://ror.org/03argrj65"))))
					.count());

		Assertions
			.assertEquals(
				5, tmp
					.filter(
						r -> r
							.getSource()
							.equals(
								"20|ror_________::" + IdentifierFactory
									.md5(
										PidCleaner
											.normalizePidValue("ROR", "https://ror.org/03265fv13"))))
					.count());

		Assertions
			.assertEquals(
				5, tmp
					.filter(
						r -> r
							.getTarget()
							.equals(
								"20|ror_________::" + IdentifierFactory
									.md5(
										PidCleaner
											.normalizePidValue("ROR", "https://ror.org/03265fv13"))))
					.count());

		Assertions
			.assertEquals(
				2, tmp
					.filter(
						r -> r
							.getTarget()
							.equals(
								"20|ror_________::" + IdentifierFactory
									.md5(
										PidCleaner
											.normalizePidValue(PidType.doi.toString(), "https://ror.org/03265fv13")))
							&& r.getSource().startsWith("50|doi"))
					.count());

		Assertions
			.assertEquals(
				2, tmp
					.filter(
						r -> r
							.getTarget()
							.equals(
								"20|ror_________::" + IdentifierFactory
									.md5(
										PidCleaner
											.normalizePidValue(PidType.doi.toString(), "https://ror.org/03265fv13")))
							&& r.getSource().startsWith("50|pmid"))
					.count());

		Assertions
			.assertEquals(
				1, tmp
					.filter(
						r -> r
							.getTarget()
							.equals(
								"20|ror_________::" + IdentifierFactory
									.md5(
										PidCleaner
											.normalizePidValue(PidType.doi.toString(), "https://ror.org/03265fv13")))
							&& r.getSource().startsWith("50|pmc"))
					.count());
	}

	@Test
	void testRelationsCollectedFrom() throws Exception {

		String inputPath = getClass()
			.getResource(
				"/eu/dnetlib/dhp/actionmanager/webcrawl")
			.getPath();

		CreateActionSetFromWebEntries
			.main(
				new String[] {
					"-isSparkSessionManaged",
					Boolean.FALSE.toString(),
					"-sourcePath",
					inputPath,
					"-outputPath",
					workingDir.toString() + "/actionSet1"
				});

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaRDD<Relation> tmp = sc
			.sequenceFile(workingDir.toString() + "/actionSet1", Text.class, Text.class)
			.map(value -> OBJECT_MAPPER.readValue(value._2().toString(), AtomicAction.class))
			.map(aa -> ((Relation) aa.getPayload()));

		tmp.foreach(r -> {
			assertEquals("Web Crawl", r.getCollectedfrom().get(0).getValue());
			assertEquals("10|openaire____::fb98a192f6a055ba495ef414c330834b", r.getCollectedfrom().get(0).getKey());
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
			assertEquals(false, r.getDataInfo().getInferred());
			assertEquals(false, r.getDataInfo().getDeletedbyinference());
			assertEquals("0.91", r.getDataInfo().getTrust());
			assertEquals(
				CreateActionSetSparkJob.OPENCITATIONS_CLASSID, r.getDataInfo().getProvenanceaction().getClassid());
			assertEquals(
				CreateActionSetSparkJob.OPENCITATIONS_CLASSNAME, r.getDataInfo().getProvenanceaction().getClassname());
			assertEquals(ModelConstants.DNET_PROVENANCE_ACTIONS, r.getDataInfo().getProvenanceaction().getSchemeid());
			assertEquals(ModelConstants.DNET_PROVENANCE_ACTIONS, r.getDataInfo().getProvenanceaction().getSchemename());
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
		assertEquals(23, tmp.filter(r -> r.getRelClass().equals("Cites")).count());
		assertEquals(0, tmp.filter(r -> r.getRelClass().equals("IsCitedBy")).count());

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
			+ IdentifierFactory.md5(CleaningFunctions.normalizePidValue("doi", "10.1007/s10854-015-3684-x"));
		final String doi2 = "50|doi_________::"
			+ IdentifierFactory.md5(CleaningFunctions.normalizePidValue("doi", "10.1111/j.1551-2916.2008.02408.x"));
		final String doi3 = "50|doi_________::"
			+ IdentifierFactory.md5(CleaningFunctions.normalizePidValue("doi", "10.1007/s10854-014-2114-9"));
		final String doi4 = "50|doi_________::"
			+ IdentifierFactory.md5(CleaningFunctions.normalizePidValue("doi", "10.1016/j.ceramint.2013.09.069"));
		final String doi5 = "50|doi_________::"
			+ IdentifierFactory.md5(CleaningFunctions.normalizePidValue("doi", "10.1007/s10854-009-9913-4"));
		final String doi6 = "50|doi_________::"
			+ IdentifierFactory.md5(CleaningFunctions.normalizePidValue("doi", "10.1016/0038-1098(72)90370-5"));

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

//		check.foreach(r -> {
//			if (r.getSource().equals(doi2) || r.getSource().equals(doi3) || r.getSource().equals(doi4) ||
//				r.getSource().equals(doi5) || r.getSource().equals(doi6)) {
//				assertEquals(ModelConstants.IS_CITED_BY, r.getRelClass());
//				assertEquals(doi1, r.getTarget());
//			}
//		});

		assertEquals(5, check.filter(r -> r.getSource().equals(doi1)).count());
		check.filter(r -> r.getSource().equals(doi1)).foreach(r -> assertEquals(ModelConstants.CITES, r.getRelClass()));

	}
}
