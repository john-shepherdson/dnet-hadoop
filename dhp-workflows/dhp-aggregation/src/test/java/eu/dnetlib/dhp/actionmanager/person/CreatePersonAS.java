
package eu.dnetlib.dhp.actionmanager.person;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import eu.dnetlib.dhp.schema.oaf.rel.AuthorAffiliation;
import eu.dnetlib.dhp.schema.oaf.rel.Authorship;
import eu.dnetlib.dhp.schema.oaf.rel.CoAuthorship;
import eu.dnetlib.dhp.schema.oaf.rel.beans.AuthorshipRoles;
import eu.dnetlib.dhp.schema.oaf.rel.beans.DeclaredAffiliation;
import eu.dnetlib.dhp.schema.oaf.rel.beans.MatchingOrganization;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.actionmanager.personentity.ExtractPerson;
import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.Person;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.utils.DHPUtils;

public class CreatePersonAS {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SparkSession spark;

	private static Path workingDir;
	private static final Logger log = LoggerFactory
		.getLogger(CreatePersonAS.class);

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files
			.createTempDirectory(CreatePersonAS.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(CreatePersonAS.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.codegen.wholeStage", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(CreatePersonAS.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	@Test
	void testAuthorship() throws Exception {

		String inputPath = getClass()
				.getResource(
						"/eu/dnetlib/dhp/actionmanager/person/")
				.getPath();

		ExtractPerson
				.main(
						new String[] {
								"-isSparkSessionManaged",
								Boolean.FALSE.toString(),
								"-inputPath",
								inputPath,
								"-outputPath",
								workingDir.toString() + "/actionSet1",
								"-workingDir",
								workingDir.toString() + "/working",
								"-postgresUrl", "noneed",
								"-postgresUser", "noneed",
								"-postgresPassword", "noneed",
								"-publisherInputPath", getClass()
								.getResource("/eu/dnetlib/dhp/actionmanager/personpublisher/ieee.json")
								.getPath()

						});

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaRDD<Authorship> relations = sc
				.sequenceFile(workingDir.toString() + "/actionSet1", Text.class, Text.class)
				.filter(v -> "eu.dnetlib.dhp.schema.oaf.rel.Authorship".equalsIgnoreCase(v._1().toString()))
				.map(value -> OBJECT_MAPPER.readValue(value._2().toString(), AtomicAction.class))
				.map(aa -> ((Authorship) aa.getPayload()));

		Assertions.assertEquals(1, relations.count());

		Authorship authorship = relations.first();
		Assertions.assertEquals("50|doi_________::0000ff82d4cf57dc2c7c8f9b4c6b593d", authorship.getProduct());
		Assertions.assertEquals("30|orcid_______::" + DHPUtils.md5("0000-0002-5534-7920"), authorship.getPerson());
		Assertions.assertEquals(0, authorship.getRoles().size());
		Assertions.assertEquals(1, authorship.getDeclaredAffiliations().size());
		Assertions.assertFalse(authorship.getCorresponding());

		DeclaredAffiliation declaredAffiliation = authorship.getDeclaredAffiliations().get(0);
		Assertions.assertEquals("Architecture, Energy and Environment Research Group, Faculty of Engineering, University of Nottingham, University Park, Nottingham NG7 2RD, UK", declaredAffiliation.getRawAffiliation());
		Assertions.assertEquals(1, declaredAffiliation.getMatchingOrganization().size());

		MatchingOrganization matchingOrganization = declaredAffiliation.getMatchingOrganization().get(0);
		Assertions.assertEquals("https://ror.org/01ee9ar58", matchingOrganization.getRor());
		Assertions.assertNull(matchingOrganization.getOpenOrgs());
		Assertions.assertEquals(1, matchingOrganization.getTrust());
		Assertions.assertEquals("affro", matchingOrganization.getProvenance());
		Assertions.assertEquals("united kingdom", matchingOrganization.getCountry());
		Assertions.assertEquals("University of Nottingham", matchingOrganization.getResolvedOrganizationName());

	}

	@Test
	void testAuthorship2() throws Exception {

		String inputPath = getClass()
				.getResource(
						"/eu/dnetlib/dhp/actionmanager/person/")
				.getPath();

		ExtractPerson
				.main(
						new String[] {
								"-isSparkSessionManaged",
								Boolean.FALSE.toString(),
								"-inputPath",
								inputPath,
								"-outputPath",
								workingDir.toString() + "/actionSet1",
								"-workingDir",
								workingDir.toString() + "/working",
								"-postgresUrl", "noneed",
								"-postgresUser", "noneed",
								"-postgresPassword", "noneed",
								"-publisherInputPath", getClass()
								.getResource("/eu/dnetlib/dhp/actionmanager/personpublisher/ieee2.json")
								.getPath()

						});

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaRDD<Authorship> relations = sc
				.sequenceFile(workingDir.toString() + "/actionSet1", Text.class, Text.class)
				.filter(v -> "eu.dnetlib.dhp.schema.oaf.rel.Authorship".equalsIgnoreCase(v._1().toString()))
				.map(value -> OBJECT_MAPPER.readValue(value._2().toString(), AtomicAction.class))
				.map(aa -> ((Authorship) aa.getPayload()));

		Assertions.assertEquals(1, relations.count());

		Authorship authorship = relations.first();
		Assertions.assertEquals("50|doi_________::0000ff82d4cf57dc2c7c8f9b4c6b593d", authorship.getProduct());
		Assertions.assertEquals("30|orcid_______::" + DHPUtils.md5("0000-0002-5534-7920"), authorship.getPerson());
		Assertions.assertEquals(0, authorship.getRoles().size());
		Assertions.assertEquals(1, authorship.getDeclaredAffiliations().size());
		Assertions.assertFalse(authorship.getCorresponding());

		DeclaredAffiliation declaredAffiliation = authorship.getDeclaredAffiliations().get(0);
		Assertions.assertEquals("Architecture, Energy and Environment Research Group, Faculty of Engineering, University of Nottingham, University Park, Nottingham NG7 2RD, UK", declaredAffiliation.getRawAffiliation());
		Assertions.assertEquals(1, declaredAffiliation.getMatchingOrganization().size());

		MatchingOrganization matchingOrganization = declaredAffiliation.getMatchingOrganization().get(0);
		Assertions.assertEquals("https://ror.org/01ee9ar58", matchingOrganization.getRor());
		Assertions.assertNull(matchingOrganization.getOpenOrgs());
		Assertions.assertEquals(1, matchingOrganization.getTrust());
		Assertions.assertEquals("affro", matchingOrganization.getProvenance());
		Assertions.assertEquals("united kingdom", matchingOrganization.getCountry());
		Assertions.assertEquals("University of Nottingham", matchingOrganization.getResolvedOrganizationName());

	}

	@Test
	void testAuthorship3() throws Exception {

		String inputPath = getClass()
				.getResource(
						"/eu/dnetlib/dhp/actionmanager/person/")
				.getPath();

		ExtractPerson
				.main(
						new String[] {
								"-isSparkSessionManaged",
								Boolean.FALSE.toString(),
								"-inputPath",
								inputPath,
								"-outputPath",
								workingDir.toString() + "/actionSet1",
								"-workingDir",
								workingDir.toString() + "/working",
								"-postgresUrl", "noneed",
								"-postgresUser", "noneed",
								"-postgresPassword", "noneed",
								"-publisherInputPath", getClass()
								.getResource("/eu/dnetlib/dhp/actionmanager/personpublisher/ieee3.json")
								.getPath()

						});

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaRDD<Authorship> relations = sc
				.sequenceFile(workingDir.toString() + "/actionSet1", Text.class, Text.class)
				.filter(v -> "eu.dnetlib.dhp.schema.oaf.rel.Authorship".equalsIgnoreCase(v._1().toString()))
				.map(value -> OBJECT_MAPPER.readValue(value._2().toString(), AtomicAction.class))
				.map(aa -> ((Authorship) aa.getPayload()));

		Assertions.assertEquals(1, relations.count());

		Authorship authorship = relations.first();
		Assertions.assertEquals("50|doi_________::0000ff82d4cf57dc2c7c8f9b4c6b593d", authorship.getProduct());
		Assertions.assertEquals("30|orcid_______::" + DHPUtils.md5("0000-0002-5534-7920"), authorship.getPerson());
		Assertions.assertEquals(0, authorship.getRoles().size());
		Assertions.assertEquals(2, authorship.getDeclaredAffiliations().size());
		Assertions.assertFalse(authorship.getCorresponding());

		Assertions.assertEquals(1, authorship.getDeclaredAffiliations().stream().filter(da -> "Architecture, Energy and Environment Research Group, Faculty of Engineering, University of Nottingham, University Park, Nottingham NG7 2RD, UK".equals(da.getRawAffiliation())).findFirst().get().getMatchingOrganization().size());
		MatchingOrganization matchingOrganization = authorship.getDeclaredAffiliations().stream().filter(da -> "Architecture, Energy and Environment Research Group, Faculty of Engineering, University of Nottingham, University Park, Nottingham NG7 2RD, UK".equals(da.getRawAffiliation())).findFirst().get().getMatchingOrganization().get(0);
		Assertions.assertEquals("https://ror.org/01ee9ar58", matchingOrganization.getRor());
		Assertions.assertNull(matchingOrganization.getOpenOrgs());
		Assertions.assertEquals(1, matchingOrganization.getTrust());
		Assertions.assertEquals("affro", matchingOrganization.getProvenance());
		Assertions.assertEquals("united kingdom", matchingOrganization.getCountry());
		Assertions.assertEquals("University of Nottingham", matchingOrganization.getResolvedOrganizationName());

		Assertions.assertEquals(1, authorship.getDeclaredAffiliations().stream().filter(da -> "Istanbul Aydin University Faculty of Engineering, Mechanical Engineering Department, TR-34668 Istanbul, Turkey".equals(da.getRawAffiliation())).findFirst().get().getMatchingOrganization().size());
		matchingOrganization = authorship.getDeclaredAffiliations().stream().filter(da -> "Istanbul Aydin University Faculty of Engineering, Mechanical Engineering Department, TR-34668 Istanbul, Turkey".equals(da.getRawAffiliation())).findFirst().get().getMatchingOrganization().get(0);
		Assertions.assertEquals("https://ror.org/00qsyw664", matchingOrganization.getRor());
		Assertions.assertNull(matchingOrganization.getOpenOrgs());
		Assertions.assertEquals(0.7745966692414834, matchingOrganization.getTrust());
		Assertions.assertEquals("affro", matchingOrganization.getProvenance());
		Assertions.assertEquals("turkey", matchingOrganization.getCountry());
		Assertions.assertEquals("Istanbul Aydın University", matchingOrganization.getResolvedOrganizationName());

	}

	@Test
	void testAuthorship4() throws Exception {

		String inputPath = getClass()
				.getResource(
						"/eu/dnetlib/dhp/actionmanager/person/")
				.getPath();

		ExtractPerson
				.main(
						new String[] {
								"-isSparkSessionManaged",
								Boolean.FALSE.toString(),
								"-inputPath",
								inputPath,
								"-outputPath",
								workingDir.toString() + "/actionSet1",
								"-workingDir",
								workingDir.toString() + "/working",
								"-postgresUrl", "noneed",
								"-postgresUser", "noneed",
								"-postgresPassword", "noneed",
								"-publisherInputPath", getClass()
								.getResource("/eu/dnetlib/dhp/actionmanager/personpublisher/ieee4.json")
								.getPath()

						});

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaRDD<Authorship> relations = sc
				.sequenceFile(workingDir.toString() + "/actionSet1", Text.class, Text.class)
				.filter(v -> "eu.dnetlib.dhp.schema.oaf.rel.Authorship".equalsIgnoreCase(v._1().toString()))
				.map(value -> OBJECT_MAPPER.readValue(value._2().toString(), AtomicAction.class))
				.map(aa -> ((Authorship) aa.getPayload()));

		Assertions.assertEquals(1, relations.count());

		Authorship authorship = relations.first();
		Assertions.assertEquals("50|doi_________::0000ff82d4cf57dc2c7c8f9b4c6b593d", authorship.getProduct());
		Assertions.assertEquals("30|orcid_______::" + DHPUtils.md5("0000-0002-5534-7920"), authorship.getPerson());
		Assertions.assertEquals(0, authorship.getRoles().size());
		Assertions.assertEquals(2, authorship.getDeclaredAffiliations().size());
		Assertions.assertFalse(authorship.getCorresponding());

		Assertions.assertEquals(2, authorship.getDeclaredAffiliations().stream().filter(da -> "Architecture, Energy and Environment Research Group, Faculty of Engineering, University of Nottingham, University Park, Nottingham NG7 2RD, UK".equals(da.getRawAffiliation())).findFirst().get().getMatchingOrganization().size());
		List<MatchingOrganization> matchingOrganizations = authorship.getDeclaredAffiliations().stream().filter(da -> "Architecture, Energy and Environment Research Group, Faculty of Engineering, University of Nottingham, University Park, Nottingham NG7 2RD, UK".equals(da.getRawAffiliation())).findFirst().get().getMatchingOrganization();

		MatchingOrganization matchingOrganization = matchingOrganizations.stream().filter(mo -> "https://ror.org/01ee9ar58".equalsIgnoreCase(mo.getRor())).findFirst().get();
		Assertions.assertNull(matchingOrganization.getOpenOrgs());
		Assertions.assertEquals(1, matchingOrganization.getTrust());
		Assertions.assertEquals("affro", matchingOrganization.getProvenance());
		Assertions.assertEquals("united kingdom", matchingOrganization.getCountry());
		Assertions.assertEquals("University of Nottingham", matchingOrganization.getResolvedOrganizationName());

		matchingOrganization= matchingOrganizations.stream().filter(mo -> !"https://ror.org/01ee9ar58".equalsIgnoreCase(mo.getRor())).findFirst().get();
		Assertions.assertEquals("https://ror.org/00excyz84", matchingOrganization.getRor());
		Assertions.assertNull(matchingOrganization.getOpenOrgs());
		Assertions.assertEquals(1, matchingOrganization.getTrust());
		Assertions.assertEquals("affro", matchingOrganization.getProvenance());
		Assertions.assertEquals("cyprus", matchingOrganization.getCountry());
		Assertions.assertEquals("Eastern Mediterranean University", matchingOrganization.getResolvedOrganizationName());

		Assertions.assertEquals(1, authorship.getDeclaredAffiliations().stream().filter(da -> "Istanbul Aydin University Faculty of Engineering, Mechanical Engineering Department, TR-34668 Istanbul, Turkey".equals(da.getRawAffiliation())).findFirst().get().getMatchingOrganization().size());
		matchingOrganization = authorship.getDeclaredAffiliations().stream().filter(da -> "Istanbul Aydin University Faculty of Engineering, Mechanical Engineering Department, TR-34668 Istanbul, Turkey".equals(da.getRawAffiliation())).findFirst().get().getMatchingOrganization().get(0);
		Assertions.assertEquals("https://ror.org/00qsyw664", matchingOrganization.getRor());
		Assertions.assertNull(matchingOrganization.getOpenOrgs());
		Assertions.assertEquals(0.7745966692414834, matchingOrganization.getTrust());
		Assertions.assertEquals("affro", matchingOrganization.getProvenance());
		Assertions.assertEquals("turkey", matchingOrganization.getCountry());
		Assertions.assertEquals("Istanbul Aydın University", matchingOrganization.getResolvedOrganizationName());

	}

	@Test
	void testAuthorship5() throws Exception {

		String inputPath = getClass()
				.getResource(
						"/eu/dnetlib/dhp/actionmanager/person/")
				.getPath();

		ExtractPerson
				.main(
						new String[] {
								"-isSparkSessionManaged",
								Boolean.FALSE.toString(),
								"-inputPath",
								inputPath,
								"-outputPath",
								workingDir.toString() + "/actionSet1",
								"-workingDir",
								workingDir.toString() + "/working",
								"-postgresUrl", "noneed",
								"-postgresUser", "noneed",
								"-postgresPassword", "noneed",
								"-publisherInputPath", getClass()
								.getResource("/eu/dnetlib/dhp/actionmanager/personpublisher/ieee5.json")
								.getPath()

						});

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaRDD<Authorship> relations = sc
				.sequenceFile(workingDir.toString() + "/actionSet1", Text.class, Text.class)
				.filter(v -> "eu.dnetlib.dhp.schema.oaf.rel.Authorship".equalsIgnoreCase(v._1().toString()))
				.map(value -> OBJECT_MAPPER.readValue(value._2().toString(), AtomicAction.class))
				.map(aa -> ((Authorship) aa.getPayload()));

		Assertions.assertEquals(2, relations.count());

		relations.foreach(authorship -> Assertions.assertEquals("50|doi_________::0000ff82d4cf57dc2c7c8f9b4c6b593d", authorship.getProduct()));

		Authorship authorship = relations.filter(r -> ("30|orcid_______::" + DHPUtils.md5("0000-0002-5534-7920")).equalsIgnoreCase(r.getPerson())).first();
		Assertions.assertEquals(0, authorship.getRoles().size());
		Assertions.assertEquals(2, authorship.getDeclaredAffiliations().size());
		Assertions.assertFalse(authorship.getCorresponding());

		Assertions.assertEquals(2, authorship.getDeclaredAffiliations().stream().filter(da -> "Architecture, Energy and Environment Research Group, Faculty of Engineering, University of Nottingham, University Park, Nottingham NG7 2RD, UK".equals(da.getRawAffiliation())).findFirst().get().getMatchingOrganization().size());
		List<MatchingOrganization> matchingOrganizations = authorship.getDeclaredAffiliations().stream().filter(da -> "Architecture, Energy and Environment Research Group, Faculty of Engineering, University of Nottingham, University Park, Nottingham NG7 2RD, UK".equals(da.getRawAffiliation())).findFirst().get().getMatchingOrganization();

		MatchingOrganization matchingOrganization = matchingOrganizations.stream().filter(mo -> "https://ror.org/01ee9ar58".equalsIgnoreCase(mo.getRor())).findFirst().get();
		Assertions.assertNull(matchingOrganization.getOpenOrgs());
		Assertions.assertEquals(1, matchingOrganization.getTrust());
		Assertions.assertEquals("affro", matchingOrganization.getProvenance());
		Assertions.assertEquals("united kingdom", matchingOrganization.getCountry());
		Assertions.assertEquals("University of Nottingham", matchingOrganization.getResolvedOrganizationName());

		matchingOrganization= matchingOrganizations.stream().filter(mo -> !"https://ror.org/01ee9ar58".equalsIgnoreCase(mo.getRor())).findFirst().get();
		Assertions.assertEquals("https://ror.org/00excyz84", matchingOrganization.getRor());
		Assertions.assertNull(matchingOrganization.getOpenOrgs());
		Assertions.assertEquals(1, matchingOrganization.getTrust());
		Assertions.assertEquals("affro", matchingOrganization.getProvenance());
		Assertions.assertEquals("cyprus", matchingOrganization.getCountry());
		Assertions.assertEquals("Eastern Mediterranean University", matchingOrganization.getResolvedOrganizationName());

		Assertions.assertEquals(1, authorship.getDeclaredAffiliations().stream().filter(da -> "Istanbul Aydin University Faculty of Engineering, Mechanical Engineering Department, TR-34668 Istanbul, Turkey".equals(da.getRawAffiliation())).findFirst().get().getMatchingOrganization().size());
		matchingOrganization = authorship.getDeclaredAffiliations().stream().filter(da -> "Istanbul Aydin University Faculty of Engineering, Mechanical Engineering Department, TR-34668 Istanbul, Turkey".equals(da.getRawAffiliation())).findFirst().get().getMatchingOrganization().get(0);
		Assertions.assertEquals("https://ror.org/00qsyw664", matchingOrganization.getRor());
		Assertions.assertNull(matchingOrganization.getOpenOrgs());
		Assertions.assertEquals(0.7745966692414834, matchingOrganization.getTrust());
		Assertions.assertEquals("affro", matchingOrganization.getProvenance());
		Assertions.assertEquals("turkey", matchingOrganization.getCountry());
		Assertions.assertEquals("Istanbul Aydın University", matchingOrganization.getResolvedOrganizationName());

		authorship = relations.filter(r -> !("30|orcid_______::" + DHPUtils.md5("0000-0002-5534-7920")).equalsIgnoreCase(r.getPerson())).first();
		Assertions.assertEquals("30|orcid_______::" + DHPUtils.md5("0000-0003-1981-9107"), authorship.getPerson());
		Assertions.assertEquals(0, authorship.getRoles().size());
		Assertions.assertEquals(1, authorship.getDeclaredAffiliations().size());
		Assertions.assertFalse(authorship.getCorresponding());

		DeclaredAffiliation declaredAffiliation = authorship.getDeclaredAffiliations().get(0);
		Assertions.assertEquals("Istanbul Aydin University Faculty of Engineering, Mechanical Engineering Department, TR-34668 Istanbul, Turkey", declaredAffiliation.getRawAffiliation());
		Assertions.assertEquals(1, declaredAffiliation.getMatchingOrganization().size());

		matchingOrganization = declaredAffiliation.getMatchingOrganization().get(0);
		Assertions.assertNull(matchingOrganization.getOpenOrgs());
		Assertions.assertEquals("affro", matchingOrganization.getProvenance());
		Assertions.assertNull(matchingOrganization.getOpenOrgs());
		Assertions.assertEquals(0.7745966692414834, matchingOrganization.getTrust());
		Assertions.assertEquals("affro", matchingOrganization.getProvenance());
		Assertions.assertEquals("turkey", matchingOrganization.getCountry());
		Assertions.assertEquals("Istanbul Aydın University", matchingOrganization.getResolvedOrganizationName());

	}

	@Test
	void testAuthorship6() throws Exception {

		String inputPath = getClass()
				.getResource(
						"/eu/dnetlib/dhp/actionmanager/person/")
				.getPath();

		ExtractPerson
				.main(
						new String[] {
								"-isSparkSessionManaged",
								Boolean.FALSE.toString(),
								"-inputPath",
								inputPath,
								"-outputPath",
								workingDir.toString() + "/actionSet1",
								"-workingDir",
								workingDir.toString() + "/working",
								"-postgresUrl", "noneed",
								"-postgresUser", "noneed",
								"-postgresPassword", "noneed",
								"-publisherInputPath", getClass()
								.getResource("/eu/dnetlib/dhp/actionmanager/personpublisher/ieee6.json")
								.getPath()

						});

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaRDD<Authorship> relations = sc
				.sequenceFile(workingDir.toString() + "/actionSet1", Text.class, Text.class)
				.filter(v -> "eu.dnetlib.dhp.schema.oaf.rel.Authorship".equalsIgnoreCase(v._1().toString()))
				.map(value -> OBJECT_MAPPER.readValue(value._2().toString(), AtomicAction.class))
				.map(aa -> ((Authorship) aa.getPayload()));

		Assertions.assertEquals(2, relations.count());

		Authorship authorship = relations.filter(r -> ("30|orcid_______::" + DHPUtils.md5("0000-0002-5534-7920")).equalsIgnoreCase(r.getPerson())).first();
		Assertions.assertEquals(2, authorship.getRoles().size());
		Assertions.assertEquals(2, authorship.getDeclaredAffiliations().size());
		Assertions.assertTrue(authorship.getCorresponding());

		Assertions.assertNull(authorship.getRoles().get(0).getRole());
		Assertions.assertEquals("I wrote the software for the work", authorship.getRoles().get(0).getText());
        Assertions.assertNull(authorship.getRoles().get(0).getSchema());
        Assertions.assertNull(authorship.getRoles().get(0).getValue());

		Assertions.assertEquals(AuthorshipRoles.CONCEPTUALIZATION, authorship.getRoles().get(1).getRole());
		Assertions.assertEquals("Conceptualization", authorship.getRoles().get(1).getText());
		Assertions.assertEquals("CRediT", authorship.getRoles().get(1).getSchema());
		Assertions.assertEquals("http://credit.niso.org/contributor-roles/conceptualization", authorship.getRoles().get(1).getValue());


	}

	@Test
	void testCoAuthorship() throws Exception {

		String inputPath = getClass()
				.getResource(
						"/eu/dnetlib/dhp/actionmanager/person/")
				.getPath();

		ExtractPerson
				.main(
						new String[] {
								"-isSparkSessionManaged",
								Boolean.FALSE.toString(),
								"-inputPath",
								inputPath,
								"-outputPath",
								workingDir.toString() + "/actionSet1",
								"-workingDir",
								workingDir.toString() + "/working",
								"-postgresUrl", "noneed",
								"-postgresUser", "noneed",
								"-postgresPassword", "noneed",
								"-publisherInputPath", getClass()
								.getResource("/eu/dnetlib/dhp/actionmanager/personpublisher/ieee7.json")
								.getPath()

						});

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaRDD<CoAuthorship> relations = sc
				.sequenceFile(workingDir.toString() + "/actionSet1", Text.class, Text.class)
				.filter(v -> "eu.dnetlib.dhp.schema.oaf.rel.CoAuthorship".equalsIgnoreCase(v._1().toString()))
				.map(value -> OBJECT_MAPPER.readValue(value._2().toString(), AtomicAction.class))
				.map(aa -> ((CoAuthorship) aa.getPayload()));

		Assertions.assertEquals(2, relations.count());

		relations.foreach(r -> Assertions.assertEquals(2, r.getCoauthoredProducts()));




	}

	@Test
	void testAffiliation() throws Exception {

		String inputPath = getClass()
				.getResource(
						"/eu/dnetlib/dhp/actionmanager/person/")
				.getPath();

		ExtractPerson
				.main(
						new String[] {
								"-isSparkSessionManaged",
								Boolean.FALSE.toString(),
								"-inputPath",
								inputPath,
								"-outputPath",
								workingDir.toString() + "/actionSet1",
								"-workingDir",
								workingDir.toString() + "/working",
								"-postgresUrl", "noneed",
								"-postgresUser", "noneed",
								"-postgresPassword", "noneed",
								"-publisherInputPath", getClass()
								.getResource("/eu/dnetlib/dhp/actionmanager/personpublisher/ieee7.json")
								.getPath()

						});

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaRDD<AuthorAffiliation> relations = sc
				.sequenceFile(workingDir.toString() + "/actionSet1", Text.class, Text.class)
				.filter(v -> "eu.dnetlib.dhp.schema.oaf.rel.AuthorAffiliation".equalsIgnoreCase(v._1().toString()))
				.map(value -> OBJECT_MAPPER.readValue(value._2().toString(), AtomicAction.class))
				.map(aa -> ((AuthorAffiliation) aa.getPayload()));



		Assertions.assertEquals(1, relations.count());
		Assertions.assertEquals(null, relations.first().getPeriod().get(0).getStartDate());
		Assertions.assertEquals("2010-07-01", relations.first().getPeriod().get(0).getEndDate());
		Assertions.assertEquals("30|orcid_______::" + DHPUtils.md5("0000-0001-6544-2588"), relations.first().getPerson());
		Assertions.assertEquals("20|ror_________::" + DHPUtils.md5("https://ror.org/01dw0ab98"), relations.first().getOrganization());


	}

	@Test
	void testAuthors() throws Exception {

		String inputPath = getClass()
			.getResource(
				"/eu/dnetlib/dhp/actionmanager/person/")
			.getPath();

		ExtractPerson
			.main(
				new String[] {
					"-isSparkSessionManaged",
					Boolean.FALSE.toString(),
					"-inputPath",
					inputPath,
					"-outputPath",
					workingDir.toString() + "/actionSet1",
					"-workingDir",
					workingDir.toString() + "/working",
					"-postgresUrl", "noneed",
					"-postgresUser", "noneed",
					"-postgresPassword", "noneed",
					"-publisherInputPath", getClass()
						.getResource("/eu/dnetlib/dhp/actionmanager/personpublisher/ieee.json")
						.getPath()

				});

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());


		JavaRDD<Person> people = sc
			.sequenceFile(workingDir.toString() + "/actionSet1", Text.class, Text.class)
			.filter(v -> "eu.dnetlib.dhp.schema.oaf.Person".equalsIgnoreCase(v._1().toString()))
			.map(value -> OBJECT_MAPPER.readValue(value._2().toString(), AtomicAction.class))
			.map(aa -> ((Person) aa.getPayload()));

		Assertions.assertEquals(8, people.count());
		Assertions
			.assertEquals(
				"Seda",
				people
					.filter(
						p -> p.getPid().stream().anyMatch(id -> id.getValue().equalsIgnoreCase("0000-0001-6544-2588")))
					.first()
					.getGivenName());
		Assertions
			.assertEquals(
				"Ardahan Sevgili",
				people
					.filter(
						p -> p.getPid().stream().anyMatch(id -> id.getValue().equalsIgnoreCase("0000-0001-6544-2588")))
					.first()
					.getFamilyName());
		Assertions
			.assertEquals(
				0,
				people
					.filter(
						p -> p.getPid().stream().anyMatch(id -> id.getValue().equalsIgnoreCase("0000-0001-6544-2588")))
					.first()
					.getAlternativeNames()
					.size());
		Assertions
			.assertEquals(
				2,
				people
					.filter(
						p -> p.getPid().stream().anyMatch(id -> id.getValue().equalsIgnoreCase("0000-0001-6544-2588")))
					.first()
					.getPid()
					.size());

		Assertions
			.assertTrue(
				people
					.filter(
						p -> p.getPid().stream().anyMatch(id -> id.getValue().equalsIgnoreCase("0000-0001-6544-2588")))
					.first()
					.getPid()
					.stream()
					.anyMatch(
						p -> p.getQualifier().getClassname().equalsIgnoreCase("Scopus Author ID")
							&& p.getValue().equalsIgnoreCase("57203318816")));



	}

	@Test
	void testAuthors2() throws Exception {

		String inputPath = getClass()
			.getResource(
				"/eu/dnetlib/dhp/actionmanager/person/")
			.getPath();

//		spark.read()
//				.parquet("/Users/miriam/Downloads/part-00000-761dbe11-9f51-4275-a8fd-592649f334ef-c000.snappy.parquet")
//						.write()
//								.json("/tmp/part-00000.json");

//
//		spark
//				.read()
//				.parquet(inputPath + "Authors")
//				.as(Encoders.bean(Author.class))
//						.filter((FilterFunction<Author>) a -> Optional.ofNullable(a.getOtherNames()).isPresent() &&
//								Optional.ofNullable(a.getBiography()).isPresent())
//								.write()
//										.mode(SaveMode.Overwrite)
//												.parquet(workingDir.toString() + "AuthorsSubset");

		ExtractPerson
			.main(
				new String[] {
					"-isSparkSessionManaged",
					Boolean.FALSE.toString(),
					"-inputPath",
					inputPath,
					"-outputPath",
					workingDir.toString() + "/actionSet1",
					"-workingDir",
					workingDir.toString() + "/working",
					"-postgresUrl", "noneed",
					"-postgresUser", "noneed",
					"-postgresPassword", "noneed",
					"-publisherInputPath", getClass()
						.getResource("/eu/dnetlib/dhp/actionmanager/personpublisher/noloaded.json")
						.getPath()

				});

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaRDD<Person> people = sc
			.sequenceFile(workingDir.toString() + "/actionSet1", Text.class, Text.class)
			.filter(v -> "eu.dnetlib.dhp.schema.oaf.Person".equalsIgnoreCase(v._1().toString()))
			.map(value -> OBJECT_MAPPER.readValue(value._2().toString(), AtomicAction.class))
			.map(aa -> ((Person) aa.getPayload()));

		Assertions.assertEquals(8, people.count());
		Assertions
			.assertEquals(
				"Seda",
				people
					.filter(
						p -> p.getPid().stream().anyMatch(id -> id.getValue().equalsIgnoreCase("0000-0001-6544-2588")))
					.first()
					.getGivenName());
		Assertions
			.assertEquals(
				"Ardahan Sevgili",
				people
					.filter(
						p -> p.getPid().stream().anyMatch(id -> id.getValue().equalsIgnoreCase("0000-0001-6544-2588")))
					.first()
					.getFamilyName());
		Assertions
			.assertEquals(
				0,
				people
					.filter(
						p -> p.getPid().stream().anyMatch(id -> id.getValue().equalsIgnoreCase("0000-0001-6544-2588")))
					.first()
					.getAlternativeNames()
					.size());
		Assertions
			.assertEquals(
				2,
				people
					.filter(
						p -> p.getPid().stream().anyMatch(id -> id.getValue().equalsIgnoreCase("0000-0001-6544-2588")))
					.first()
					.getPid()
					.size());

		Assertions
			.assertTrue(
				people
					.filter(
						p -> p.getPid().stream().anyMatch(id -> id.getValue().equalsIgnoreCase("0000-0001-6544-2588")))
					.first()
					.getPid()
					.stream()
					.anyMatch(
						p -> p.getQualifier().getClassname().equalsIgnoreCase("Scopus Author ID")
							&& p.getValue().equalsIgnoreCase("57203318816")));

//
//
//		JavaRDD<Relation> filterAffiliation = relations
//			.filter(r -> r.getRelClass().equalsIgnoreCase(ModelConstants.ORG_PERSON_PARTICIPATES));
//		JavaRDD<Relation> rels = filterAffiliation;
//		relations.foreach(r -> System.out.println(new ObjectMapper().writeValueAsString(r)));
//		Assertions.assertEquals(2, filterAffiliation.count());
//		Assertions
//			.assertEquals(
//				1,
//				filterAffiliation
//					.filter(r -> r.getCollectedfrom().get(0).getValue().equalsIgnoreCase("OpenAIRE"))
//					.count());
//		Assertions
//			.assertEquals(
//				1,
//				filterAffiliation
//					.filter(r -> r.getCollectedfrom().get(0).getValue().equalsIgnoreCase("ORCID"))
//					.count());

	}
}
