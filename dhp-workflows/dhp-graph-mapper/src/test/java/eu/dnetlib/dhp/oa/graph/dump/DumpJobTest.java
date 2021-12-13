
package eu.dnetlib.dhp.oa.graph.dump;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;

import eu.dnetlib.dhp.oa.graph.dump.community.CommunityMap;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.dump.oaf.Instance;
import eu.dnetlib.dhp.schema.dump.oaf.OpenAccessRoute;
import eu.dnetlib.dhp.schema.dump.oaf.community.CommunityResult;
import eu.dnetlib.dhp.schema.dump.oaf.graph.GraphResult;
import eu.dnetlib.dhp.schema.oaf.Dataset;
import eu.dnetlib.dhp.schema.oaf.OtherResearchProduct;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Software;

//@Disabled
public class DumpJobTest {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SparkSession spark;

	private static Path workingDir;

	private static final Logger log = LoggerFactory.getLogger(DumpJobTest.class);

	private static final CommunityMap map = new CommunityMap();

	static {
		map.put("egi", "EGI Federation");
		map.put("fet-fp7", "FET FP7");
		map.put("fet-h2020", "FET H2020");
		map.put("clarin", "CLARIN");
		map.put("fam", "Fisheries and Aquaculture Management");
		map.put("ni", "Neuroinformatics");
		map.put("mes", "European Marine Scinece");
		map.put("instruct", "Instruct-Eric");
		map.put("rda", "Research Data Alliance");
		map.put("elixir-gr", "ELIXIR GR");
		map.put("aginfra", "Agricultural and Food Sciences");
		map.put("dariah", "DARIAH EU");
		map.put("risis", "RISI");
		map.put("ee", "SDSN - Greece");
		map.put("oa-pg", "EC Post-Grant Open Access Pilot");
		map.put("beopen", "Transport Research");
		map.put("euromarine", "Euromarine");
		map.put("ifremer", "Ifremer");
		map.put("dh-ch", "Digital Humanities and Cultural Heritage");
		map.put("science-innovation-policy", "Science and Innovation Policy Studies");
		map.put("covid-19", "COVID-19");
		map.put("enrmaps", "Energy Research");
		map.put("epos", "EPOS");

	}

	List<String> communityMap = Arrays
		.asList(
			"<community id=\"egi\" label=\"EGI Federation\"/>",
			"<community id=\"fet-fp7\" label=\"FET FP7\"/>",
			"<community id=\"fet-h2020\" label=\"FET H2020\"/>",
			"<community id=\"clarin\" label=\"CLARIN\"/>",
			"<community id=\"rda\" label=\"Research Data Alliance\"/>",
			"<community id=\"ee\" label=\"SDSN - Greece\"/>",
			"<community id=\"dh-ch\" label=\"Digital Humanities and Cultural Heritage\"/>",
			"<community id=\"fam\" label=\"Fisheries and Aquaculture Management\"/>",
			"<community id=\"ni\" label=\"Neuroinformatics\"/>",
			"<community id=\"mes\" label=\"European Marine Science\"/>",
			"<community id=\"instruct\" label=\"Instruct-ERIC\"/>",
			"<community id=\"elixir-gr\" label=\"ELIXIR GR\"/>",
			"<community id=\"aginfra\" label=\"Agricultural and Food Sciences\"/>",
			"<community id=\"dariah\" label=\"DARIAH EU\"/>",
			"<community id=\"risis\" label=\"RISIS\"/>",
			"<community id=\"epos\" label=\"EPOS\"/>",
			"<community id=\"beopen\" label=\"Transport Research\"/>",
			"<community id=\"euromarine\" label=\"EuroMarine\"/>",
			"<community id=\"ifremer\" label=\"Ifremer\"/>",
			"<community id=\"oa-pg\" label=\"EC Post-Grant Open Access Pilot\"/>",
			"<community id=\"science-innovation-policy\" label=\"Science and Innovation Policy Studies\"/>",
			"<community id=\"covid-19\" label=\"COVID-19\"/>",
			"<community id=\"enermaps\" label=\"Energy Research\"/>");

	private static final String XQUERY = "for $x in collection('/db/DRIVER/ContextDSResources/ContextDSResourceType') "
		+
		"  where $x//CONFIGURATION/context[./@type='community' or ./@type='ri'] " +
		"  return " +
		"<community> " +
		"{$x//CONFIGURATION/context/@id}" +
		"{$x//CONFIGURATION/context/@label}" +
		"</community>";

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files.createTempDirectory(DumpJobTest.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(DumpJobTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(DumpJobTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	@Test
	public void testMap() {
		System.out.println(new Gson().toJson(map));
	}

	@Test
	public void testPublicationDump() {
		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/resultDump/publication_extendedinstance")
			.getPath();

		final String communityMapPath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/communityMapPath/communitymap.json")
			.getPath();

		DumpProducts dump = new DumpProducts();
		dump
			.run(
				// false, sourcePath, workingDir.toString() + "/result", communityMapPath, Publication.class,
				false, sourcePath, workingDir.toString() + "/result", communityMapPath, Publication.class,
				GraphResult.class, Constants.DUMPTYPE.COMPLETE.getType());

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<GraphResult> tmp = sc
			.textFile(workingDir.toString() + "/result")
			.map(item -> OBJECT_MAPPER.readValue(item, GraphResult.class));

		org.apache.spark.sql.Dataset<GraphResult> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(GraphResult.class));

		Assertions.assertEquals(1, verificationDataset.count());

		GraphResult gr = verificationDataset.first();

		Assertions.assertEquals(1, gr.getInstance().size());

		Assertions.assertEquals(2, gr.getInstance().get(0).getMeasures().size());
		Assertions
			.assertTrue(
				gr
					.getInstance()
					.get(0)
					.getMeasures()
					.stream()
					.anyMatch(
						m -> m.getKey().equals("influence")
							&& m.getValue().equals("1.62759106106e-08")));
		Assertions
			.assertTrue(
				gr
					.getInstance()
					.get(0)
					.getMeasures()
					.stream()
					.anyMatch(
						m -> m.getKey().equals("popularity")
							&& m.getValue().equals("0.22519296")));

		Assertions.assertEquals(6, gr.getAuthor().size());
		Assertions
			.assertTrue(
				gr
					.getAuthor()
					.stream()
					.anyMatch(
						a -> a.getFullname().equals("Nikolaidou,Charitini") &&
							a.getName().equals("Charitini") && a.getSurname().equals("Nikolaidou")
							&& a.getRank() == 1 && a.getPid() == null));

		Assertions
			.assertTrue(
				gr
					.getAuthor()
					.stream()
					.anyMatch(
						a -> a.getFullname().equals("Votsi,Nefta") &&
							a.getName().equals("Nefta") && a.getSurname().equals("Votsi")
							&& a.getRank() == 2 && a.getPid().getId().getScheme().equals(ModelConstants.ORCID)
							&& a.getPid().getId().getValue().equals("0000-0001-6651-1178")
							&& a.getPid().getProvenance() != null));

		Assertions
			.assertTrue(
				gr
					.getAuthor()
					.stream()
					.anyMatch(
						a -> a.getFullname().equals("Sgardelis,Steanos") &&
							a.getName().equals("Steanos") && a.getSurname().equals("Sgardelis")
							&& a.getRank() == 3 && a.getPid().getId().getScheme().equals(ModelConstants.ORCID_PENDING)
							&& a.getPid().getId().getValue().equals("0000-0001-6651-1178")
							&& a.getPid().getProvenance() != null));

		Assertions
			.assertTrue(
				gr
					.getAuthor()
					.stream()
					.anyMatch(
						a -> a.getFullname().equals("Halley,John") &&
							a.getName().equals("John") && a.getSurname().equals("Halley")
							&& a.getRank() == 4 && a.getPid() == null));

		Assertions
			.assertTrue(
				gr
					.getAuthor()
					.stream()
					.anyMatch(
						a -> a.getFullname().equals("Pantis,John") &&
							a.getName().equals("John") && a.getSurname().equals("Pantis")
							&& a.getRank() == 5 && a.getPid().getId().getScheme().equals(ModelConstants.ORCID)
							&& a.getPid().getId().getValue().equals("0000-0001-6651-1178")
							&& a.getPid().getProvenance() != null));

		Assertions
			.assertTrue(
				gr
					.getAuthor()
					.stream()
					.anyMatch(
						a -> a.getFullname().equals("Tsiafouli,Maria") &&
							a.getName().equals("Maria") && a.getSurname().equals("Tsiafouli")
							&& a.getRank() == 6 && a.getPid().getId().getScheme().equals(ModelConstants.ORCID_PENDING)
							&& a.getPid().getId().getValue().equals("0000-0001-6651-1178")
							&& a.getPid().getProvenance() != null));

		Assertions.assertEquals("publication", gr.getType());

		Assertions.assertEquals("eng", gr.getLanguage().getCode());
		Assertions.assertEquals("English", gr.getLanguage().getLabel());

		Assertions.assertEquals(1, gr.getCountry().size());
		Assertions.assertEquals("IT", gr.getCountry().get(0).getCode());
		Assertions.assertEquals("Italy", gr.getCountry().get(0).getLabel());
		Assertions.assertTrue(gr.getCountry().get(0).getProvenance() == null);

		Assertions.assertEquals(12, gr.getSubjects().size());
		Assertions
			.assertTrue(
				gr
					.getSubjects()
					.stream()
					.anyMatch(
						s -> s.getSubject().getValue().equals("Ecosystem Services hotspots")
							&& s.getSubject().getScheme().equals("ACM") && s.getProvenance() != null &&
							s.getProvenance().getProvenance().equals("sysimport:crosswalk:repository")));
		Assertions
			.assertTrue(
				gr
					.getSubjects()
					.stream()
					.anyMatch(
						s -> s.getSubject().getValue().equals("Natura 2000")
							&& s.getSubject().getScheme().equals("") && s.getProvenance() != null &&
							s.getProvenance().getProvenance().equals("sysimport:crosswalk:repository")));

		Assertions
			.assertEquals(
				"Ecosystem Service capacity is higher in areas of multiple designation types",
				gr.getMaintitle());

		Assertions.assertEquals(null, gr.getSubtitle());

		Assertions.assertEquals(1, gr.getDescription().size());

		Assertions
			.assertTrue(
				gr
					.getDescription()
					.get(0)
					.startsWith("The implementation of the Ecosystem Service (ES) concept into practice"));
		Assertions
			.assertTrue(
				gr
					.getDescription()
					.get(0)
					.endsWith(
						"start complying with new standards and demands for nature conservation and environmental management."));

		Assertions.assertEquals("2017-01-01", gr.getPublicationdate());

		Assertions.assertEquals("Pensoft Publishers", gr.getPublisher());

		Assertions.assertEquals(null, gr.getEmbargoenddate());

		Assertions.assertEquals(1, gr.getSource().size());
		Assertions.assertEquals("One Ecosystem 2: e13718", gr.getSource().get(0));

		Assertions.assertEquals(1, gr.getFormat().size());
		Assertions.assertEquals("text/html", gr.getFormat().get(0));

		Assertions.assertEquals(0, gr.getContributor().size());

		Assertions.assertEquals(0, gr.getCoverage().size());

		Assertions.assertEquals(ModelConstants.ACCESS_RIGHT_OPEN, gr.getBestaccessright().getLabel());
		Assertions
			.assertEquals(
				Constants.accessRightsCoarMap.get(ModelConstants.ACCESS_RIGHT_OPEN), gr.getBestaccessright().getCode());

		Assertions.assertEquals("One Ecosystem", gr.getContainer().getName());
		Assertions.assertEquals("2367-8194", gr.getContainer().getIssnOnline());
		Assertions.assertEquals("", gr.getContainer().getIssnPrinted());
		Assertions.assertEquals("", gr.getContainer().getIssnLinking());

		Assertions.assertTrue(null == gr.getDocumentationUrl() || gr.getDocumentationUrl().size() == 0);

		Assertions.assertTrue(null == gr.getCodeRepositoryUrl());

		Assertions.assertEquals(null, gr.getProgrammingLanguage());

		Assertions.assertTrue(null == gr.getContactperson() || gr.getContactperson().size() == 0);

		Assertions.assertTrue(null == gr.getContactgroup() || gr.getContactgroup().size() == 0);

		Assertions.assertTrue(null == gr.getTool() || gr.getTool().size() == 0);

		Assertions.assertEquals(null, gr.getSize());

		Assertions.assertEquals(null, gr.getVersion());

		Assertions.assertTrue(null == gr.getGeolocation() || gr.getGeolocation().size() == 0);

		Assertions.assertEquals("50|pensoft_____::00ea4a1cd53806a97d62ea6bf268f2a2", gr.getId());

		System.out.println(gr.getOriginalId().size());

		Assertions.assertEquals(1, gr.getOriginalId().size());
		Assertions
			.assertTrue(
				gr.getOriginalId().contains("10.3897/oneeco.2.e13718"));

		Assertions.assertEquals(1, gr.getPid().size());
		Assertions
			.assertTrue(
				gr.getPid().get(0).getScheme().equals("doi")
					&& gr.getPid().get(0).getValue().equals("10.1016/j.triboint.2014.05.004"));

		Assertions.assertEquals("2020-03-23T00:20:51.392Z", gr.getDateofcollection());

		Assertions.assertEquals(1, gr.getInstance().size());

		Instance instance = gr.getInstance().get(0);
		Assertions.assertEquals(0, instance.getPid().size());
		Assertions.assertEquals(1, instance.getAlternateIdentifier().size());
		Assertions
			.assertTrue(
				instance.getAlternateIdentifier().get(0).getScheme().equals("doi")
					&& instance.getAlternateIdentifier().get(0).getValue().equals("10.3897/oneeco.2.e13718"));
		Assertions.assertEquals(null, instance.getLicense());
		Assertions
			.assertTrue(
				instance
					.getAccessright()
					.getCode()
					.equals(
						Constants.accessRightsCoarMap
							.get(ModelConstants.ACCESS_RIGHT_OPEN)));
		Assertions.assertTrue(instance.getAccessright().getLabel().equals(ModelConstants.ACCESS_RIGHT_OPEN));
		Assertions.assertTrue(instance.getAccessright().getOpenAccessRoute().equals(OpenAccessRoute.green));
		Assertions.assertTrue(instance.getType().equals("Article"));
		Assertions.assertEquals(2, instance.getUrl().size());
		Assertions
			.assertTrue(
				instance.getUrl().contains("https://doi.org/10.3897/oneeco.2.e13718")
					&& instance.getUrl().contains("https://oneecosystem.pensoft.net/article/13718/"));
		Assertions.assertEquals("2017-01-01", instance.getPublicationdate());
		Assertions.assertEquals(null, instance.getArticleprocessingcharge());
		Assertions.assertEquals("peerReviewed", instance.getRefereed());
	}

	@Test
	public void testDatasetDump() {
		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/resultDump/dataset_extendedinstance")
			.getPath();

		final String communityMapPath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/communityMapPath/communitymap.json")
			.getPath();

		DumpProducts dump = new DumpProducts();
		dump
			.run(
				false, sourcePath, workingDir.toString() + "/result",
				communityMapPath, Dataset.class,
				GraphResult.class, Constants.DUMPTYPE.COMPLETE.getType());

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<GraphResult> tmp = sc
			.textFile(workingDir.toString() + "/result")
			.map(item -> OBJECT_MAPPER.readValue(item, GraphResult.class));

		org.apache.spark.sql.Dataset<GraphResult> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(GraphResult.class));

		Assertions.assertEquals(1, verificationDataset.count());

		Assertions.assertEquals(1, verificationDataset.filter("type = 'dataset'").count());

		// the common fields in the result have been already checked. Now checking only
		// community specific fields

		GraphResult gr = verificationDataset.first();

		Assertions.assertEquals(2, gr.getGeolocation().size());
		Assertions.assertEquals(2, gr.getGeolocation().stream().filter(gl -> gl.getBox().equals("")).count());
		Assertions.assertEquals(1, gr.getGeolocation().stream().filter(gl -> gl.getPlace().equals("")).count());
		Assertions.assertEquals(1, gr.getGeolocation().stream().filter(gl -> gl.getPoint().equals("")).count());
		Assertions
			.assertEquals(
				1,
				gr
					.getGeolocation()
					.stream()
					.filter(gl -> gl.getPlace().equals("18 York St, Ottawa, ON K1N 5S6; Ottawa; Ontario; Canada"))
					.count());
		Assertions
			.assertEquals(
				1, gr.getGeolocation().stream().filter(gl -> gl.getPoint().equals("45.427242 -75.693904")).count());
		Assertions
			.assertEquals(
				1,
				gr
					.getGeolocation()
					.stream()
					.filter(gl -> gl.getPoint().equals("") && !gl.getPlace().equals(""))
					.count());
		Assertions
			.assertEquals(
				1,
				gr
					.getGeolocation()
					.stream()
					.filter(gl -> !gl.getPoint().equals("") && gl.getPlace().equals(""))
					.count());

		Assertions.assertEquals("1024Gb", gr.getSize());

		Assertions.assertEquals("1.01", gr.getVersion());

		Assertions.assertEquals(null, gr.getContainer());
		Assertions.assertEquals(null, gr.getCodeRepositoryUrl());
		Assertions.assertEquals(null, gr.getProgrammingLanguage());
		Assertions.assertEquals(null, gr.getDocumentationUrl());
		Assertions.assertEquals(null, gr.getContactperson());
		Assertions.assertEquals(null, gr.getContactgroup());
		Assertions.assertEquals(null, gr.getTool());

	}

	@Test
	public void testSoftwareDump() {
		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/resultDump/software_extendedinstance")
			.getPath();

		final String communityMapPath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/communityMapPath/communitymap.json")
			.getPath();

		DumpProducts dump = new DumpProducts();
		dump
			.run(
				false, sourcePath, workingDir.toString() + "/result",
				communityMapPath, Software.class,
				GraphResult.class, Constants.DUMPTYPE.COMPLETE.getType());

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<GraphResult> tmp = sc
			.textFile(workingDir.toString() + "/result")
			.map(item -> OBJECT_MAPPER.readValue(item, GraphResult.class));

		org.apache.spark.sql.Dataset<GraphResult> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(GraphResult.class));

		Assertions.assertEquals(1, verificationDataset.count());

		Assertions.assertEquals(1, verificationDataset.filter("type = 'software'").count());

		GraphResult gr = verificationDataset.first();

		Assertions.assertEquals(2, gr.getDocumentationUrl().size());
		Assertions.assertTrue(gr.getDocumentationUrl().contains("doc_url_1"));
		Assertions.assertTrue(gr.getDocumentationUrl().contains("doc_url_2"));

		Assertions.assertEquals("code_repo", gr.getCodeRepositoryUrl());

		Assertions.assertEquals("perl", gr.getProgrammingLanguage());

		Assertions.assertEquals(null, gr.getContainer());
		Assertions.assertEquals(null, gr.getContactperson());
		Assertions.assertEquals(null, gr.getContactgroup());
		Assertions.assertEquals(null, gr.getTool());
		Assertions.assertEquals(null, gr.getGeolocation());
		Assertions.assertEquals(null, gr.getSize());
		Assertions.assertEquals(null, gr.getVersion());

	}

	@Test
	public void testOrpDump() {
		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/resultDump/orp_extendedinstance")
			.getPath();

		final String communityMapPath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/communityMapPath/communitymap.json")
			.getPath();

		DumpProducts dump = new DumpProducts();
		dump
			.run(
				false, sourcePath, workingDir.toString() + "/result",
				communityMapPath, OtherResearchProduct.class,
				GraphResult.class, Constants.DUMPTYPE.COMPLETE.getType());

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<GraphResult> tmp = sc
			.textFile(workingDir.toString() + "/result")
			.map(item -> OBJECT_MAPPER.readValue(item, GraphResult.class));

		org.apache.spark.sql.Dataset<GraphResult> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(GraphResult.class));

		Assertions.assertEquals(1, verificationDataset.count());

		Assertions.assertEquals(1, verificationDataset.filter("type = 'other'").count());

		GraphResult gr = verificationDataset.first();

		Assertions.assertEquals(2, gr.getContactperson().size());
		Assertions.assertTrue(gr.getContactperson().contains(("contact_person1")));
		Assertions.assertTrue(gr.getContactperson().contains(("contact_person2")));

		Assertions.assertEquals(1, gr.getContactgroup().size());
		Assertions.assertTrue(gr.getContactgroup().contains(("contact_group")));

		Assertions.assertEquals(2, gr.getTool().size());
		Assertions.assertTrue(gr.getTool().contains("tool1"));
		Assertions.assertTrue(gr.getTool().contains("tool2"));

		Assertions.assertEquals(null, gr.getContainer());
		Assertions.assertEquals(null, gr.getDocumentationUrl());
		Assertions.assertEquals(null, gr.getCodeRepositoryUrl());
		Assertions.assertEquals(null, gr.getProgrammingLanguage());
		Assertions.assertEquals(null, gr.getGeolocation());
		Assertions.assertEquals(null, gr.getSize());
		Assertions.assertEquals(null, gr.getVersion());

	}

	@Test
	public void testPublicationDumpCommunity() throws JsonProcessingException {

		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/resultDump/publication_extendedinstance")
			.getPath();

		final String communityMapPath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/communityMapPath/communitymap.json")
			.getPath();

		DumpProducts dump = new DumpProducts();
		dump
			.run(
				false, sourcePath, workingDir.toString() + "/result", communityMapPath, Publication.class,
				CommunityResult.class, Constants.DUMPTYPE.COMMUNITY.getType());

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<CommunityResult> tmp = sc
			.textFile(workingDir.toString() + "/result")
			.map(item -> OBJECT_MAPPER.readValue(item, CommunityResult.class));

		org.apache.spark.sql.Dataset<CommunityResult> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(CommunityResult.class));

		Assertions.assertEquals(1, verificationDataset.count());

		Assertions.assertEquals(1, verificationDataset.filter("type = 'publication'").count());

		// the common fields in the result have been already checked. Now checking only
		// community specific fields

		CommunityResult cr = verificationDataset.first();

		Assertions.assertEquals(1, cr.getContext().size());
		Assertions.assertEquals("dh-ch", cr.getContext().get(0).getCode());
		Assertions.assertEquals("Digital Humanities and Cultural Heritage", cr.getContext().get(0).getLabel());
		Assertions.assertEquals(1, cr.getContext().get(0).getProvenance().size());
		Assertions.assertEquals("Inferred by OpenAIRE", cr.getContext().get(0).getProvenance().get(0).getProvenance());
		Assertions.assertEquals("0.9", cr.getContext().get(0).getProvenance().get(0).getTrust());

		Assertions.assertEquals(1, cr.getCollectedfrom().size());
		Assertions
			.assertEquals("10|openaire____::fdc7e0400d8c1634cdaf8051dbae23db", cr.getCollectedfrom().get(0).getKey());
		Assertions.assertEquals("Pensoft", cr.getCollectedfrom().get(0).getValue());

		Assertions.assertEquals(1, cr.getInstance().size());
		Assertions
			.assertEquals(
				"10|openaire____::fdc7e0400d8c1634cdaf8051dbae23db",
				cr.getInstance().get(0).getCollectedfrom().getKey());
		Assertions.assertEquals("Pensoft", cr.getInstance().get(0).getCollectedfrom().getValue());
		Assertions
			.assertEquals(
				"10|openaire____::e707e544b9a5bd23fc27fbfa65eb60dd", cr.getInstance().get(0).getHostedby().getKey());
		Assertions.assertEquals("One Ecosystem", cr.getInstance().get(0).getHostedby().getValue());

	}

	@Test
	public void testDataset() {

		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/resultDump/dataset.json")
			.getPath();

		final String communityMapPath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/communityMapPath/communitymap.json")
			.getPath();

		DumpProducts dump = new DumpProducts();
		dump
			.run(
				false, sourcePath, workingDir.toString() + "/result", communityMapPath, Dataset.class,
				CommunityResult.class, Constants.DUMPTYPE.COMMUNITY.getType());

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<CommunityResult> tmp = sc
			.textFile(workingDir.toString() + "/result")
			.map(item -> OBJECT_MAPPER.readValue(item, CommunityResult.class));

		org.apache.spark.sql.Dataset<CommunityResult> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(CommunityResult.class));

		Assertions.assertEquals(90, verificationDataset.count());

		Assertions
			.assertTrue(
				verificationDataset.filter("bestAccessright.code = 'c_abf2'").count() == verificationDataset
					.filter("bestAccessright.code = 'c_abf2' and bestAccessright.label = 'OPEN'")
					.count());

		Assertions
			.assertTrue(
				verificationDataset.filter("bestAccessright.code = 'c_16ec'").count() == verificationDataset
					.filter("bestAccessright.code = 'c_16ec' and bestAccessright.label = 'RESTRICTED'")
					.count());

		Assertions
			.assertTrue(
				verificationDataset.filter("bestAccessright.code = 'c_14cb'").count() == verificationDataset
					.filter("bestAccessright.code = 'c_14cb' and bestAccessright.label = 'CLOSED'")
					.count());

		Assertions
			.assertTrue(
				verificationDataset.filter("bestAccessright.code = 'c_f1cf'").count() == verificationDataset
					.filter("bestAccessright.code = 'c_f1cf' and bestAccessright.label = 'EMBARGO'")
					.count());

		Assertions.assertTrue(verificationDataset.filter("size(context) > 0").count() == 90);

		Assertions.assertTrue(verificationDataset.filter("type = 'dataset'").count() == 90);

	}

	@Test
	public void testDataset2All() {

		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/resultDump/dataset_cleaned")
			.getPath();

		final String communityMapPath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/communityMapPath/communitymap.json")
			.getPath();

		DumpProducts dump = new DumpProducts();
		dump
			.run(
				// false, sourcePath, workingDir.toString() + "/result", communityMapPath, Dataset.class,
				false, sourcePath, workingDir.toString() + "/result", communityMapPath, Dataset.class,
				GraphResult.class, Constants.DUMPTYPE.COMPLETE.getType());

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<eu.dnetlib.dhp.schema.dump.oaf.graph.GraphResult> tmp = sc
			.textFile(workingDir.toString() + "/result")
			.map(item -> OBJECT_MAPPER.readValue(item, eu.dnetlib.dhp.schema.dump.oaf.graph.GraphResult.class));

		org.apache.spark.sql.Dataset<eu.dnetlib.dhp.schema.dump.oaf.graph.GraphResult> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(eu.dnetlib.dhp.schema.dump.oaf.graph.GraphResult.class));

		Assertions.assertEquals(5, verificationDataset.count());

	}

	@Test
	public void testDataset2Communities() {

		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/resultDump/dataset_cleaned")
			.getPath();

		final String communityMapPath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/communityMapPath/communitymap.json")
			.getPath();

		DumpProducts dump = new DumpProducts();
		dump
			.run(
				false, sourcePath, workingDir.toString() + "/result", communityMapPath, Dataset.class,
				CommunityResult.class, Constants.DUMPTYPE.COMMUNITY.getType());

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<CommunityResult> tmp = sc
			.textFile(workingDir.toString() + "/result")
			.map(item -> OBJECT_MAPPER.readValue(item, CommunityResult.class));

		org.apache.spark.sql.Dataset<CommunityResult> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(CommunityResult.class));

		Assertions.assertEquals(0, verificationDataset.count());

	}

	@Test
	public void testPublication() {

		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/resultDump/publication.json")
			.getPath();

		final String communityMapPath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/communityMapPath/communitymap.json")
			.getPath();

		DumpProducts dump = new DumpProducts();
		dump
			.run(
				// false, sourcePath, workingDir.toString() + "/result", communityMapPath, Publication.class,
				false, sourcePath, workingDir.toString() + "/result", communityMapPath, Publication.class,
				CommunityResult.class, Constants.DUMPTYPE.COMMUNITY.getType());

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<CommunityResult> tmp = sc
			.textFile(workingDir.toString() + "/result")
			.map(item -> OBJECT_MAPPER.readValue(item, CommunityResult.class));

		org.apache.spark.sql.Dataset<CommunityResult> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(CommunityResult.class));

		Assertions.assertEquals(74, verificationDataset.count());
		verificationDataset.show(false);

		Assertions.assertEquals(74, verificationDataset.filter("type = 'publication'").count());

	}

	@Test
	public void testSoftware() {

		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/resultDump/software.json")
			.getPath();

		final String communityMapPath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/communityMapPath/communitymap.json")
			.getPath();

		DumpProducts dump = new DumpProducts();
		dump
			.run(
				// false, sourcePath, workingDir.toString() + "/result", communityMapPath, Software.class,
				false, sourcePath, workingDir.toString() + "/result", communityMapPath, Software.class,
				CommunityResult.class, Constants.DUMPTYPE.COMMUNITY.getType());

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<CommunityResult> tmp = sc
			.textFile(workingDir.toString() + "/result")
			.map(item -> OBJECT_MAPPER.readValue(item, CommunityResult.class));

		org.apache.spark.sql.Dataset<CommunityResult> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(CommunityResult.class));

		Assertions.assertEquals(6, verificationDataset.count());

		Assertions.assertEquals(6, verificationDataset.filter("type = 'software'").count());

	}

	@Test
	public void testORP() {

		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/resultDump/orp.json")
			.getPath();

		final String communityMapPath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/communityMapPath/communitymap.json")
			.getPath();

		DumpProducts dump = new DumpProducts();
		dump
			.run(
				// false, sourcePath, workingDir.toString() + "/result", communityMapPath, OtherResearchProduct.class,
				false, sourcePath, workingDir.toString() + "/result", communityMapPath, OtherResearchProduct.class,
				CommunityResult.class, Constants.DUMPTYPE.COMMUNITY.getType());

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<CommunityResult> tmp = sc
			.textFile(workingDir.toString() + "/result")
			.map(item -> OBJECT_MAPPER.readValue(item, CommunityResult.class));

		org.apache.spark.sql.Dataset<CommunityResult> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(CommunityResult.class));

		Assertions.assertEquals(3, verificationDataset.count());

		Assertions.assertEquals(3, verificationDataset.filter("type = 'other'").count());

	}

	@Test
	public void testRecord() {
		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/resultDump/singelRecord_pub.json")
			.getPath();

		final String communityMapPath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/communityMapPath/communitymap.json")
			.getPath();

		DumpProducts dump = new DumpProducts();
		dump
			.run(
				false, sourcePath, workingDir.toString() + "/result", communityMapPath, Publication.class,
				CommunityResult.class, Constants.DUMPTYPE.COMMUNITY.getType());

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<CommunityResult> tmp = sc
			.textFile(workingDir.toString() + "/result")
			.map(item -> OBJECT_MAPPER.readValue(item, CommunityResult.class));

		org.apache.spark.sql.Dataset<CommunityResult> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(CommunityResult.class));

		Assertions.assertEquals(2, verificationDataset.count());
		verificationDataset.show(false);

		Assertions.assertEquals(2, verificationDataset.filter("type = 'publication'").count());

	}

	@Test
	public void testArticlePCA() {
		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/resultDump/publication_pca")
			.getPath();

		final String communityMapPath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/communityMapPath/communitymap.json")
			.getPath();

		DumpProducts dump = new DumpProducts();
		dump
			.run(
				false, sourcePath, workingDir.toString() + "/result", communityMapPath, Publication.class,
				GraphResult.class, Constants.DUMPTYPE.COMPLETE.getType());

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<GraphResult> tmp = sc
			.textFile(workingDir.toString() + "/result")
			.map(item -> OBJECT_MAPPER.readValue(item, GraphResult.class));

		org.apache.spark.sql.Dataset<GraphResult> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(GraphResult.class));

		Assertions.assertEquals(23, verificationDataset.count());

		Assertions.assertEquals(23, verificationDataset.filter("type = 'publication'").count());

		verificationDataset.createOrReplaceTempView("check");

		org.apache.spark.sql.Dataset<Row> temp = spark
			.sql(
				"select id " +
					"from check " +
					"lateral view explode (instance) i as inst " +
					"where inst.articleprocessingcharge is not null");

		Assertions.assertTrue(temp.count() == 2);

		Assertions.assertTrue(temp.filter("id = '50|datacite____::05c611fdfc93d7a2a703d1324e28104a'").count() == 1);

		Assertions.assertTrue(temp.filter("id = '50|dedup_wf_001::01e6a28565ca01376b7548e530c6f6e8'").count() == 1);

		temp = spark
			.sql(
				"select id, inst.articleprocessingcharge.amount, inst.articleprocessingcharge.currency " +
					"from check " +
					"lateral view explode (instance) i as inst " +
					"where inst.articleprocessingcharge is not null");

		Assertions
			.assertEquals(
				"3131.64",
				temp
					.filter("id = '50|datacite____::05c611fdfc93d7a2a703d1324e28104a'")
					.collectAsList()
					.get(0)
					.getString(1));
		Assertions
			.assertEquals(
				"EUR",
				temp
					.filter("id = '50|datacite____::05c611fdfc93d7a2a703d1324e28104a'")
					.collectAsList()
					.get(0)
					.getString(2));

		Assertions
			.assertEquals(
				"2578.35",
				temp
					.filter("id = '50|dedup_wf_001::01e6a28565ca01376b7548e530c6f6e8'")
					.collectAsList()
					.get(0)
					.getString(1));
		Assertions
			.assertEquals(
				"EUR",
				temp
					.filter("id = '50|dedup_wf_001::01e6a28565ca01376b7548e530c6f6e8'")
					.collectAsList()
					.get(0)
					.getString(2));
	}

}
