
package eu.dnetlib.dhp.oa.graph.dump;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import eu.dnetlib.dhp.oa.graph.raw.common.VocabularyGroup;
import eu.dnetlib.dhp.schema.oaf.Dataset;
import eu.dnetlib.dhp.schema.oaf.OtherResearchProduct;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Software;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.dom4j.DocumentException;
import org.junit.jupiter.api.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DumpJobTest {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SparkSession spark;

	private static Path workingDir;

	private static String MOCK_IS_LOOK_UP_URL = "BASEURL:8280/is/services/isLookUp";

	private static final Logger log = LoggerFactory.getLogger(DumpJobTest.class);

	private static HashMap<String,String> map  = new HashMap<>();

	static{
		map.put("egi", "EGI Federation");
		map.put("fet-fp7", "FET FP7");
		map.put("fet-h2020", "FET H2020");
		map.put("clarin", "CLARIN");map.put("fam", "Fisheries and Aquaculture Management");map.put("ni", "Neuroinformatics");map.put("mes", "European Marine Scinece");map.put("instruct", "Instruct-Eric");
		map.put("rda", "Research Data Alliance");map.put("elixir-gr", "ELIXIR GR");map.put("aginfra", "Agricultural and Food Sciences");map.put("dariah", "DARIAH EU");map.put("risis", "RISI");
		map.put("ee", "SDSN - Greece");map.put("oa-pg", "EC Post-Grant Open Access Pilot");map.put("beopen", "Transport Research");map.put("euromarine", "Euromarine");map.put("ifremer", "Ifremer");
		map.put("dh-ch", "Digital Humanities and Cultural Heritage");map.put("science-innovation-policy", "Science and Innovation Policy Studies");map.put("covid-19", "COVID-19");map.put("enrmaps", "Energy Research");map.put("epos", "EPOS");


	}

	List<String> communityMap = Arrays.asList("<community id=\"egi\" label=\"EGI Federation\"/>",
			"<community id=\"fet-fp7\" label=\"FET FP7\"/>" ,
			"<community id=\"fet-h2020\" label=\"FET H2020\"/>" ,
			"<community id=\"clarin\" label=\"CLARIN\"/>",
			"<community id=\"rda\" label=\"Research Data Alliance\"/>",
			"<community id=\"ee\" label=\"SDSN - Greece\"/>",
			"<community id=\"dh-ch\" label=\"Digital Humanities and Cultural Heritage\"/>" ,
			"<community id=\"fam\" label=\"Fisheries and Aquaculture Management\"/>",
			"<community id=\"ni\" label=\"Neuroinformatics\"/>",
			"<community id=\"mes\" label=\"European Marine Science\"/>",
			"<community id=\"instruct\" label=\"Instruct-ERIC\"/>",
			"<community id=\"elixir-gr\" label=\"ELIXIR GR\"/>" ,
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

	@Mock
	private SparkDumpCommunityProducts dumpCommunityProducts;

	@Mock
	private QueryInformationSystem queryInformationSystem;

	@Mock
	private ISLookUpService isLookUpService;


	private static final String XQUERY = "for $x in collection('/db/DRIVER/ContextDSResources/ContextDSResourceType') " +
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

	@BeforeEach
	public void setUp() throws ISLookUpException {
		lenient().when(dumpCommunityProducts.getCommunityMap(MOCK_IS_LOOK_UP_URL)).thenReturn(map);
		lenient().when(queryInformationSystem.getCommunityMap(MOCK_IS_LOOK_UP_URL)).thenReturn(communityMap);
		lenient().when(isLookUpService.quickSearchProfile(XQUERY)).thenReturn(communityMap);

	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	@Test
	public void test1() throws Exception {

		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/dataset.json")
			.getPath();
		dumpCommunityProducts
			.main(
				new String[] {
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath", sourcePath,
						"-resultType","dataset",
					"-resultTableName", "eu.dnetlib.dhp.schema.oaf.Dataset",
						"-dumpTableName","eu.dnetlib.dhp.schema.dump.oaf.Dataset",
					"-outputPath", workingDir.toString() + "/dataset",
					"-isLookUpUrl", MOCK_IS_LOOK_UP_URL
				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<eu.dnetlib.dhp.schema.dump.oaf.Dataset> tmp = sc
			.textFile(workingDir.toString() + "/dataset")
			.map(item -> OBJECT_MAPPER.readValue(item, eu.dnetlib.dhp.schema.dump.oaf.Dataset.class));

		org.apache.spark.sql.Dataset<eu.dnetlib.dhp.schema.dump.oaf.Dataset> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(eu.dnetlib.dhp.schema.dump.oaf.Dataset.class));

		verificationDataset.show(false);


	}

//	@Test
//	public void bulktagBySubjectNoPreviousContextTest() throws Exception {
//		final String sourcePath = getClass()
//			.getResource("/eu/dnetlib/dhp/bulktag/sample/dataset/update_subject/nocontext")
//			.getPath();
//		final String pathMap = DumpJobTest.pathMap;
//		DumpJobTest
//			.main(
//				new String[] {
//					"-isTest", Boolean.TRUE.toString(),
//					"-isSparkSessionManaged", Boolean.FALSE.toString(),
//					"-sourcePath", sourcePath,
//					"-taggingConf", taggingConf,
//					"-resultTableName", "eu.dnetlib.dhp.schema.oaf.Dataset",
//					"-outputPath", workingDir.toString() + "/dataset",
//					"-isLookUpUrl", MOCK_IS_LOOK_UP_URL,
//					"-pathMap", pathMap
//				});
//
//		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
//
//		JavaRDD<Dataset> tmp = sc
//			.textFile(workingDir.toString() + "/dataset")
//			.map(item -> OBJECT_MAPPER.readValue(item, Dataset.class));
//
//		Assertions.assertEquals(10, tmp.count());
//		org.apache.spark.sql.Dataset<Dataset> verificationDataset = spark
//			.createDataset(tmp.rdd(), Encoders.bean(Dataset.class));
//
//		verificationDataset.createOrReplaceTempView("dataset");
//
//		String query = "select id, MyT.id community, MyD.provenanceaction.classid provenance, MyD.provenanceaction.classname name "
//			+ "from dataset "
//			+ "lateral view explode(context) c as MyT "
//			+ "lateral view explode(MyT.datainfo) d as MyD "
//			+ "where MyD.inferenceprovenance = 'bulktagging'";
//
//		Assertions.assertEquals(5, spark.sql(query).count());
//
//		org.apache.spark.sql.Dataset<Row> idExplodeCommunity = spark.sql(query);
//		Assertions
//			.assertEquals(
//				5, idExplodeCommunity.filter("provenance = 'community:subject'").count());
//		Assertions
//			.assertEquals(
//				5,
//				idExplodeCommunity.filter("name = 'Bulktagging for Community - Subject'").count());
//
//		Assertions.assertEquals(2, idExplodeCommunity.filter("community = 'covid-19'").count());
//		Assertions.assertEquals(1, idExplodeCommunity.filter("community = 'mes'").count());
//		Assertions.assertEquals(1, idExplodeCommunity.filter("community = 'fam'").count());
//		Assertions.assertEquals(1, idExplodeCommunity.filter("community = 'aginfra'").count());
//
//		Assertions
//			.assertEquals(
//				1,
//				idExplodeCommunity
//					.filter("id = '50|od______3989::02dd5d2c222191b0b9bd4f33c8e96529'")
//					.count());
//		Assertions
//			.assertEquals(
//				1,
//				idExplodeCommunity
//					.filter(
//						"community = 'covid-19' and id = '50|od______3989::02dd5d2c222191b0b9bd4f33c8e96529'")
//					.count());
//
//		Assertions
//			.assertEquals(
//				2,
//				idExplodeCommunity
//					.filter("id = '50|od______3989::05d8c751462f9bb8d2b06956dfbc5c7b'")
//					.count());
//		Assertions
//			.assertEquals(
//				2,
//				idExplodeCommunity
//					.filter(
//						"(community = 'covid-19' or community = 'aginfra') and id = '50|od______3989::05d8c751462f9bb8d2b06956dfbc5c7b'")
//					.count());
//
//		Assertions
//			.assertEquals(
//				2,
//				idExplodeCommunity
//					.filter("id = '50|od______3989::0f89464c4ac4c398fe0c71433b175a62'")
//					.count());
//		Assertions
//			.assertEquals(
//				2,
//				idExplodeCommunity
//					.filter(
//						"(community = 'mes' or community = 'fam') and id = '50|od______3989::0f89464c4ac4c398fe0c71433b175a62'")
//					.count());
//	}
//
//	@Test
//	public void bulktagBySubjectPreviousContextNoProvenanceTest() throws Exception {
//		final String sourcePath = getClass()
//			.getResource(
//				"/eu/dnetlib/dhp/bulktag/sample/dataset/update_subject/contextnoprovenance")
//			.getPath();
//		final String pathMap = BulkTagJobTest.pathMap;
//		SparkBulkTagJob
//			.main(
//				new String[] {
//					"-isTest", Boolean.TRUE.toString(),
//					"-isSparkSessionManaged", Boolean.FALSE.toString(),
//					"-sourcePath", sourcePath,
//					"-taggingConf", taggingConf,
//					"-resultTableName", "eu.dnetlib.dhp.schema.oaf.Dataset",
//					"-outputPath", workingDir.toString() + "/dataset",
//					"-isLookUpUrl", MOCK_IS_LOOK_UP_URL,
//					"-pathMap", pathMap
//				});
//
//		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
//
//		JavaRDD<Dataset> tmp = sc
//			.textFile(workingDir.toString() + "/dataset")
//			.map(item -> OBJECT_MAPPER.readValue(item, Dataset.class));
//
//		Assertions.assertEquals(10, tmp.count());
//		org.apache.spark.sql.Dataset<Dataset> verificationDataset = spark
//			.createDataset(tmp.rdd(), Encoders.bean(Dataset.class));
//
//		verificationDataset.createOrReplaceTempView("dataset");
//
//		String query = "select id, MyT.id community, MyD.provenanceaction.classid provenance "
//			+ "from dataset "
//			+ "lateral view explode(context) c as MyT "
//			+ "lateral view explode(MyT.datainfo) d as MyD "
//			+ "where MyT.id = 'covid-19' ";
//
//		Assertions.assertEquals(3, spark.sql(query).count());
//
//		org.apache.spark.sql.Dataset<Row> communityContext = spark.sql(query);
//
//		Assertions
//			.assertEquals(
//				2,
//				communityContext
//					.filter("id = '50|od______3989::02dd5d2c222191b0b9bd4f33c8e96529'")
//					.count());
//		Assertions
//			.assertEquals(
//				1,
//				communityContext
//					.filter(
//						"id = '50|od______3989::02dd5d2c222191b0b9bd4f33c8e96529' and provenance = 'community:subject'")
//					.count());
//		Assertions
//			.assertEquals(
//				1,
//				communityContext
//					.filter(
//						"id = '50|od______3989::02dd5d2c222191b0b9bd4f33c8e96529' and provenance = 'propagation:community:productsthroughsemrel'")
//					.count());
//
//		query = "select id, MyT.id community, size(MyT.datainfo) datainfosize "
//			+ "from dataset "
//			+ "lateral view explode (context) as MyT "
//			+ "where size(MyT.datainfo) > 0";
//
//		Assertions
//			.assertEquals(
//				2,
//				spark
//					.sql(query)
//					.select("datainfosize")
//					.where(
//						"id = '50|od______3989::02dd5d2c222191b0b9bd4f33c8e96529' a"
//							+ "nd community = 'covid-19'")
//					.collectAsList()
//					.get(0)
//					.getInt(0));
//	}
//
//	@Test
//	public void bulktagByDatasourceTest() throws Exception {
//		final String sourcePath = getClass()
//			.getResource("/eu/dnetlib/dhp/bulktag/sample/publication/update_datasource")
//			.getPath();
//		SparkBulkTagJob
//			.main(
//				new String[] {
//					"-isTest", Boolean.TRUE.toString(),
//					"-isSparkSessionManaged", Boolean.FALSE.toString(),
//					"-sourcePath", sourcePath,
//					"-taggingConf", taggingConf,
//					"-resultTableName", "eu.dnetlib.dhp.schema.oaf.Publication",
//					"-outputPath", workingDir.toString() + "/publication",
//					"-isLookUpUrl", MOCK_IS_LOOK_UP_URL,
//					"-pathMap", pathMap
//				});
//
//		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
//
//		JavaRDD<Publication> tmp = sc
//			.textFile(workingDir.toString() + "/publication")
//			.map(item -> OBJECT_MAPPER.readValue(item, Publication.class));
//
//		Assertions.assertEquals(10, tmp.count());
//		org.apache.spark.sql.Dataset<Publication> verificationDataset = spark
//			.createDataset(tmp.rdd(), Encoders.bean(Publication.class));
//
//		verificationDataset.createOrReplaceTempView("publication");
//
//		String query = "select id, MyT.id community, MyD.provenanceaction.classid provenance, MyD.provenanceaction.classname name "
//			+ "from publication "
//			+ "lateral view explode(context) c as MyT "
//			+ "lateral view explode(MyT.datainfo) d as MyD "
//			+ "where MyD.inferenceprovenance = 'bulktagging'";
//
//		org.apache.spark.sql.Dataset<Row> idExplodeCommunity = spark.sql(query);
//
//		Assertions.assertEquals(5, idExplodeCommunity.count());
//		Assertions
//			.assertEquals(
//				5, idExplodeCommunity.filter("provenance = 'community:datasource'").count());
//		Assertions
//			.assertEquals(
//				5,
//				idExplodeCommunity
//					.filter("name = 'Bulktagging for Community - Datasource'")
//					.count());
//
//		Assertions.assertEquals(3, idExplodeCommunity.filter("community = 'fam'").count());
//		Assertions.assertEquals(2, idExplodeCommunity.filter("community = 'aginfra'").count());
//
//		Assertions
//			.assertEquals(
//				3,
//				idExplodeCommunity
//					.filter(
//						"community = 'fam' and (id = '50|ec_fp7health::000085c89f4b96dc2269bd37edb35306' "
//							+ "or id = '50|ec_fp7health::000b9e61f83f5a4b0c35777b7bccdf38' "
//							+ "or id = '50|ec_fp7health::0010eb63e181e3e91b8b6dc6b3e1c798')")
//					.count());
//
//		Assertions
//			.assertEquals(
//				2,
//				idExplodeCommunity
//					.filter(
//						"community = 'aginfra' and (id = '50|ec_fp7health::000c8195edd542e4e64ebb32172cbf89' "
//							+ "or id = '50|ec_fp7health::0010eb63e181e3e91b8b6dc6b3e1c798')")
//					.count());
//	}
//
//	@Test
//	public void bulktagByZenodoCommunityTest() throws Exception {
//		final String sourcePath = getClass()
//			.getResource(
//				"/eu/dnetlib/dhp/bulktag/sample/otherresearchproduct/update_zenodocommunity")
//			.getPath();
//		SparkBulkTagJob
//			.main(
//				new String[] {
//					"-isTest", Boolean.TRUE.toString(),
//					"-isSparkSessionManaged", Boolean.FALSE.toString(),
//					"-sourcePath", sourcePath,
//					"-taggingConf", taggingConf,
//					"-resultTableName", "eu.dnetlib.dhp.schema.oaf.OtherResearchProduct",
//					"-outputPath", workingDir.toString() + "/orp",
//					"-isLookUpUrl", MOCK_IS_LOOK_UP_URL,
//					"-pathMap", pathMap
//				});
//
//		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
//
//		JavaRDD<OtherResearchProduct> tmp = sc
//			.textFile(workingDir.toString() + "/orp")
//			.map(item -> OBJECT_MAPPER.readValue(item, OtherResearchProduct.class));
//
//		Assertions.assertEquals(10, tmp.count());
//		org.apache.spark.sql.Dataset<OtherResearchProduct> verificationDataset = spark
//			.createDataset(tmp.rdd(), Encoders.bean(OtherResearchProduct.class));
//
//		verificationDataset.createOrReplaceTempView("orp");
//
//		String query = "select id, MyT.id community, MyD.provenanceaction.classid provenance, MyD.provenanceaction.classname name "
//			+ "from orp "
//			+ "lateral view explode(context) c as MyT "
//			+ "lateral view explode(MyT.datainfo) d as MyD "
//			+ "where MyD.inferenceprovenance = 'bulktagging'";
//
//		org.apache.spark.sql.Dataset<Row> idExplodeCommunity = spark.sql(query);
//		Assertions.assertEquals(8, idExplodeCommunity.count());
//
//		Assertions
//			.assertEquals(
//				8, idExplodeCommunity.filter("provenance = 'community:zenodocommunity'").count());
//		Assertions
//			.assertEquals(
//				8,
//				idExplodeCommunity.filter("name = 'Bulktagging for Community - Zenodo'").count());
//
//		Assertions.assertEquals(1, idExplodeCommunity.filter("community = 'covid-19'").count());
//		Assertions.assertEquals(1, idExplodeCommunity.filter("community = 'aginfra'").count());
//		Assertions.assertEquals(2, idExplodeCommunity.filter("community = 'beopen'").count());
//		Assertions.assertEquals(2, idExplodeCommunity.filter("community = 'fam'").count());
//		Assertions.assertEquals(2, idExplodeCommunity.filter("community = 'mes'").count());
//
//		Assertions
//			.assertEquals(
//				1,
//				idExplodeCommunity
//					.filter(
//						"id = '50|od______2017::0750a4d0782265873d669520f5e33c07' "
//							+ "and community = 'covid-19'")
//					.count());
//		Assertions
//			.assertEquals(
//				3,
//				idExplodeCommunity
//					.filter(
//						"id = '50|od______2017::1bd97baef19dbd2db3203b112bb83bc5' and "
//							+ "(community = 'aginfra' or community = 'mes' or community = 'fam')")
//					.count());
//		Assertions
//			.assertEquals(
//				1,
//				idExplodeCommunity
//					.filter(
//						"id = '50|od______2017::1e400f1747487fd15998735c41a55c72' "
//							+ "and community = 'beopen'")
//					.count());
//		Assertions
//			.assertEquals(
//				3,
//				idExplodeCommunity
//					.filter(
//						"id = '50|od______2017::210281c5bc1c739a11ccceeeca806396' and "
//							+ "(community = 'beopen' or community = 'fam' or community = 'mes')")
//					.count());
//
//		query = "select id, MyT.id community, size(MyT.datainfo) datainfosize "
//			+ "from orp "
//			+ "lateral view explode (context) as MyT "
//			+ "where size(MyT.datainfo) > 0";
//
//		Assertions
//			.assertEquals(
//				2,
//				spark
//					.sql(query)
//					.select("datainfosize")
//					.where(
//						"id = '50|od______2017::210281c5bc1c739a11ccceeeca806396' a"
//							+ "nd community = 'beopen'")
//					.collectAsList()
//					.get(0)
//					.getInt(0));
//
//		// verify the zenodo community context is not present anymore in the records
//		query = "select id, MyT.id community "
//			+ "from orp "
//			+ "lateral view explode(context) c as MyT "
//			+ "lateral view explode(MyT.datainfo) d as MyD ";
//
//		org.apache.spark.sql.Dataset<Row> tmp2 = spark.sql(query);
//
//		Assertions
//			.assertEquals(
//				0,
//				tmp2
//					.select("community")
//					.where(tmp2.col("community").contains(ZENODO_COMMUNITY_INDICATOR))
//					.count());
//	}
//
//	@Test
//	public void bulktagBySubjectDatasourceTest() throws Exception {
//		final String sourcePath = getClass()
//			.getResource("/eu/dnetlib/dhp/bulktag/sample/dataset/update_subject_datasource")
//			.getPath();
//		SparkBulkTagJob
//			.main(
//				new String[] {
//					"-isTest", Boolean.TRUE.toString(),
//					"-isSparkSessionManaged", Boolean.FALSE.toString(),
//					"-sourcePath", sourcePath,
//					"-taggingConf", taggingConf,
//					"-resultTableName", "eu.dnetlib.dhp.schema.oaf.Dataset",
//					"-outputPath", workingDir.toString() + "/dataset",
//					"-isLookUpUrl", MOCK_IS_LOOK_UP_URL,
//					"-pathMap", pathMap
//				});
//
//		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
//
//		JavaRDD<Dataset> tmp = sc
//			.textFile(workingDir.toString() + "/dataset")
//			.map(item -> OBJECT_MAPPER.readValue(item, Dataset.class));
//
//		Assertions.assertEquals(10, tmp.count());
//		org.apache.spark.sql.Dataset<Dataset> verificationDataset = spark
//			.createDataset(tmp.rdd(), Encoders.bean(Dataset.class));
//
//		verificationDataset.createOrReplaceTempView("dataset");
//
//		String query = "select id, MyT.id community, MyD.provenanceaction.classid provenance, MyD.provenanceaction.classname name "
//			+ "from dataset "
//			+ "lateral view explode(context) c as MyT "
//			+ "lateral view explode(MyT.datainfo) d as MyD "
//			+ "where MyD.inferenceprovenance = 'bulktagging'";
//
//		org.apache.spark.sql.Dataset<Row> idExplodeCommunity = spark.sql(query);
//		Assertions.assertEquals(7, idExplodeCommunity.count());
//
//		Assertions
//			.assertEquals(
//				5, idExplodeCommunity.filter("provenance = 'community:subject'").count());
//		Assertions
//			.assertEquals(
//				2, idExplodeCommunity.filter("provenance = 'community:datasource'").count());
//		Assertions.assertEquals(2, idExplodeCommunity.filter("community = 'covid-19'").count());
//		Assertions.assertEquals(2, idExplodeCommunity.filter("community = 'fam'").count());
//		Assertions.assertEquals(2, idExplodeCommunity.filter("community = 'aginfra'").count());
//		Assertions.assertEquals(1, idExplodeCommunity.filter("community = 'mes'").count());
//
//		query = "select id, MyT.id community, size(MyT.datainfo) datainfosize "
//			+ "from dataset "
//			+ "lateral view explode (context) as MyT "
//			+ "where size(MyT.datainfo) > 0";
//
//		org.apache.spark.sql.Dataset<Row> tmp2 = spark.sql(query);
//
//		Assertions
//			.assertEquals(
//				2,
//				tmp2
//					.select("datainfosize")
//					.where(
//						"id = '50|od______3989::05d8c751462f9bb8d2b06956dfbc5c7b' and "
//							+ "community = 'aginfra'")
//					.collectAsList()
//					.get(0)
//					.getInt(0));
//
//		Assertions
//			.assertEquals(
//				1,
//				tmp2
//					.select("datainfosize")
//					.where(
//						"id = '50|od______3989::05d8c751462f9bb8d2b06956dfbc5c7b' and "
//							+ "community = 'covid-19'")
//					.collectAsList()
//					.get(0)
//					.getInt(0));
//
//		Assertions
//			.assertEquals(
//				2,
//				tmp2
//					.select("datainfosize")
//					.where(
//						"id = '50|od______3989::02dd5d2c222191b0b9bd4f33c8e96529' and "
//							+ "community = 'fam'")
//					.collectAsList()
//					.get(0)
//					.getInt(0));
//		Assertions
//			.assertEquals(
//				2,
//				tmp2
//					.select("datainfosize")
//					.where(
//						"id = '50|od______3989::02dd5d2c222191b0b9bd4f33c8e96529' and "
//							+ "community = 'covid-19'")
//					.collectAsList()
//					.get(0)
//					.getInt(0));
//
//		Assertions
//			.assertEquals(
//				1,
//				tmp2
//					.select("datainfosize")
//					.where(
//						"id = '50|od______3989::0f89464c4ac4c398fe0c71433b175a62' and "
//							+ "community = 'fam'")
//					.collectAsList()
//					.get(0)
//					.getInt(0));
//		Assertions
//			.assertEquals(
//				1,
//				tmp2
//					.select("datainfosize")
//					.where(
//						"id = '50|od______3989::0f89464c4ac4c398fe0c71433b175a62' and "
//							+ "community = 'mes'")
//					.collectAsList()
//					.get(0)
//					.getInt(0));
//	}
//
//	@Test
//	public void bulktagBySubjectDatasourceZenodoCommunityTest() throws Exception {
//
//		SparkBulkTagJob
//			.main(
//				new String[] {
//					"-isTest", Boolean.TRUE.toString(),
//					"-isSparkSessionManaged", Boolean.FALSE.toString(),
//					"-sourcePath", getClass().getResource("/eu/dnetlib/dhp/bulktag/sample/software/").getPath(),
//					"-taggingConf", taggingConf,
//					"-resultTableName", "eu.dnetlib.dhp.schema.oaf.Software",
//					"-outputPath", workingDir.toString() + "/software",
//					"-isLookUpUrl", MOCK_IS_LOOK_UP_URL,
//					"-pathMap", pathMap
//				});
//
//		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
//
//		JavaRDD<Software> tmp = sc
//			.textFile(workingDir.toString() + "/software")
//			.map(item -> OBJECT_MAPPER.readValue(item, Software.class));
//
//		Assertions.assertEquals(10, tmp.count());
//		org.apache.spark.sql.Dataset<Software> verificationDataset = spark
//			.createDataset(tmp.rdd(), Encoders.bean(Software.class));
//
//		verificationDataset.createOrReplaceTempView("software");
//
//		String query = "select id, MyT.id community, MyD.provenanceaction.classid provenance, MyD.provenanceaction.classname name "
//			+ "from software "
//			+ "lateral view explode(context) c as MyT "
//			+ "lateral view explode(MyT.datainfo) d as MyD "
//			+ "where MyD.inferenceprovenance = 'bulktagging'";
//
//		org.apache.spark.sql.Dataset<Row> idExplodeCommunity = spark.sql(query);
//		Assertions.assertEquals(10, idExplodeCommunity.count());
//
//		idExplodeCommunity.show(false);
//		Assertions
//			.assertEquals(
//				3, idExplodeCommunity.filter("provenance = 'community:subject'").count());
//		Assertions
//			.assertEquals(
//				3, idExplodeCommunity.filter("provenance = 'community:datasource'").count());
//		Assertions
//			.assertEquals(
//				4, idExplodeCommunity.filter("provenance = 'community:zenodocommunity'").count());
//
//		Assertions.assertEquals(3, idExplodeCommunity.filter("community = 'covid-19'").count());
//		Assertions.assertEquals(1, idExplodeCommunity.filter("community = 'dh-ch'").count());
//		Assertions.assertEquals(4, idExplodeCommunity.filter("community = 'aginfra'").count());
//		Assertions.assertEquals(1, idExplodeCommunity.filter("community = 'dariah'").count());
//		Assertions.assertEquals(1, idExplodeCommunity.filter("community = 'fam'").count());
//
//		Assertions
//			.assertEquals(
//				2,
//				idExplodeCommunity
//					.filter(
//						"provenance = 'community:zenodocommunity' and "
//							+ "id = '50|od______1582::4132f5ec9496f0d6adc7b00a50a56ff4' and ("
//							+ "community = 'dh-ch' or community = 'dariah')")
//					.count());
//
//		query = "select id, MyT.id community, size(MyT.datainfo) datainfosize "
//			+ "from software "
//			+ "lateral view explode (context) as MyT "
//			+ "where size(MyT.datainfo) > 0";
//
//		org.apache.spark.sql.Dataset<Row> tmp2 = spark.sql(query);
//
//		Assertions
//			.assertEquals(
//				2,
//				tmp2
//					.select("datainfosize")
//					.where(
//						"id = '50|od______1582::501b25d420f808c8eddcd9b16e917f11' and "
//							+ "community = 'covid-19'")
//					.collectAsList()
//					.get(0)
//					.getInt(0));
//
//		Assertions
//			.assertEquals(
//				3,
//				tmp2
//					.select("datainfosize")
//					.where(
//						"id = '50|od______1582::581621232a561b7e8b4952b18b8b0e56' and "
//							+ "community = 'aginfra'")
//					.collectAsList()
//					.get(0)
//					.getInt(0));
//	}
//
//	@Test
//	public void bulktagDatasourcewithConstraintsTest() throws Exception {
//
//		final String sourcePath = getClass()
//			.getResource(
//				"/eu/dnetlib/dhp/bulktag/sample/dataset/update_datasourcewithconstraints")
//			.getPath();
//		SparkBulkTagJob
//			.main(
//				new String[] {
//					"-isTest", Boolean.TRUE.toString(),
//					"-isSparkSessionManaged", Boolean.FALSE.toString(),
//					"-sourcePath", sourcePath,
//					"-taggingConf", taggingConf,
//					"-resultTableName", "eu.dnetlib.dhp.schema.oaf.Dataset",
//					"-outputPath", workingDir.toString() + "/dataset",
//					"-isLookUpUrl", MOCK_IS_LOOK_UP_URL,
//					"-pathMap", pathMap
//				});
//
//		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
//
//		JavaRDD<Dataset> tmp = sc
//			.textFile(workingDir.toString() + "/dataset")
//			.map(item -> OBJECT_MAPPER.readValue(item, Dataset.class));
//
//		Assertions.assertEquals(10, tmp.count());
//		org.apache.spark.sql.Dataset<Dataset> verificationDataset = spark
//			.createDataset(tmp.rdd(), Encoders.bean(Dataset.class));
//
//		verificationDataset.createOrReplaceTempView("dataset");
//		String query = "select id, MyT.id community, MyD.provenanceaction.classid provenance, MyD.provenanceaction.classname name "
//			+ "from dataset "
//			+ "lateral view explode(context) c as MyT "
//			+ "lateral view explode(MyT.datainfo) d as MyD "
//			+ "where MyD.inferenceprovenance = 'bulktagging'";
//
//		org.apache.spark.sql.Dataset<Row> idExplodeCommunity = spark.sql(query);
//
//		idExplodeCommunity.show(false);
//		Assertions.assertEquals(3, idExplodeCommunity.count());
//
//		Assertions
//			.assertEquals(
//				3, idExplodeCommunity.filter("provenance = 'community:datasource'").count());
//	}
}
