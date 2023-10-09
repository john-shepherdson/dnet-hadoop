
package eu.dnetlib.dhp.bulktag;

import static eu.dnetlib.dhp.bulktag.community.TaggingConstants.ZENODO_COMMUNITY_INDICATOR;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
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

import eu.dnetlib.dhp.schema.oaf.*;

public class BulkTagJobTest {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static final String MOCK_IS_LOOK_UP_URL = "BASEURL:8280/is/services/isLookUp";

	public static final String pathMap = "{ \"author\" : \"$['author'][*]['fullname']\","
		+ "  \"title\" : \"$['title'][*]['value']\","
		+ "  \"orcid\" : \"$['author'][*]['pid'][*][?(@['key']=='ORCID')]['value']\","
		+ "  \"contributor\" : \"$['contributor'][*]['value']\","
		+ "  \"description\" : \"$['description'][*]['value']\", "
		+ " \"subject\" :\"$['subject'][*]['value']\" , " +
		"\"fos\" : \"$['subject'][?(@['qualifier']['classid']=='FOS')].value\"," +
		"\"sdg\" : \"$['subject'][?(@['qualifier']['classid']=='SDG')].value\"," +
		"\"hostedby\" : \"$['instance'][*]['hostedby']['key']\" , " +
		"\"collectedfrom\" : \"$['instance'][*]['collectedfrom']['key']\"} ";

	private static SparkSession spark;

	private static Path workingDir;

	private static final Logger log = LoggerFactory.getLogger(BulkTagJobTest.class);

	private static String taggingConf = "";

	static {
		try {
			taggingConf = IOUtils
				.toString(
					BulkTagJobTest.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/bulktag/communityconfiguration/tagging_conf_remove.xml"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files.createTempDirectory(BulkTagJobTest.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(BulkTagJobTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(BulkTagJobTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	@Test
	void noUpdatesTest() throws Exception {
		final String pathMap = BulkTagJobTest.pathMap;
		SparkBulkTagJob
			.main(
				new String[] {
					"-isTest", Boolean.TRUE.toString(),
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath",
					getClass().getResource("/eu/dnetlib/dhp/bulktag/sample/dataset/no_updates").getPath(),
					"-taggingConf", taggingConf,
					"-resultTableName", "eu.dnetlib.dhp.schema.oaf.Dataset",
					"-outputPath", workingDir.toString() + "/dataset",
					"-isLookUpUrl", MOCK_IS_LOOK_UP_URL,
					"-pathMap", pathMap
				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Dataset> tmp = sc
			.textFile(workingDir.toString() + "/dataset")
			.map(item -> OBJECT_MAPPER.readValue(item, Dataset.class));

		Assertions.assertEquals(10, tmp.count());
		org.apache.spark.sql.Dataset<Dataset> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(Dataset.class));

		verificationDataset.createOrReplaceTempView("dataset");

		String query = "select id, MyT.id community "
			+ "from dataset "
			+ "lateral view explode(context) c as MyT "
			+ "lateral view explode(MyT.datainfo) d as MyD "
			+ "where MyD.inferenceprovenance = 'bulktagging'";

		Assertions.assertEquals(0, spark.sql(query).count());
	}

	@Test
	void bulktagBySubjectNoPreviousContextTest() throws Exception {
		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/bulktag/sample/dataset/update_subject/nocontext")
			.getPath();
		final String pathMap = BulkTagJobTest.pathMap;
		SparkBulkTagJob
			.main(
				new String[] {
					"-isTest", Boolean.TRUE.toString(),
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath", sourcePath,
					"-taggingConf", taggingConf,
					"-resultTableName", "eu.dnetlib.dhp.schema.oaf.Dataset",
					"-outputPath", workingDir.toString() + "/dataset",
					"-isLookUpUrl", MOCK_IS_LOOK_UP_URL,
					"-pathMap", pathMap
				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Dataset> tmp = sc
			.textFile(workingDir.toString() + "/dataset")
			.map(item -> OBJECT_MAPPER.readValue(item, Dataset.class));

		Assertions.assertEquals(10, tmp.count());
		org.apache.spark.sql.Dataset<Dataset> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(Dataset.class));

		verificationDataset.createOrReplaceTempView("dataset");

		String query = "select id, MyT.id community, MyD.provenanceaction.classid provenance, MyD.provenanceaction.classname name "
			+ "from dataset "
			+ "lateral view explode(context) c as MyT "
			+ "lateral view explode(MyT.datainfo) d as MyD "
			+ "where MyD.inferenceprovenance = 'bulktagging'";

		Assertions.assertEquals(5, spark.sql(query).count());

		org.apache.spark.sql.Dataset<Row> idExplodeCommunity = spark.sql(query);
		Assertions
			.assertEquals(
				5, idExplodeCommunity.filter("provenance = 'community:subject'").count());
		Assertions
			.assertEquals(
				5,
				idExplodeCommunity.filter("name = 'Bulktagging for Community - Subject'").count());

		Assertions.assertEquals(2, idExplodeCommunity.filter("community = 'covid-19'").count());
		Assertions.assertEquals(1, idExplodeCommunity.filter("community = 'mes'").count());
		Assertions.assertEquals(1, idExplodeCommunity.filter("community = 'fam'").count());
		Assertions.assertEquals(1, idExplodeCommunity.filter("community = 'aginfra'").count());

		Assertions
			.assertEquals(
				1,
				idExplodeCommunity
					.filter("id = '50|od______3989::02dd5d2c222191b0b9bd4f33c8e96529'")
					.count());
		Assertions
			.assertEquals(
				1,
				idExplodeCommunity
					.filter(
						"community = 'covid-19' and id = '50|od______3989::02dd5d2c222191b0b9bd4f33c8e96529'")
					.count());

		Assertions
			.assertEquals(
				2,
				idExplodeCommunity
					.filter("id = '50|od______3989::05d8c751462f9bb8d2b06956dfbc5c7b'")
					.count());
		Assertions
			.assertEquals(
				2,
				idExplodeCommunity
					.filter(
						"(community = 'covid-19' or community = 'aginfra') and id = '50|od______3989::05d8c751462f9bb8d2b06956dfbc5c7b'")
					.count());

		Assertions
			.assertEquals(
				2,
				idExplodeCommunity
					.filter("id = '50|od______3989::0f89464c4ac4c398fe0c71433b175a62'")
					.count());
		Assertions
			.assertEquals(
				2,
				idExplodeCommunity
					.filter(
						"(community = 'mes' or community = 'fam') and id = '50|od______3989::0f89464c4ac4c398fe0c71433b175a62'")
					.count());
	}

	@Test
	void bulktagBySubjectPreviousContextNoProvenanceTest() throws Exception {
		final String sourcePath = getClass()
			.getResource(
				"/eu/dnetlib/dhp/bulktag/sample/dataset/update_subject/contextnoprovenance")
			.getPath();
		final String pathMap = BulkTagJobTest.pathMap;
		SparkBulkTagJob
			.main(
				new String[] {
					"-isTest", Boolean.TRUE.toString(),
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath", sourcePath,
					"-taggingConf", taggingConf,
					"-resultTableName", "eu.dnetlib.dhp.schema.oaf.Dataset",
					"-outputPath", workingDir.toString() + "/dataset",
					"-isLookUpUrl", MOCK_IS_LOOK_UP_URL,
					"-pathMap", pathMap
				});

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaRDD<Dataset> tmp = sc
			.textFile(workingDir.toString() + "/dataset")
			.map(item -> OBJECT_MAPPER.readValue(item, Dataset.class));

		Assertions.assertEquals(10, tmp.count());
		org.apache.spark.sql.Dataset<Dataset> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(Dataset.class));

		verificationDataset.createOrReplaceTempView("dataset");

		String query = "select id, MyT.id community, MyD.provenanceaction.classid provenance "
			+ "from dataset "
			+ "lateral view explode(context) c as MyT "
			+ "lateral view explode(MyT.datainfo) d as MyD "
			+ "where MyT.id = 'covid-19' ";

		Assertions.assertEquals(3, spark.sql(query).count());

		org.apache.spark.sql.Dataset<Row> communityContext = spark.sql(query);

		Assertions
			.assertEquals(
				2,
				communityContext
					.filter("id = '50|od______3989::02dd5d2c222191b0b9bd4f33c8e96529'")
					.count());
		Assertions
			.assertEquals(
				1,
				communityContext
					.filter(
						"id = '50|od______3989::02dd5d2c222191b0b9bd4f33c8e96529' and provenance = 'community:subject'")
					.count());
		Assertions
			.assertEquals(
				1,
				communityContext
					.filter(
						"id = '50|od______3989::02dd5d2c222191b0b9bd4f33c8e96529' and provenance = 'propagation:community:productsthroughsemrel'")
					.count());

		query = "select id, MyT.id community, size(MyT.datainfo) datainfosize "
			+ "from dataset "
			+ "lateral view explode (context) as MyT "
			+ "where size(MyT.datainfo) > 0";

		Assertions
			.assertEquals(
				2,
				spark
					.sql(query)
					.select("datainfosize")
					.where(
						"id = '50|od______3989::02dd5d2c222191b0b9bd4f33c8e96529' a"
							+ "nd community = 'covid-19'")
					.collectAsList()
					.get(0)
					.getInt(0));
	}

	@Test
	void bulktagByDatasourceTest() throws Exception {
		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/bulktag/sample/publication/update_datasource")
			.getPath();
		SparkBulkTagJob
			.main(
				new String[] {
					"-isTest", Boolean.TRUE.toString(),
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath", sourcePath,
					"-taggingConf", taggingConf,
					"-resultTableName", "eu.dnetlib.dhp.schema.oaf.Publication",
					"-outputPath", workingDir.toString() + "/publication",
					"-isLookUpUrl", MOCK_IS_LOOK_UP_URL,
					"-pathMap", pathMap
				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Publication> tmp = sc
			.textFile(workingDir.toString() + "/publication")
			.map(item -> OBJECT_MAPPER.readValue(item, Publication.class));

		Assertions.assertEquals(10, tmp.count());
		org.apache.spark.sql.Dataset<Publication> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(Publication.class));

		verificationDataset.createOrReplaceTempView("publication");

		String query = "select id, MyT.id community, MyD.provenanceaction.classid provenance, MyD.provenanceaction.classname name "
			+ "from publication "
			+ "lateral view explode(context) c as MyT "
			+ "lateral view explode(MyT.datainfo) d as MyD "
			+ "where MyD.inferenceprovenance = 'bulktagging'";

		org.apache.spark.sql.Dataset<Row> idExplodeCommunity = spark.sql(query);

		Assertions.assertEquals(5, idExplodeCommunity.count());
		Assertions
			.assertEquals(
				5, idExplodeCommunity.filter("provenance = 'community:datasource'").count());
		Assertions
			.assertEquals(
				5,
				idExplodeCommunity
					.filter("name = 'Bulktagging for Community - Datasource'")
					.count());

		Assertions.assertEquals(3, idExplodeCommunity.filter("community = 'fam'").count());
		Assertions.assertEquals(2, idExplodeCommunity.filter("community = 'aginfra'").count());

		Assertions
			.assertEquals(
				3,
				idExplodeCommunity
					.filter(
						"community = 'fam' and (id = '50|ec_fp7health::000085c89f4b96dc2269bd37edb35306' "
							+ "or id = '50|ec_fp7health::000b9e61f83f5a4b0c35777b7bccdf38' "
							+ "or id = '50|ec_fp7health::0010eb63e181e3e91b8b6dc6b3e1c798')")
					.count());

		Assertions
			.assertEquals(
				2,
				idExplodeCommunity
					.filter(
						"community = 'aginfra' and (id = '50|ec_fp7health::000c8195edd542e4e64ebb32172cbf89' "
							+ "or id = '50|ec_fp7health::0010eb63e181e3e91b8b6dc6b3e1c798')")
					.count());
	}

	@Test
	void bulktagByZenodoCommunityTest() throws Exception {
		final String sourcePath = getClass()
			.getResource(
				"/eu/dnetlib/dhp/bulktag/sample/otherresearchproduct/update_zenodocommunity")
			.getPath();
		SparkBulkTagJob
			.main(
				new String[] {
					"-isTest", Boolean.TRUE.toString(),
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath", sourcePath,
					"-taggingConf", taggingConf,
					"-resultTableName", "eu.dnetlib.dhp.schema.oaf.OtherResearchProduct",
					"-outputPath", workingDir.toString() + "/orp",
					"-isLookUpUrl", MOCK_IS_LOOK_UP_URL,
					"-pathMap", pathMap
				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<OtherResearchProduct> tmp = sc
			.textFile(workingDir.toString() + "/orp")
			.map(item -> OBJECT_MAPPER.readValue(item, OtherResearchProduct.class));

		Assertions.assertEquals(10, tmp.count());
		org.apache.spark.sql.Dataset<OtherResearchProduct> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(OtherResearchProduct.class));

		verificationDataset.createOrReplaceTempView("orp");

		String query = "select id, MyT.id community, MyD.provenanceaction.classid provenance, MyD.provenanceaction.classname name "
			+ "from orp "
			+ "lateral view explode(context) c as MyT "
			+ "lateral view explode(MyT.datainfo) d as MyD "
			+ "where MyD.inferenceprovenance = 'bulktagging'";

		org.apache.spark.sql.Dataset<Row> idExplodeCommunity = spark.sql(query);
		Assertions.assertEquals(8, idExplodeCommunity.count());

		Assertions
			.assertEquals(
				8, idExplodeCommunity.filter("provenance = 'community:zenodocommunity'").count());
		Assertions
			.assertEquals(
				8,
				idExplodeCommunity.filter("name = 'Bulktagging for Community - Zenodo'").count());

		Assertions.assertEquals(1, idExplodeCommunity.filter("community = 'covid-19'").count());
		Assertions.assertEquals(1, idExplodeCommunity.filter("community = 'aginfra'").count());
		Assertions.assertEquals(2, idExplodeCommunity.filter("community = 'beopen'").count());
		Assertions.assertEquals(2, idExplodeCommunity.filter("community = 'fam'").count());
		Assertions.assertEquals(2, idExplodeCommunity.filter("community = 'mes'").count());

		Assertions
			.assertEquals(
				1,
				idExplodeCommunity
					.filter(
						"id = '50|od______2017::0750a4d0782265873d669520f5e33c07' "
							+ "and community = 'covid-19'")
					.count());
		Assertions
			.assertEquals(
				3,
				idExplodeCommunity
					.filter(
						"id = '50|od______2017::1bd97baef19dbd2db3203b112bb83bc5' and "
							+ "(community = 'aginfra' or community = 'mes' or community = 'fam')")
					.count());
		Assertions
			.assertEquals(
				1,
				idExplodeCommunity
					.filter(
						"id = '50|od______2017::1e400f1747487fd15998735c41a55c72' "
							+ "and community = 'beopen'")
					.count());
		Assertions
			.assertEquals(
				3,
				idExplodeCommunity
					.filter(
						"id = '50|od______2017::210281c5bc1c739a11ccceeeca806396' and "
							+ "(community = 'beopen' or community = 'fam' or community = 'mes')")
					.count());

		query = "select id, MyT.id community, size(MyT.datainfo) datainfosize "
			+ "from orp "
			+ "lateral view explode (context) as MyT "
			+ "where size(MyT.datainfo) > 0";

		Assertions
			.assertEquals(
				2,
				spark
					.sql(query)
					.select("datainfosize")
					.where(
						"id = '50|od______2017::210281c5bc1c739a11ccceeeca806396' a"
							+ "nd community = 'beopen'")
					.collectAsList()
					.get(0)
					.getInt(0));

		// verify the zenodo community context is not present anymore in the records
		query = "select id, MyT.id community "
			+ "from orp "
			+ "lateral view explode(context) c as MyT "
			+ "lateral view explode(MyT.datainfo) d as MyD ";

		org.apache.spark.sql.Dataset<Row> tmp2 = spark.sql(query);

		Assertions
			.assertEquals(
				0,
				tmp2
					.select("community")
					.where(tmp2.col("community").contains(ZENODO_COMMUNITY_INDICATOR))
					.count());
	}

	@Test
	void bulktagBySubjectDatasourceTest() throws Exception {
		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/bulktag/sample/dataset/update_subject_datasource")
			.getPath();
		SparkBulkTagJob
			.main(
				new String[] {
					"-isTest", Boolean.TRUE.toString(),
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath", sourcePath,
					"-taggingConf", taggingConf,
					"-resultTableName", "eu.dnetlib.dhp.schema.oaf.Dataset",
					"-outputPath", workingDir.toString() + "/dataset",
					"-isLookUpUrl", MOCK_IS_LOOK_UP_URL,
					"-pathMap", pathMap
				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Dataset> tmp = sc
			.textFile(workingDir.toString() + "/dataset")
			.map(item -> OBJECT_MAPPER.readValue(item, Dataset.class));

		Assertions.assertEquals(10, tmp.count());
		org.apache.spark.sql.Dataset<Dataset> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(Dataset.class));

		verificationDataset.createOrReplaceTempView("dataset");

		String query = "select id, MyT.id community, MyD.provenanceaction.classid provenance, MyD.provenanceaction.classname name "
			+ "from dataset "
			+ "lateral view explode(context) c as MyT "
			+ "lateral view explode(MyT.datainfo) d as MyD "
			+ "where MyD.inferenceprovenance = 'bulktagging'";

		org.apache.spark.sql.Dataset<Row> idExplodeCommunity = spark.sql(query);
		Assertions.assertEquals(7, idExplodeCommunity.count());

		Assertions
			.assertEquals(
				5, idExplodeCommunity.filter("provenance = 'community:subject'").count());
		Assertions
			.assertEquals(
				2, idExplodeCommunity.filter("provenance = 'community:datasource'").count());
		Assertions.assertEquals(2, idExplodeCommunity.filter("community = 'covid-19'").count());
		Assertions.assertEquals(2, idExplodeCommunity.filter("community = 'fam'").count());
		Assertions.assertEquals(2, idExplodeCommunity.filter("community = 'aginfra'").count());
		Assertions.assertEquals(1, idExplodeCommunity.filter("community = 'mes'").count());

		query = "select id, MyT.id community, size(MyT.datainfo) datainfosize "
			+ "from dataset "
			+ "lateral view explode (context) as MyT "
			+ "where size(MyT.datainfo) > 0";

		org.apache.spark.sql.Dataset<Row> tmp2 = spark.sql(query);

		Assertions
			.assertEquals(
				2,
				tmp2
					.select("datainfosize")
					.where(
						"id = '50|od______3989::05d8c751462f9bb8d2b06956dfbc5c7b' and "
							+ "community = 'aginfra'")
					.collectAsList()
					.get(0)
					.getInt(0));

		Assertions
			.assertEquals(
				1,
				tmp2
					.select("datainfosize")
					.where(
						"id = '50|od______3989::05d8c751462f9bb8d2b06956dfbc5c7b' and "
							+ "community = 'covid-19'")
					.collectAsList()
					.get(0)
					.getInt(0));

		Assertions
			.assertEquals(
				2,
				tmp2
					.select("datainfosize")
					.where(
						"id = '50|od______3989::02dd5d2c222191b0b9bd4f33c8e96529' and "
							+ "community = 'fam'")
					.collectAsList()
					.get(0)
					.getInt(0));
		Assertions
			.assertEquals(
				2,
				tmp2
					.select("datainfosize")
					.where(
						"id = '50|od______3989::02dd5d2c222191b0b9bd4f33c8e96529' and "
							+ "community = 'covid-19'")
					.collectAsList()
					.get(0)
					.getInt(0));

		Assertions
			.assertEquals(
				1,
				tmp2
					.select("datainfosize")
					.where(
						"id = '50|od______3989::0f89464c4ac4c398fe0c71433b175a62' and "
							+ "community = 'fam'")
					.collectAsList()
					.get(0)
					.getInt(0));
		Assertions
			.assertEquals(
				1,
				tmp2
					.select("datainfosize")
					.where(
						"id = '50|od______3989::0f89464c4ac4c398fe0c71433b175a62' and "
							+ "community = 'mes'")
					.collectAsList()
					.get(0)
					.getInt(0));
	}

	@Test
	void bulktagBySubjectDatasourceZenodoCommunityTest() throws Exception {

		SparkBulkTagJob
			.main(
				new String[] {
					"-isTest", Boolean.TRUE.toString(),
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath",
					getClass().getResource("/eu/dnetlib/dhp/bulktag/sample/software/software_10.json.gz").getPath(),
					"-taggingConf", taggingConf,
					"-resultTableName", "eu.dnetlib.dhp.schema.oaf.Software",
					"-outputPath", workingDir.toString() + "/software",
					"-isLookUpUrl", MOCK_IS_LOOK_UP_URL,
					"-pathMap", pathMap
				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Software> tmp = sc
			.textFile(workingDir.toString() + "/software")
			.map(item -> OBJECT_MAPPER.readValue(item, Software.class));

		Assertions.assertEquals(10, tmp.count());
		org.apache.spark.sql.Dataset<Software> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(Software.class));

		verificationDataset.createOrReplaceTempView("software");

		String query = "select id, MyT.id community, MyD.provenanceaction.classid provenance, MyD.provenanceaction.classname name "
			+ "from software "
			+ "lateral view explode(context) c as MyT "
			+ "lateral view explode(MyT.datainfo) d as MyD "
			+ "where MyD.inferenceprovenance = 'bulktagging'";

		org.apache.spark.sql.Dataset<Row> idExplodeCommunity = spark.sql(query);
		Assertions.assertEquals(10, idExplodeCommunity.count());

		idExplodeCommunity.show(false);
		Assertions
			.assertEquals(
				3, idExplodeCommunity.filter("provenance = 'community:subject'").count());
		Assertions
			.assertEquals(
				3, idExplodeCommunity.filter("provenance = 'community:datasource'").count());
		Assertions
			.assertEquals(
				4, idExplodeCommunity.filter("provenance = 'community:zenodocommunity'").count());

		Assertions.assertEquals(3, idExplodeCommunity.filter("community = 'covid-19'").count());
		Assertions.assertEquals(1, idExplodeCommunity.filter("community = 'dh-ch'").count());
		Assertions.assertEquals(4, idExplodeCommunity.filter("community = 'aginfra'").count());
		Assertions.assertEquals(1, idExplodeCommunity.filter("community = 'dth'").count());
		Assertions.assertEquals(1, idExplodeCommunity.filter("community = 'fam'").count());

		Assertions
			.assertEquals(
				2,
				idExplodeCommunity
					.filter(
						"provenance = 'community:zenodocommunity' and "
							+ "id = '50|od______1582::4132f5ec9496f0d6adc7b00a50a56ff4' and ("
							+ "community = 'dh-ch' or community = 'dth')")
					.count());

		query = "select id, MyT.id community, size(MyT.datainfo) datainfosize "
			+ "from software "
			+ "lateral view explode (context) as MyT "
			+ "where size(MyT.datainfo) > 0";

		org.apache.spark.sql.Dataset<Row> tmp2 = spark.sql(query);

		Assertions
			.assertEquals(
				2,
				tmp2
					.select("datainfosize")
					.where(
						"id = '50|od______1582::501b25d420f808c8eddcd9b16e917f11' and "
							+ "community = 'covid-19'")
					.collectAsList()
					.get(0)
					.getInt(0));

		Assertions
			.assertEquals(
				3,
				tmp2
					.select("datainfosize")
					.where(
						"id = '50|od______1582::581621232a561b7e8b4952b18b8b0e56' and "
							+ "community = 'aginfra'")
					.collectAsList()
					.get(0)
					.getInt(0));
	}

	@Test
	void bulktagDatasourcewithConstraintsTest() throws Exception {

		final String sourcePath = getClass()
			.getResource(
				"/eu/dnetlib/dhp/bulktag/sample/dataset/update_datasourcewithconstraints")
			.getPath();
		SparkBulkTagJob
			.main(
				new String[] {
					"-isTest", Boolean.TRUE.toString(),
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath", sourcePath,
					"-taggingConf", taggingConf,
					"-resultTableName", "eu.dnetlib.dhp.schema.oaf.Dataset",
					"-outputPath", workingDir.toString() + "/dataset",
					"-isLookUpUrl", MOCK_IS_LOOK_UP_URL,
					"-pathMap", pathMap
				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Dataset> tmp = sc
			.textFile(workingDir.toString() + "/dataset")
			.map(item -> OBJECT_MAPPER.readValue(item, Dataset.class));

		Assertions.assertEquals(12, tmp.count());
		org.apache.spark.sql.Dataset<Dataset> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(Dataset.class));

		verificationDataset.createOrReplaceTempView("dataset");
		String query = "select id, MyT.id community, MyD.provenanceaction.classid provenance, MyD.provenanceaction.classname name "
			+ "from dataset "
			+ "lateral view explode(context) c as MyT "
			+ "lateral view explode(MyT.datainfo) d as MyD "
			+ "where MyD.inferenceprovenance = 'bulktagging'";

		org.apache.spark.sql.Dataset<Row> idExplodeCommunity = spark.sql(query);

		idExplodeCommunity.show(false);

	}

	@Test
	void bulkTagOtherJupyter() throws Exception {
		final String sourcePath = getClass()
			.getResource(
				"/eu/dnetlib/dhp/eosctag/jupyter/otherresearchproduct")
			.getPath();

		SparkBulkTagJob
			.main(
				new String[] {
					"-isTest", Boolean.TRUE.toString(),
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath", sourcePath,
					"-taggingConf", taggingConf,
					"-resultTableName", "eu.dnetlib.dhp.schema.oaf.OtherResearchProduct",
					"-outputPath", workingDir.toString() + "/otherresearchproduct",
					"-isLookUpUrl", MOCK_IS_LOOK_UP_URL,
					"-pathMap", pathMap
				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		Assertions
			.assertEquals(
				10, sc
					.textFile(workingDir.toString() + "/otherresearchproduct")
					.map(item -> OBJECT_MAPPER.readValue(item, OtherResearchProduct.class))
					.count());

		Assertions
			.assertEquals(
				0, sc
					.textFile(workingDir.toString() + "/otherresearchproduct")
					.map(item -> OBJECT_MAPPER.readValue(item, OtherResearchProduct.class))
					.filter(
						orp -> orp
							.getSubject()
							.stream()
							.anyMatch(sbj -> sbj.getValue().equals("EOSC::Jupyter Notebook")))
					.count());

		Assertions
			.assertEquals(
				0, sc
					.textFile(workingDir.toString() + "/otherresearchproduct")
					.map(item -> OBJECT_MAPPER.readValue(item, OtherResearchProduct.class))
					.filter(
						orp -> orp
							.getSubject()
							.stream()
							.anyMatch(eig -> eig.getValue().equals("EOSC::Jupyter Notebook")))
					.count());

	}

	@Test
	public void bulkTagDatasetJupyter() throws Exception {
		final String sourcePath = getClass()
			.getResource(
				"/eu/dnetlib/dhp/eosctag/jupyter/dataset")
			.getPath();
		SparkBulkTagJob
			.main(
				new String[] {
					"-isTest", Boolean.TRUE.toString(),
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath", sourcePath,
					"-taggingConf", taggingConf,
					"-resultTableName", "eu.dnetlib.dhp.schema.oaf.Dataset",
					"-outputPath", workingDir.toString() + "/dataset",
					"-isLookUpUrl", MOCK_IS_LOOK_UP_URL,
					"-pathMap", pathMap
				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
		Assertions
			.assertEquals(
				10, sc
					.textFile(workingDir.toString() + "/dataset")
					.map(item -> OBJECT_MAPPER.readValue(item, Dataset.class))
					.count());

		Assertions
			.assertEquals(
				0, sc
					.textFile(workingDir.toString() + "/dataset")
					.map(item -> OBJECT_MAPPER.readValue(item, Dataset.class))
					.filter(
						ds -> ds.getSubject().stream().anyMatch(sbj -> sbj.getValue().equals("EOSC::Jupyter Notebook")))
					.count());
		Assertions
			.assertEquals(
				0, sc
					.textFile(workingDir.toString() + "/dataset")
					.map(item -> OBJECT_MAPPER.readValue(item, Dataset.class))
					.filter(
						ds -> ds
							.getEoscifguidelines()
							.stream()
							.anyMatch(eig -> eig.getCode().equals("EOSC::Jupyter Notebook")))
					.count());
	}

	@Test
	public void bulkTagSoftwareJupyter() throws Exception {

		final String sourcePath = getClass()
			.getResource(
				"/eu/dnetlib/dhp/eosctag/jupyter/software")
			.getPath();
		SparkBulkTagJob
			.main(
				new String[] {
					"-isTest", Boolean.TRUE.toString(),
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath", sourcePath,
					"-taggingConf", taggingConf,
					"-resultTableName", "eu.dnetlib.dhp.schema.oaf.Software",
					"-outputPath", workingDir.toString() + "/software",
					"-isLookUpUrl", MOCK_IS_LOOK_UP_URL,
					"-pathMap", pathMap
				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Software> tmp = sc
			.textFile(workingDir.toString() + "/software")
			.map(item -> OBJECT_MAPPER.readValue(item, Software.class));

		Assertions.assertEquals(10, tmp.count());

		Assertions
			.assertEquals(
				4,
				tmp
					.filter(s -> s.getEoscifguidelines() != null)
					.filter(
						s -> s
							.getEoscifguidelines()
							.stream()
							.anyMatch(eig -> eig.getCode().equals("EOSC::Jupyter Notebook")))
					.count());

		Assertions
			.assertEquals(
				1, tmp
					.filter(sw -> sw.getId().equals("50|od______1582::4132f5ec9496f0d6adc7b00a50a56ff4"))
					.collect()
					.get(0)
					.getEoscifguidelines()
					.size());

		Assertions
			.assertEquals(
				1, tmp
					.filter(sw -> sw.getId().equals("50|od______1582::4132f5ec9496f0d6adc7b00a50a56ff4"))
					.collect()
					.get(0)
					.getSubject()
					.size());
		Assertions
			.assertTrue(
				tmp
					.filter(sw -> sw.getId().equals("50|od______1582::4132f5ec9496f0d6adc7b00a50a56ff4"))
					.collect()
					.get(0)
					.getEoscifguidelines()
					.stream()
					.anyMatch(s -> s.getCode().equals("EOSC::Jupyter Notebook")));

		Assertions
			.assertFalse(
				tmp
					.filter(sw -> sw.getId().equals("50|od______1582::4132f5ec9496f0d6adc7b00a50a56ff4"))
					.collect()
					.get(0)
					.getSubject()
					.stream()
					.anyMatch(s -> s.getValue().equals("EOSC::Jupyter Notebook")));

		Assertions
			.assertEquals(
				5, tmp
					.filter(sw -> sw.getId().equals("50|od______1582::501b25d420f808c8eddcd9b16e917f11"))
					.collect()
					.get(0)
					.getSubject()
					.size());
		Assertions
			.assertFalse(
				tmp
					.filter(sw -> sw.getId().equals("50|od______1582::501b25d420f808c8eddcd9b16e917f11"))
					.collect()
					.get(0)
					.getSubject()
					.stream()
					.anyMatch(s -> s.getValue().equals("EOSC::Jupyter Notebook")));

		Assertions
			.assertEquals(
				0,
				tmp
					.filter(sw -> sw.getId().equals("50|od______1582::501b25d420f808c8eddcd9b16e917f11"))
					.collect()
					.get(0)
					.getEoscifguidelines()
					.size());

		Assertions
			.assertEquals(
				8, tmp
					.filter(sw -> sw.getId().equals("50|od______1582::581621232a561b7e8b4952b18b8b0e56"))
					.collect()
					.get(0)
					.getSubject()
					.size());
		Assertions
			.assertFalse(
				tmp
					.filter(sw -> sw.getId().equals("50|od______1582::581621232a561b7e8b4952b18b8b0e56"))
					.collect()
					.get(0)
					.getSubject()
					.stream()
					.anyMatch(s -> s.getValue().equals("EOSC::Jupyter Notebook")));
		Assertions
			.assertEquals(
				1, tmp
					.filter(sw -> sw.getId().equals("50|od______1582::581621232a561b7e8b4952b18b8b0e56"))
					.collect()
					.get(0)
					.getEoscifguidelines()
					.size());
		Assertions
			.assertTrue(
				tmp
					.filter(sw -> sw.getId().equals("50|od______1582::581621232a561b7e8b4952b18b8b0e56"))
					.collect()
					.get(0)
					.getEoscifguidelines()
					.stream()
					.anyMatch(s -> s.getCode().equals("EOSC::Jupyter Notebook")));

		Assertions
			.assertEquals(
				5, tmp
					.filter(sw -> sw.getId().equals("50|od______1582::5aec1186054301b66c0c5dc35972a589"))
					.collect()
					.get(0)
					.getSubject()
					.size());
		Assertions
			.assertFalse(
				tmp
					.filter(sw -> sw.getId().equals("50|od______1582::5aec1186054301b66c0c5dc35972a589"))
					.collect()
					.get(0)
					.getSubject()
					.stream()
					.anyMatch(s -> s.getValue().equals("EOSC::Jupyter Notebook")));
		Assertions
			.assertEquals(
				0,
				tmp
					.filter(sw -> sw.getId().equals("50|od______1582::5aec1186054301b66c0c5dc35972a589"))
					.collect()
					.get(0)
					.getEoscifguidelines()
					.size());

		Assertions
			.assertEquals(
				8, tmp
					.filter(sw -> sw.getId().equals("50|od______1582::639909adfad9d708308f2aedb733e4a0"))
					.collect()
					.get(0)
					.getSubject()
					.size());
		Assertions
			.assertFalse(
				tmp
					.filter(sw -> sw.getId().equals("50|od______1582::639909adfad9d708308f2aedb733e4a0"))
					.collect()
					.get(0)
					.getSubject()
					.stream()
					.anyMatch(s -> s.getValue().equals("EOSC::Jupyter Notebook")));
		Assertions
			.assertEquals(
				1,
				tmp
					.filter(sw -> sw.getId().equals("50|od______1582::639909adfad9d708308f2aedb733e4a0"))
					.collect()
					.get(0)
					.getEoscifguidelines()
					.size());
		Assertions
			.assertTrue(
				tmp
					.filter(sw -> sw.getId().equals("50|od______1582::639909adfad9d708308f2aedb733e4a0"))
					.collect()
					.get(0)
					.getEoscifguidelines()
					.stream()
					.anyMatch(s -> s.getCode().equals("EOSC::Jupyter Notebook")));

		List<Subject> subjects = tmp
			.filter(sw -> sw.getId().equals("50|od______1582::6e7a9b21a2feef45673890432af34244"))
			.collect()
			.get(0)
			.getSubject();
		Assertions.assertEquals(7, subjects.size());
		Assertions.assertTrue(subjects.stream().anyMatch(s -> s.getValue().equals("jupyter")));
		Assertions.assertTrue(subjects.stream().anyMatch(s -> s.getValue().equals("Modeling and Simulation")));
		Assertions.assertTrue(subjects.stream().anyMatch(s -> s.getValue().equals("structure granulaire")));
		Assertions.assertTrue(subjects.stream().anyMatch(s -> s.getValue().equals("algorithme")));
		Assertions.assertTrue(subjects.stream().anyMatch(s -> s.getValue().equals("simulation numérique")));
		Assertions.assertTrue(subjects.stream().anyMatch(s -> s.getValue().equals("flux de gaz")));
		Assertions.assertTrue(subjects.stream().anyMatch(s -> s.getValue().equals("flux de liquide")));

	}

	@Test
	void galaxyOtherTest() throws Exception {
		final String sourcePath = getClass()
			.getResource(
				"/eu/dnetlib/dhp/eosctag/galaxy/otherresearchproduct")
			.getPath();
		SparkBulkTagJob
			.main(
				new String[] {
					"-isTest", Boolean.TRUE.toString(),
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath", sourcePath,
					"-taggingConf", taggingConf,
					"-resultTableName", "eu.dnetlib.dhp.schema.oaf.OtherResearchProduct",
					"-outputPath", workingDir.toString() + "/otherresearchproduct",
					"-isLookUpUrl", MOCK_IS_LOOK_UP_URL,
					"-pathMap", pathMap
				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
		JavaRDD<OtherResearchProduct> orp = sc
			.textFile(workingDir.toString() + "/otherresearchproduct")
			.map(item -> OBJECT_MAPPER.readValue(item, OtherResearchProduct.class));

		Assertions.assertEquals(10, orp.count());

		Assertions
			.assertEquals(
				0,
				orp
					.filter(
						s -> s.getSubject().stream().anyMatch(sbj -> sbj.getValue().equals("EOSC::Galaxy Workflow")))
					.count());
		orp.foreach(o -> System.out.println(OBJECT_MAPPER.writeValueAsString(o)));

		Assertions
			.assertEquals(
				1, orp
					.filter(o -> o.getEoscifguidelines() != null)
					.filter(
						o -> o
							.getEoscifguidelines()
							.stream()
							.anyMatch(eig -> eig.getCode().equals("EOSC::Galaxy Workflow")))
					.count());

		Assertions
			.assertEquals(
				2, orp
					.filter(sw -> sw.getId().equals("50|od______2017::0750a4d0782265873d669520f5e33c07"))
					.collect()
					.get(0)
					.getSubject()
					.size());
		Assertions
			.assertFalse(
				orp
					.filter(sw -> sw.getId().equals("50|od______2017::0750a4d0782265873d669520f5e33c07"))
					.collect()
					.get(0)
					.getSubject()
					.stream()
					.anyMatch(s -> s.getValue().equals("EOSC::Galaxy Workflow")));
		Assertions
			.assertEquals(
				1, orp
					.filter(sw -> sw.getId().equals("50|od______2017::0750a4d0782265873d669520f5e33c07"))
					.collect()
					.get(0)
					.getEoscifguidelines()
					.size());
		Assertions
			.assertTrue(
				orp
					.filter(sw -> sw.getId().equals("50|od______2017::0750a4d0782265873d669520f5e33c07"))
					.collect()
					.get(0)
					.getEoscifguidelines()
					.stream()
					.anyMatch(s -> s.getCode().equals("EOSC::Galaxy Workflow")));

		Assertions
			.assertEquals(
				2, orp
					.filter(sw -> sw.getId().equals("50|od______2017::1bd97baef19dbd2db3203b112bb83bc5"))
					.collect()
					.get(0)
					.getSubject()
					.size());
		Assertions
			.assertFalse(
				orp
					.filter(sw -> sw.getId().equals("50|od______2017::1bd97baef19dbd2db3203b112bb83bc5"))
					.collect()
					.get(0)
					.getSubject()
					.stream()
					.anyMatch(s -> s.getValue().equals("EOSC::Galaxy Workflow")));

		Assertions
			.assertEquals(
				2, orp
					.filter(sw -> sw.getId().equals("50|od______2017::1e400f1747487fd15998735c41a55c72"))
					.collect()
					.get(0)
					.getSubject()
					.size());
		Assertions
			.assertFalse(
				orp
					.filter(sw -> sw.getId().equals("50|od______2017::1e400f1747487fd15998735c41a55c72"))
					.collect()
					.get(0)
					.getSubject()
					.stream()
					.anyMatch(s -> s.getValue().equals("EOSC::Galaxy Workflow")));
	}

	@Test
	void galaxySoftwareTest() throws Exception {
		final String sourcePath = getClass()
			.getResource(
				"/eu/dnetlib/dhp/eosctag/galaxy/software")
			.getPath();
		SparkBulkTagJob
			.main(
				new String[] {
					"-isTest", Boolean.TRUE.toString(),
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath", sourcePath,
					"-taggingConf", taggingConf,
					"-resultTableName", "eu.dnetlib.dhp.schema.oaf.Software",
					"-outputPath", workingDir.toString() + "/software",
					"-isLookUpUrl", MOCK_IS_LOOK_UP_URL,
					"-pathMap", pathMap
				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Software> tmp = sc
			.textFile(workingDir.toString() + "/software")
			.map(item -> OBJECT_MAPPER.readValue(item, Software.class));

		Assertions.assertEquals(11, tmp.count());

		Assertions
			.assertEquals(
				0,
				tmp
					.filter(
						s -> s.getSubject().stream().anyMatch(sbj -> sbj.getValue().equals("EOSC::Galaxy Workflow")))
					.count());
		Assertions
			.assertEquals(
				1,
				tmp
					.filter(
						s -> s.getEoscifguidelines().size() > 0)
					.count());
		Assertions
			.assertEquals(
				1,
				tmp
					.filter(
						s -> s.getEoscifguidelines().size() > 0)
					.filter(
						s -> s
							.getEoscifguidelines()
							.stream()
							.anyMatch(eig -> eig.getCode().equals("EOSC::Galaxy Workflow")))
					.count());

		Assertions
			.assertEquals(
				1, tmp
					.filter(sw -> sw.getId().equals("50|od______1582::4132f5ec9496f0d6adc7b00a50a56ff4"))
					.collect()
					.get(0)
					.getSubject()
					.size());
		Assertions
			.assertFalse(
				tmp
					.filter(sw -> sw.getId().equals("50|od______1582::4132f5ec9496f0d6adc7b00a50a56ff4"))
					.collect()
					.get(0)
					.getSubject()
					.stream()
					.anyMatch(s -> s.getValue().equals("EOSC::Galaxy Workflow")));

		Assertions
			.assertEquals(
				1, tmp
					.filter(sw -> sw.getId().equals("50|od______1582::4132f5ec9496f0d6adc7b00a50a56ff4"))
					.collect()
					.get(0)
					.getEoscifguidelines()
					.size());
		Assertions
			.assertTrue(
				tmp
					.filter(sw -> sw.getId().equals("50|od______1582::4132f5ec9496f0d6adc7b00a50a56ff4"))
					.collect()
					.get(0)
					.getEoscifguidelines()
					.stream()
					.anyMatch(eig -> eig.getCode().equals("EOSC::Galaxy Workflow")));

		Assertions
			.assertEquals(
				5, tmp
					.filter(sw -> sw.getId().equals("50|od______1582::501b25d420f808c8eddcd9b16e917f11"))
					.collect()
					.get(0)
					.getSubject()
					.size());

		Assertions
			.assertEquals(
				8, tmp
					.filter(sw -> sw.getId().equals("50|od______1582::581621232a561b7e8b4952b18b8b0e56"))
					.collect()
					.get(0)
					.getSubject()
					.size());
		Assertions
			.assertFalse(
				tmp
					.filter(sw -> sw.getId().equals("50|od______1582::581621232a561b7e8b4952b18b8b0e56"))
					.collect()
					.get(0)
					.getSubject()
					.stream()
					.anyMatch(s -> s.getValue().equals("EOSC::Galaxy Workflow")));

	}

	@Test
	void twitterDatasetTest() throws Exception {
		final String sourcePath = getClass()
			.getResource(
				"/eu/dnetlib/dhp/eosctag/twitter/dataset")
			.getPath();

		SparkBulkTagJob
			.main(
				new String[] {
					"-isTest", Boolean.TRUE.toString(),
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath", sourcePath,
					"-taggingConf", taggingConf,
					"-resultTableName", "eu.dnetlib.dhp.schema.oaf.Dataset",
					"-outputPath", workingDir.toString() + "/dataset",
					"-isLookUpUrl", MOCK_IS_LOOK_UP_URL,
					"-pathMap", pathMap
				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
		JavaRDD<Dataset> dats = sc
			.textFile(workingDir.toString() + "/dataset")
			.map(item -> OBJECT_MAPPER.readValue(item, Dataset.class));

		Assertions.assertEquals(11, dats.count());

		Assertions
			.assertEquals(
				3,
				dats
					.filter(
						s -> s
							.getEoscifguidelines()
							.stream()
							.anyMatch(eig -> eig.getCode().equals("EOSC::Twitter Data")))
					.count());

	}

	@Test
	void twitterOtherTest() throws Exception {
		final String sourcePath = getClass()
			.getResource(
				"/eu/dnetlib/dhp/eosctag/twitter/otherresearchproduct")
			.getPath();

		SparkBulkTagJob
			.main(
				new String[] {
					"-isTest", Boolean.TRUE.toString(),
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath", sourcePath,
					"-taggingConf", taggingConf,
					"-resultTableName", "eu.dnetlib.dhp.schema.oaf.OtherResearchProduct",
					"-outputPath", workingDir.toString() + "/otherresearchproduct",
					"-isLookUpUrl", MOCK_IS_LOOK_UP_URL,
					"-pathMap", pathMap
				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
		JavaRDD<OtherResearchProduct> orp = sc
			.textFile(workingDir.toString() + "/otherresearchproduct")
			.map(item -> OBJECT_MAPPER.readValue(item, OtherResearchProduct.class));

		Assertions.assertEquals(10, orp.count());

		Assertions
			.assertEquals(
				0,
				orp
					.filter(s -> s.getSubject().stream().anyMatch(sbj -> sbj.getValue().equals("EOSC::Twitter Data")))
					.count());
		Assertions
			.assertEquals(
				3,
				orp
					.filter(
						s -> s
							.getEoscifguidelines()
							.stream()
							.anyMatch(eig -> eig.getCode().equals("EOSC::Twitter Data")))
					.count());
	}

	@Test
	void twitterSoftwareTest() throws Exception {
		final String sourcePath = getClass()
			.getResource(
				"/eu/dnetlib/dhp/eosctag/twitter/software")
			.getPath();

		SparkBulkTagJob
			.main(
				new String[] {
					"-isTest", Boolean.TRUE.toString(),
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath", sourcePath,
					"-taggingConf", taggingConf,
					"-resultTableName", "eu.dnetlib.dhp.schema.oaf.Software",
					"-outputPath", workingDir.toString() + "/software",
					"-isLookUpUrl", MOCK_IS_LOOK_UP_URL,
					"-pathMap", pathMap
				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Software> tmp = sc
			.textFile(workingDir.toString() + "/software")
			.map(item -> OBJECT_MAPPER.readValue(item, Software.class));

		Assertions.assertEquals(10, tmp.count());

		Assertions
			.assertEquals(
				0,
				tmp
					.filter(s -> s.getSubject().stream().anyMatch(sbj -> sbj.getValue().equals("EOSC::Twitter Data")))
					.count());

	}

	@Test
	void EoscContextTagTest() throws Exception {
		final String sourcePath = getClass()
			.getResource(
				"/eu/dnetlib/dhp/bulktag/eosc/dataset/dataset_10.json")
			.getPath();

		SparkBulkTagJob
			.main(
				new String[] {
					"-isTest", Boolean.TRUE.toString(),
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath", sourcePath,
					"-taggingConf", taggingConf,
					"-resultTableName", "eu.dnetlib.dhp.schema.oaf.Dataset",
					"-outputPath", workingDir.toString() + "/dataset",
					"-isLookUpUrl", MOCK_IS_LOOK_UP_URL,
					"-pathMap", pathMap
				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Dataset> tmp = sc
			.textFile(workingDir.toString() + "/dataset")
			.map(item -> OBJECT_MAPPER.readValue(item, Dataset.class));

		Assertions.assertEquals(8, tmp.count());

		Assertions
			.assertEquals(
				4,
				tmp
					.filter(
						s -> s.getContext().stream().anyMatch(c -> c.getId().equals("eosc")))
					.count());

		Assertions
			.assertEquals(
				1,
				tmp
					.filter(
						d -> d.getId().equals("50|475c1990cbb2::0fecfb874d9395aa69d2f4d7cd1acbea")
							&&
							d.getContext().stream().anyMatch(c -> c.getId().equals("eosc")))
					.count());
		Assertions
			.assertEquals(
				1,
				tmp
					.filter(
						d -> d.getId().equals("50|475c1990cbb2::3185cd5d8a2b0a06bb9b23ef11748eb1")
							&&
							d.getContext().stream().anyMatch(c -> c.getId().equals("eosc")))
					.count());

		Assertions
			.assertEquals(
				1,
				tmp
					.filter(
						d -> d.getId().equals("50|475c1990cbb2::3894c94123e96df8a21249957cf160cb")
							&&
							d.getContext().stream().anyMatch(c -> c.getId().equals("eosc")))
					.count());

		Assertions
			.assertEquals(
				1,
				tmp
					.filter(
						d -> d.getId().equals("50|475c1990cbb2::449f28eefccf9f70c04ad70d61e041c7")
							&&
							d.getContext().stream().anyMatch(c -> c.getId().equals("eosc")))
					.count());
	}

	@Test
	void removeTest() throws Exception {
		final String pathMap = BulkTagJobTest.pathMap;
		SparkBulkTagJob
			.main(
				new String[] {
					"-isTest", Boolean.TRUE.toString(),
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath",
					getClass()
						.getResource("/eu/dnetlib/dhp/bulktag/sample/dataset/update_datasourcewithconstraints")
						.getPath(),
					"-taggingConf", taggingConf,
					"-resultTableName", "eu.dnetlib.dhp.schema.oaf.Dataset",
					"-outputPath", workingDir.toString() + "/dataset",
					"-isLookUpUrl", MOCK_IS_LOOK_UP_URL,
					"-pathMap", pathMap
				});
		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Dataset> tmp = sc
			.textFile(workingDir.toString() + "/dataset")
			.map(item -> OBJECT_MAPPER.readValue(item, Dataset.class));

		Assertions.assertEquals(12, tmp.count());
		org.apache.spark.sql.Dataset<Dataset> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(Dataset.class));

		verificationDataset.createOrReplaceTempView("dataset");
		String query = "select id, MyT.id community, MyD.provenanceaction.classid provenance, MyD.provenanceaction.classname name "
			+ "from dataset "
			+ "lateral view explode(context) c as MyT "
			+ "lateral view explode(MyT.datainfo) d as MyD "
			+ "where MyD.inferenceprovenance = 'bulktagging'";

		org.apache.spark.sql.Dataset<Row> idExplodeCommunity = spark.sql(query);

		Assertions.assertEquals(3, idExplodeCommunity.filter("community = 'dth'").count());

	}

	@Test
	void newConfTest() throws Exception {
		final String pathMap = BulkTagJobTest.pathMap;
		SparkBulkTagJob
			.main(
				new String[] {
					"-isTest", Boolean.TRUE.toString(),
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath",
					getClass().getResource("/eu/dnetlib/dhp/bulktag/sample/dataset/no_updates").getPath(),
					"-taggingConf", taggingConf,
					"-resultTableName", "eu.dnetlib.dhp.schema.oaf.Dataset",
					"-outputPath", workingDir.toString() + "/dataset",
					"-isLookUpUrl", MOCK_IS_LOOK_UP_URL,
					"-pathMap", pathMap
				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Dataset> tmp = sc
			.textFile(workingDir.toString() + "/dataset")
			.map(item -> OBJECT_MAPPER.readValue(item, Dataset.class));

		Assertions.assertEquals(10, tmp.count());
		org.apache.spark.sql.Dataset<Dataset> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(Dataset.class));

		verificationDataset.createOrReplaceTempView("dataset");

		String query = "select id, MyT.id community "
			+ "from dataset "
			+ "lateral view explode(context) c as MyT "
			+ "lateral view explode(MyT.datainfo) d as MyD "
			+ "where MyD.inferenceprovenance = 'bulktagging'";

		Assertions.assertEquals(0, spark.sql(query).count());
	}

}
