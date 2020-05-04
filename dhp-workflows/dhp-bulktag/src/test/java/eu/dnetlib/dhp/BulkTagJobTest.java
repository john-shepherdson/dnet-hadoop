
package eu.dnetlib.dhp;

import static eu.dnetlib.dhp.community.TagginConstants.ZENODO_COMMUNITY_INDICATOR;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.io.FileUtils;
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
import org.mortbay.util.IO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.bulktag.SparkBulkTagJob2;
import eu.dnetlib.dhp.schema.oaf.Dataset;
import eu.dnetlib.dhp.schema.oaf.OtherResearchProduct;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Software;

public class BulkTagJobTest {
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static final ClassLoader cl = eu.dnetlib.dhp.BulkTagJobTest.class.getClassLoader();

	private static SparkSession spark;

	private static Path workingDir;
	private static final Logger log = LoggerFactory.getLogger(eu.dnetlib.dhp.BulkTagJobTest.class);

	private static String taggingConf = "";

	static {
		try {
			taggingConf = IO
				.toString(
					BulkTagJobTest.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/communityconfiguration/tagging_conf.xml"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files.createTempDirectory(eu.dnetlib.dhp.BulkTagJobTest.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(eu.dnetlib.dhp.BulkTagJobTest.class.getSimpleName());

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
	public void noUpdatesTest() throws Exception {
		SparkBulkTagJob2
			.main(
				new String[] {
					"-isTest",
					Boolean.TRUE.toString(),
					"-isSparkSessionManaged",
					Boolean.FALSE.toString(),
					"-sourcePath",
					getClass().getResource("/eu/dnetlib/dhp/sample/dataset/no_updates").getPath(),
					"-taggingConf",
					taggingConf,
					"-resultTableName",
					"eu.dnetlib.dhp.schema.oaf.Dataset",
					"-outputPath",
					workingDir.toString() + "/dataset",
					"-isLookUpUrl",
					"http://beta.services.openaire.eu:8280/is/services/isLookUp",
					"-pathMap",
					"{ \"author\" : \"$['author'][*]['fullname']\","
						+ "  \"title\" : \"$['title'][*]['value']\","
						+ "  \"orcid\" : \"$['author'][*]['pid'][*][?(@['key']=='ORCID')]['value']\","
						+ "  \"contributor\" : \"$['contributor'][*]['value']\","
						+ "  \"description\" : \"$['description'][*]['value']\"}"
				// "-preparedInfoPath",
				// getClass().getResource("/eu/dnetlib/dhp/resulttocommunityfromsemrel/preparedInfo").getPath()
				});

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

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
	public void bulktagBySubjectNoPreviousContextTest() throws Exception {
		SparkBulkTagJob2
			.main(
				new String[] {
					"-isTest",
					Boolean.TRUE.toString(),
					"-isSparkSessionManaged",
					Boolean.FALSE.toString(),
					"-sourcePath",
					getClass()
						.getResource("/eu/dnetlib/dhp/sample/dataset/update_subject/nocontext")
						.getPath(),
					"-taggingConf",
					taggingConf,
					"-resultTableName",
					"eu.dnetlib.dhp.schema.oaf.Dataset",
					"-outputPath",
					workingDir.toString() + "/dataset",
					"-isLookUpUrl",
					"http://beta.services.openaire.eu:8280/is/services/isLookUp",
					"-pathMap",
					"{ \"author\" : \"$['author'][*]['fullname']\","
						+ "  \"title\" : \"$['title'][*]['value']\","
						+ "  \"orcid\" : \"$['author'][*]['pid'][*][?(@['key']=='ORCID')]['value']\","
						+ "  \"contributor\" : \"$['contributor'][*]['value']\","
						+ "  \"description\" : \"$['description'][*]['value']\"}"
				});

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

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
	public void bulktagBySubjectPreviousContextNoProvenanceTest() throws Exception {
		SparkBulkTagJob2
			.main(
				new String[] {
					"-isTest",
					Boolean.TRUE.toString(),
					"-isSparkSessionManaged",
					Boolean.FALSE.toString(),
					"-sourcePath",
					getClass()
						.getResource(
							"/eu/dnetlib/dhp/sample/dataset/update_subject/contextnoprovenance")
						.getPath(),
					"-taggingConf",
					taggingConf,
					"-resultTableName",
					"eu.dnetlib.dhp.schema.oaf.Dataset",
					"-outputPath",
					workingDir.toString() + "/dataset",
					"-isLookUpUrl",
					"http://beta.services.openaire.eu:8280/is/services/isLookUp",
					"-pathMap",
					"{ \"author\" : \"$['author'][*]['fullname']\","
						+ "  \"title\" : \"$['title'][*]['value']\","
						+ "  \"orcid\" : \"$['author'][*]['pid'][*][?(@['key']=='ORCID')]['value']\","
						+ "  \"contributor\" : \"$['contributor'][*]['value']\","
						+ "  \"description\" : \"$['description'][*]['value']\"}"
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
	public void bulktagByDatasourceTest() throws Exception {
		SparkBulkTagJob2
			.main(
				new String[] {
					"-isTest",
					Boolean.TRUE.toString(),
					"-isSparkSessionManaged",
					Boolean.FALSE.toString(),
					"-sourcePath",
					getClass()
						.getResource("/eu/dnetlib/dhp/sample/publication/update_datasource")
						.getPath(),
					"-taggingConf",
					taggingConf,
					"-resultTableName",
					"eu.dnetlib.dhp.schema.oaf.Publication",
					"-outputPath",
					workingDir.toString() + "/publication",
					"-isLookUpUrl",
					"http://beta.services.openaire.eu:8280/is/services/isLookUp",
					"-pathMap",
					"{ \"author\" : \"$['author'][*]['fullname']\","
						+ "  \"title\" : \"$['title'][*]['value']\","
						+ "  \"orcid\" : \"$['author'][*]['pid'][*][?(@['key']=='ORCID')]['value']\","
						+ "  \"contributor\" : \"$['contributor'][*]['value']\","
						+ "  \"description\" : \"$['description'][*]['value']\"}"
				});

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

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
	public void bulktagByZenodoCommunityTest() throws Exception {
		SparkBulkTagJob2
			.main(
				new String[] {
					"-isTest",
					Boolean.TRUE.toString(),
					"-isSparkSessionManaged",
					Boolean.FALSE.toString(),
					"-sourcePath",
					getClass()
						.getResource(
							"/eu/dnetlib/dhp/sample/otherresearchproduct/update_zenodocommunity")
						.getPath(),
					"-taggingConf",
					taggingConf,
					"-resultTableName",
					"eu.dnetlib.dhp.schema.oaf.OtherResearchProduct",
					"-outputPath",
					workingDir.toString() + "/orp",
					"-isLookUpUrl",
					"http://beta.services.openaire.eu:8280/is/services/isLookUp",
					"-pathMap",
					"{ \"author\" : \"$['author'][*]['fullname']\","
						+ "  \"title\" : \"$['title'][*]['value']\","
						+ "  \"orcid\" : \"$['author'][*]['pid'][*][?(@['key']=='ORCID')]['value']\","
						+ "  \"contributor\" : \"$['contributor'][*]['value']\","
						+ "  \"description\" : \"$['description'][*]['value']\"}"
				});

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

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
	public void bulktagBySubjectDatasourceTest() throws Exception {
		SparkBulkTagJob2
			.main(
				new String[] {
					"-isTest",
					Boolean.TRUE.toString(),
					"-isSparkSessionManaged",
					Boolean.FALSE.toString(),
					"-sourcePath",
					getClass()
						.getResource("/eu/dnetlib/dhp/sample/dataset/update_subject_datasource")
						.getPath(),
					"-taggingConf",
					taggingConf,
					"-resultTableName",
					"eu.dnetlib.dhp.schema.oaf.Dataset",
					"-outputPath",
					workingDir.toString() + "/dataset",
					"-isLookUpUrl",
					"http://beta.services.openaire.eu:8280/is/services/isLookUp",
					"-pathMap",
					"{ \"author\" : \"$['author'][*]['fullname']\","
						+ "  \"title\" : \"$['title'][*]['value']\","
						+ "  \"orcid\" : \"$['author'][*]['pid'][*][?(@['key']=='ORCID')]['value']\","
						+ "  \"contributor\" : \"$['contributor'][*]['value']\","
						+ "  \"description\" : \"$['description'][*]['value']\"}"
				});

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

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
	public void bulktagBySubjectDatasourceZenodoCommunityTest() throws Exception {

		SparkBulkTagJob2
			.main(
				new String[] {
					"-isTest",
					Boolean.TRUE.toString(),
					"-isSparkSessionManaged",
					Boolean.FALSE.toString(),
					"-sourcePath",
					getClass().getResource("/eu/dnetlib/dhp/sample/software/").getPath(),
					"-taggingConf",
					taggingConf,
					"-resultTableName",
					"eu.dnetlib.dhp.schema.oaf.Software",
					"-outputPath",
					workingDir.toString() + "/software",
					"-isLookUpUrl",
					"http://beta.services.openaire.eu:8280/is/services/isLookUp",
					"-pathMap",
					"{ \"author\" : \"$['author'][*]['fullname']\","
						+ "  \"title\" : \"$['title'][*]['value']\","
						+ "  \"orcid\" : \"$['author'][*]['pid'][*][?(@['key']=='ORCID')]['value']\","
						+ "  \"contributor\" : \"$['contributor'][*]['value']\","
						+ "  \"description\" : \"$['description'][*]['value']\"}"
				});

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

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
		Assertions.assertEquals(1, idExplodeCommunity.filter("community = 'dariah'").count());
		Assertions.assertEquals(1, idExplodeCommunity.filter("community = 'fam'").count());

		Assertions
			.assertEquals(
				2,
				idExplodeCommunity
					.filter(
						"provenance = 'community:zenodocommunity' and "
							+ "id = '50|od______1582::4132f5ec9496f0d6adc7b00a50a56ff4' and ("
							+ "community = 'dh-ch' or community = 'dariah')")
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
	public void bulktagDatasourcewithConstraintsTest() throws Exception {

		SparkBulkTagJob2
			.main(
				new String[] {
					"-isTest",
					Boolean.TRUE.toString(),
					"-isSparkSessionManaged",
					Boolean.FALSE.toString(),
					"-sourcePath",
					getClass()
						.getResource(
							"/eu/dnetlib/dhp/sample/dataset/update_datasourcewithconstraints")
						.getPath(),
					"-taggingConf",
					taggingConf,
					"-resultTableName",
					"eu.dnetlib.dhp.schema.oaf.Dataset",
					"-outputPath",
					workingDir.toString() + "/dataset",
					"-isLookUpUrl",
					"http://beta.services.openaire.eu:8280/is/services/isLookUp",
					"-pathMap",
					"{ \"author\" : \"$['author'][*]['fullname']\","
						+ "  \"title\" : \"$['title'][*]['value']\","
						+ "  \"orcid\" : \"$['author'][*]['pid'][*][?(@['key']=='ORCID')]['value']\","
						+ "  \"contributor\" : \"$['contributor'][*]['value']\","
						+ "  \"description\" : \"$['description'][*]['value']\"}"
				});

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

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

		idExplodeCommunity.show(false);
		Assertions.assertEquals(3, idExplodeCommunity.count());

		Assertions
			.assertEquals(
				3, idExplodeCommunity.filter("provenance = 'community:datasource'").count());
	}
}
