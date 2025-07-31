
package eu.dnetlib.dhp.bulktag;

import static eu.dnetlib.dhp.bulktag.community.TaggingConstants.ZENODO_COMMUNITY_INDICATOR;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import eu.dnetlib.dhp.bulktag.community.ResultTagger;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;

import eu.dnetlib.dhp.api.QueryCommunityAPI;
import eu.dnetlib.dhp.api.Utils;
import eu.dnetlib.dhp.api.model.SubCommunityModel;
import eu.dnetlib.dhp.bulktag.community.CommunityConfiguration;
import eu.dnetlib.dhp.bulktag.community.ProtoMap;
import eu.dnetlib.dhp.schema.oaf.*;

public class BulkTagJobTest {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static final String pathMap = "{\"protoMap\":{\"author\":{\"path\":\"$['author'][*]['fullname']\"}," +
		" \"title\":{\"path\":\"$['title'][*]['value']\"}, " +
		" \"orcid\":{\"path\":\"$['author'][*]['pid'][*][?(@['qualifier']['classid']=='orcid')]['value']\"} , " +
		" \"orcid_pending\":{\"path\":\"$['author'][*]['pid'][*][?(@['qualifier']['classid']=='orcid_pending')]['value']\"} ,"
		+
		"\"contributor\" : {\"path\":\"$['contributor'][*]['value']\"}," +
		" \"description\" : {\"path\":\"$['description'][*]['value']\"}," +
		" \"subject\" :{\"path\":\"$['subject'][*]['value']\"}, " +
		" \"fos\" : {\"path\":\"$['subject'][?(@['qualifier']['classid']=='FOS')].value\"} , " +
		"\"sdg\" : {\"path\":\"$['subject'][?(@['qualifier']['classid']=='SDG')].value\"}," +
		"\"journal\":{\"path\":\"$['journal'].name\"}," +
		"\"hostedby\":{\"path\":\"$['instance'][*]['hostedby']['key']\"}," +
		"\"collectedfrom\":{\"path\":\"$['instance'][*]['collectedfrom']['key']\"}," +
		"\"publisher\":{\"path\":\"$['publisher'].value\"}," +
		"\"publicationyear\":{\"path\":\"$['dateofacceptance'].value\", " +
		" \"action\":{\"clazz\":\"eu.dnetlib.dhp.bulktag.actions.ExecSubstringAction\"," +
		"\"method\":\"execSubstring\"," +
		"\"params\":[" +
		"{\"paramName\":\"From\",  \"paramValue\":0}, " +
		"{\"paramName\":\"To\",\"paramValue\":4}]}}}}";

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
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath",
					getClass().getResource("/eu/dnetlib/dhp/bulktag/sample/dataset/no_updates/").getPath(),
					"-taggingConf", taggingConf,
					"-outputPath", workingDir.toString() + "/",
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
			.getResource("/eu/dnetlib/dhp/bulktag/sample/dataset/update_subject/nocontext/")
			.getPath();
		final String pathMap = BulkTagJobTest.pathMap;
		SparkBulkTagJob
			.main(
				new String[] {
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath", sourcePath,
					"-taggingConf", taggingConf,
					"-outputPath", workingDir.toString() + "/",
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
		LocalFileSystem fs = FileSystem.getLocal(new Configuration());
		fs
			.copyFromLocalFile(
				false, new org.apache.hadoop.fs.Path(getClass()
					.getResource("/eu/dnetlib/dhp/bulktag/pathMap/")
					.getPath()),
				new org.apache.hadoop.fs.Path(workingDir.toString() + "/data/bulktagging/protoMap"));

		final String sourcePath = getClass()
			.getResource(
				"/eu/dnetlib/dhp/bulktag/sample/dataset/update_subject/contextnoprovenance/")
			.getPath();
		final String pathMap = BulkTagJobTest.pathMap;
		SparkBulkTagJob
			.main(
				new String[] {

					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath", sourcePath,
					"-taggingConf", taggingConf,

					"-outputPath", workingDir.toString() + "/",

					"-pathMap", workingDir.toString() + "/data/bulktagging/protoMap",
					"-nameNode", "local"
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
			.getResource("/eu/dnetlib/dhp/bulktag/sample/publication/update_datasource/")
			.getPath();

		SparkBulkTagJob
			.main(
				new String[] {

					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath", sourcePath,
					"-taggingConf", taggingConf,

					"-outputPath", workingDir.toString() + "/",
					"-baseURL", "https://services.openaire.eu/openaire/community/",
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
	void datasourceTag() throws Exception {
		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/bulktag/sample/publication/update_datasource/")
			.getPath();
		LocalFileSystem fs = FileSystem.getLocal(new Configuration());
		fs
			.copyFromLocalFile(
				false, new org.apache.hadoop.fs.Path(getClass()
					.getResource("/eu/dnetlib/dhp/bulktag/pathMap/")
					.getPath()),
				new org.apache.hadoop.fs.Path(workingDir.toString() + "/data/bulktagging/protoMap"));
		SparkBulkTagJob
			.main(
				new String[] {

					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath", sourcePath,
					"-taggingConf", taggingConf,

					"-outputPath", workingDir.toString() + "/",
					"-baseURL", "https://services.openaire.eu/openaire/community/",

					"-pathMap", workingDir.toString() + "/data/bulktagging/protoMap/pathMap",
					"-nameNode", "local"
				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Datasource> tmp = sc
			.textFile(workingDir.toString() + "/datasource")
			.map(item -> OBJECT_MAPPER.readValue(item, Datasource.class));

		Assertions.assertEquals(3, tmp.count());
		org.apache.spark.sql.Dataset<Datasource> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(Datasource.class));

		verificationDataset.createOrReplaceTempView("datasource");

		String query = "select id, MyT.id community, MyD.provenanceaction.classid provenance, MyD.provenanceaction.classname name "
			+ "from datasource "
			+ "lateral view explode(context) c as MyT "
			+ "lateral view explode(MyT.datainfo) d as MyD "
			+ "where MyD.inferenceprovenance = 'bulktagging'";

		org.apache.spark.sql.Dataset<Row> idExplodeCommunity = spark.sql(query);

		idExplodeCommunity.show(false);

		Assertions.assertEquals(3, idExplodeCommunity.count());
		Assertions
			.assertEquals(
				3, idExplodeCommunity.filter("provenance = 'community:datasource'").count());
		Assertions
			.assertEquals(
				3,
				idExplodeCommunity
					.filter("name = 'Bulktagging for Community - Datasource'")
					.count());

		Assertions.assertEquals(2, idExplodeCommunity.filter("community = 'dh-ch'").count());
		Assertions.assertEquals(1, idExplodeCommunity.filter("community = 'clarin'").count());

	}

	@Test
	void organizationTag() throws Exception {
		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/bulktag/sample/publication/update_datasource/")
			.getPath();
		LocalFileSystem fs = FileSystem.getLocal(new Configuration());
		fs
			.copyFromLocalFile(
				false, new org.apache.hadoop.fs.Path(getClass()
					.getResource("/eu/dnetlib/dhp/bulktag/pathMap/")
					.getPath()),
				new org.apache.hadoop.fs.Path(workingDir.toString() + "/data/bulktagging/protoMap"));
		SparkBulkTagJob
			.main(
				new String[] {

					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath", sourcePath,
					"-taggingConf", taggingConf,

					"-outputPath", workingDir.toString() + "/",
					"-baseURL", "https://services.openaire.eu/openaire/community/",

					"-pathMap", workingDir.toString() + "/data/bulktagging/protoMap/pathMap",
					"-nameNode", "local"
				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Organization> tmp = sc
			.textFile(workingDir.toString() + "/organization")
			.map(item -> OBJECT_MAPPER.readValue(item, Organization.class));

		Assertions.assertEquals(4, tmp.count());
		org.apache.spark.sql.Dataset<Organization> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(Organization.class));

		verificationDataset.createOrReplaceTempView("organization");

		String query = "select id, MyT.id community, MyD.provenanceaction.classid provenance, MyD.provenanceaction.classname name "
			+ "from organization "
			+ "lateral view explode(context) c as MyT "
			+ "lateral view explode(MyT.datainfo) d as MyD "
			+ "where MyD.inferenceprovenance = 'bulktagging'";

		org.apache.spark.sql.Dataset<Row> idExplodeCommunity = spark.sql(query);

		idExplodeCommunity.show(false);

		Assertions.assertEquals(3, idExplodeCommunity.count());
		Assertions
			.assertEquals(
				3, idExplodeCommunity.filter("provenance = 'community:organization'").count());
		Assertions
			.assertEquals(
				3,
				idExplodeCommunity
					.filter("name = 'Bulktagging for Community - Organization'")
					.count());

		Assertions.assertEquals(1, idExplodeCommunity.filter("community = 'netherlands'").count());
		Assertions.assertEquals(1, idExplodeCommunity.filter("community = 'beopen'").count());
		Assertions.assertEquals(1, idExplodeCommunity.filter("community = 'mes'").count());

	}

	@Test
	void projectTag() throws Exception {
		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/bulktag/sample/publication/update_datasource/")
			.getPath();
		LocalFileSystem fs = FileSystem.getLocal(new Configuration());
		fs
			.copyFromLocalFile(
				false, new org.apache.hadoop.fs.Path(getClass()
					.getResource("/eu/dnetlib/dhp/bulktag/pathMap/")
					.getPath()),
				new org.apache.hadoop.fs.Path(workingDir.toString() + "/data/bulktagging/protoMap"));
		SparkBulkTagJob
			.main(
				new String[] {

					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath", sourcePath,
					"-taggingConf", taggingConf,

					"-outputPath", workingDir.toString() + "/",
					"-baseURL", "https://services.openaire.eu/openaire/community/",

					"-pathMap", workingDir.toString() + "/data/bulktagging/protoMap/pathMap",
					"-nameNode", "local"
				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Project> tmp = sc
			.textFile(workingDir.toString() + "/project")
			.map(item -> OBJECT_MAPPER.readValue(item, Project.class));

		Assertions.assertEquals(4, tmp.count());
		org.apache.spark.sql.Dataset<Project> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(Project.class));

		verificationDataset.createOrReplaceTempView("project");

		String query = "select id, MyT.id community, MyD.provenanceaction.classid provenance, MyD.provenanceaction.classname name "
			+ "from project "
			+ "lateral view explode(context) c as MyT "
			+ "lateral view explode(MyT.datainfo) d as MyD "
			+ "where MyD.inferenceprovenance = 'bulktagging'";

		org.apache.spark.sql.Dataset<Row> idExplodeCommunity = spark.sql(query);

		idExplodeCommunity.show(false);

		Assertions.assertEquals(4, idExplodeCommunity.count());
		Assertions
			.assertEquals(
				4, idExplodeCommunity.filter("provenance = 'community:project'").count());
		Assertions
			.assertEquals(
				4,
				idExplodeCommunity
					.filter("name = 'Bulktagging for Community - Project'")
					.count());

		Assertions.assertEquals(1, idExplodeCommunity.filter("community = 'enermaps'").count());
		Assertions.assertEquals(1, idExplodeCommunity.filter("community = 'clarin'").count());
		Assertions.assertEquals(2, idExplodeCommunity.filter("community = 'dh-ch'").count());

	}

	@Test
	void bulktagByZenodoCommunityTest() throws Exception {
		final String sourcePath = getClass()
			.getResource(
				"/eu/dnetlib/dhp/bulktag/sample/otherresearchproduct/update_zenodocommunity/")
			.getPath();
		SparkBulkTagJob
			.main(
				new String[] {

					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath", sourcePath,
					"-taggingConf", taggingConf,

					"-outputPath", workingDir.toString() + "/",

					"-pathMap", pathMap
				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<OtherResearchProduct> tmp = sc
			.textFile(workingDir.toString() + "/otherresearchproduct")
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
			.getResource("/eu/dnetlib/dhp/bulktag/sample/dataset/update_subject_datasource/")
			.getPath();
		SparkBulkTagJob
			.main(
				new String[] {

					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath", sourcePath,
					"-taggingConf", taggingConf,

					"-outputPath", workingDir.toString() + "/",

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

					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath",
					getClass().getResource("/eu/dnetlib/dhp/bulktag/sample/software/").getPath(),
					"-taggingConf", taggingConf,

					"-outputPath", workingDir.toString() + "/",

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
				"/eu/dnetlib/dhp/bulktag/sample/dataset/update_datasourcewithconstraints/")
			.getPath();
		SparkBulkTagJob
			.main(
				new String[] {

					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath", sourcePath,
					"-taggingConf", taggingConf,

					"-outputPath", workingDir.toString() + "/",

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
				"/eu/dnetlib/dhp/eosctag/jupyter/")
			.getPath();

		SparkBulkTagJob
			.main(
				new String[] {

					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath", sourcePath,
					"-taggingConf", taggingConf,

					"-outputPath", workingDir.toString() + "/",

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
				"/eu/dnetlib/dhp/eosctag/jupyter/")
			.getPath();
		SparkBulkTagJob
			.main(
				new String[] {

					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath", sourcePath,
					"-taggingConf", taggingConf,

					"-outputPath", workingDir.toString() + "/",

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
				"/eu/dnetlib/dhp/eosctag/jupyter/")
			.getPath();
		SparkBulkTagJob
			.main(
				new String[] {

					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath", sourcePath,
					"-taggingConf", taggingConf,

					"-outputPath", workingDir.toString() + "/",

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
				"/eu/dnetlib/dhp/eosctag/galaxy/")
			.getPath();
		SparkBulkTagJob
			.main(
				new String[] {

					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath", sourcePath,
					"-taggingConf", taggingConf,

					"-outputPath", workingDir.toString() + "/",

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
				"/eu/dnetlib/dhp/eosctag/galaxy/")
			.getPath();
		SparkBulkTagJob
			.main(
				new String[] {

					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath", sourcePath,
					"-taggingConf", taggingConf,

					"-outputPath", workingDir.toString() + "/",

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
				"/eu/dnetlib/dhp/eosctag/twitter/")
			.getPath();

		SparkBulkTagJob
			.main(
				new String[] {

					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath", sourcePath,
					"-taggingConf", taggingConf,

					"-outputPath", workingDir.toString() + "/",

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
				"/eu/dnetlib/dhp/eosctag/twitter/")
			.getPath();

		SparkBulkTagJob
			.main(
				new String[] {

					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath", sourcePath,
					"-taggingConf", taggingConf,

					"-outputPath", workingDir.toString() + "/",

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
				"/eu/dnetlib/dhp/eosctag/twitter/")
			.getPath();

		SparkBulkTagJob
			.main(
				new String[] {

					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath", sourcePath,
					"-taggingConf", taggingConf,

					"-outputPath", workingDir.toString() + "/",

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
				"/eu/dnetlib/dhp/bulktag/eosc/dataset/")
			.getPath();

		SparkBulkTagJob
			.main(
				new String[] {

					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath", sourcePath,
					"-taggingConf", taggingConf,

					"-outputPath", workingDir.toString() + "/",

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

					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath",
					getClass()
						.getResource("/eu/dnetlib/dhp/bulktag/sample/dataset/update_datasourcewithconstraints/")
						.getPath(),
					"-taggingConf", taggingConf,

					"-outputPath", workingDir.toString() + "/",

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

					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath",
					getClass().getResource("/eu/dnetlib/dhp/bulktag/sample/dataset/no_updates/").getPath(),
					"-outputPath", workingDir.toString() + "/",
//					"-baseURL", "https://services.openaire.eu/openaire/community/",
					"-pathMap", pathMap,
					"-taggingConf", taggingConf
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
	void pubdateTest() throws Exception {

		final String pathMap = BulkTagJobTest.pathMap;
		SparkBulkTagJob
			.main(
				new String[] {
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath",
					getClass().getResource("/eu/dnetlib/dhp/bulktag/sample/dataset/publicationyear/").getPath(),
					"-taggingConf",
					IOUtils
						.toString(
							BulkTagJobTest.class
								.getResourceAsStream(
									"/eu/dnetlib/dhp/bulktag/communityconfiguration/tagging_conf_publicationdate.xml")),
					"-outputPath", workingDir.toString() + "/",
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

		String query = "select id, MyT.id community, MyD.provenanceaction.classid "
			+ "from dataset "
			+ "lateral view explode(context) c as MyT "
			+ "lateral view explode(MyT.datainfo) d as MyD "
			+ "where MyD.inferenceprovenance = 'bulktagging'";

		org.apache.spark.sql.Dataset<Row> queryResult = spark.sql(query);
		queryResult.show(false);
		Assertions.assertEquals(5, queryResult.count());

		Assertions
			.assertEquals(
				1,
				queryResult
					.filter(
						(FilterFunction<Row>) r -> r
							.getAs("id")
							.equals("50|od______3989::02dd5d2c222191b0b9bd4f33c8e96529"))
					.count());
		Assertions
			.assertEquals(
				1,
				queryResult
					.filter(
						(FilterFunction<Row>) r -> r
							.getAs("id")
							.equals("50|od______3989::2f4f3c820c450bd08dac08d07cc82dcf"))
					.count());
		Assertions
			.assertEquals(
				1,
				queryResult
					.filter(
						(FilterFunction<Row>) r -> r
							.getAs("id")
							.equals("50|od______3989::7fcbe3a03280663cddebfd3cb9203177"))
					.count());
		Assertions
			.assertEquals(
				1,
				queryResult
					.filter(
						(FilterFunction<Row>) r -> r
							.getAs("id")
							.equals("50|od______3989::d791339867bec6d3eb2104deeb4e4961"))
					.count());
		Assertions
			.assertEquals(
				1,
				queryResult
					.filter(
						(FilterFunction<Row>) r -> r
							.getAs("id")
							.equals("50|od______3989::d90d3a1f64ad264b5ebed8a35b280343"))
					.count());

	}

	@Test
	public void prova() throws Exception {
		ResultTagger resultTagger = new ResultTagger();
		String baseURL = "https://services.openaire.eu/openaire/community/";
		String pathMap = "{\n" +
				"   \"author\":{\n" +
				"      \"path\":\"$['author'][*]['fullname']\"\n" +
				"   },\n" +
				"   \"title\":{\n" +
				"      \"path\":\"$['title'][*]['value']\"\n" +
				"   },\n" +
				"   \"orcid\":{\n" +
				"      \"path\":\"$['author'][*]['pid'][*][?(@['qualifier']['classid']=='orcid')]['value']\"\n" +
				"   },\n" +
				"   \"orcid_pending\":{\n" +
				"      \"path\":\"$['author'][*]['pid'][*][?(@['qualifier']['classid']=='orcid_pending')]['value']\"\n" +
				"   },\n" +
				"   \"contributor\":{\n" +
				"      \"path\":\"$['contributor'][*]['value']\"\n" +
				"   },\n" +
				"   \"description\":{\n" +
				"      \"path\":\"$['description'][*]['value']\"\n" +
				"   },\n" +
				"   \"subject\":{\n" +
				"      \"path\":\"$['subject'][*]['value']\"\n" +
				"   },\n" +
				"   \"fos\":{\n" +
				"      \"path\":\"$['subject'][?(@['qualifier']['classid']=='FOS')].value\"\n" +
				"   },\n" +
				"   \"sdg\":{\n" +
				"      \"path\":\"$['subject'][?(@['qualifier']['classid']=='SDG')].value\"\n" +
				"   },\n" +
				"   \"journal\":{\n" +
				"      \"path\":\"$['journal'].name\"\n" +
				"   },\n" +
				"   \"hostedby\":{\n" +
				"      \"path\":\"$['instance'][*]['hostedby']['key']\"\n" +
				"   },\n" +
				"   \"collectedfrom\":{\n" +
				"      \"path\":\"$['instance'][*]['collectedfrom']['key']\"\n" +
				"   },\n" +
				"   \"publisher\":{\n" +
				"      \"path\":\"$['publisher'].value\"\n" +
				"   },\n" +
				"   \"access\":{\n" +
				"      \"path\":\"$['bestaccessright'].classid\"\n" +
				"   },\n" +
				"   \"documentType\":{\n" +
				"      \"path\":\"$['instance'][*]['instancetype'].classname\"\n" +
				"   },\n" +
				"   \"language\":{\n" +
				"      \"path\":\"$['language'].classid\"\n" +
				"   },\n" +
				"   \"resultType\":{\n" +
				"      \"path\":\"$['resulttype'].classid\"\n" +
				"   },\n" +
				"   \"publicationyear\":{\n" +
				"      \"path\":\"$['dateofacceptance'].value\",\n" +
				"      \"action\":{\n" +
				"         \"clazz\":\"eu.dnetlib.dhp.bulktag.actions.ExecSubstringAction\",\n" +
				"         \"method\":\"execSubstring\",\n" +
				"         \"params\":[\n" +
				"            {\n" +
				"               \"paramName\":\"From\",\n" +
				"               \"paramValue\":0\n" +
				"            },\n" +
				"            {\n" +
				"               \"paramName\":\"To\",\n" +
				"               \"paramValue\":4\n" +
				"            }\n" +
				"         ]\n" +
				"      }\n" +
				"   }\n" +
				"}";
		String value = "{\"geolocation\": [], \"dataInfo\": {\"invisible\": false, \"provenanceaction\": {\"classid\": \"sysimport:crosswalk:repository\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"trust\": \"0.9\", \"inferred\": false, \"deletedbyinference\": false}, \"resourcetype\": {\"classid\": \"dataset\", \"classname\": \"dataset\", \"schemeid\": \"dnet:dataCite_resource\", \"schemename\": \"dnet:dataCite_resource\"}, \"pid\": [], \"contributor\": [], \"bestaccessright\": {\"classid\": \"OPEN\", \"classname\": \"Open Access\", \"schemeid\": \"dnet:access_modes\", \"schemename\": \"dnet:access_modes\"}, \"relevantdate\": [{\"dataInfo\": {\"invisible\": false, \"provenanceaction\": {\"classid\": \"sysimport:crosswalk:repository\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"trust\": \"0.9\", \"inferred\": false, \"deletedbyinference\": false}, \"qualifier\": {\"classid\": \"issued\", \"classname\": \"issued\", \"schemeid\": \"dnet:dataCite_date\", \"schemename\": \"dnet:dataCite_date\"}, \"value\": \"2021-10-12\"}], \"collectedfrom\": [{\"key\": \"10|re3data_____::c4b2081b224be6b3e79d0e5e5556f631\", \"value\": \"European Union Open Data Portal\"}], \"id\": \"50|r3c4b2081b22::21f4b7af9e5ee8e19006ee644e89dd0e\", \"subject\": [], \"lastupdatetimestamp\": 1752498154893, \"author\": [], \"instance\": [{\"refereed\": {\"classid\": \"0002\", \"classname\": \"nonPeerReviewed\", \"schemeid\": \"dnet:review_levels\", \"schemename\": \"dnet:review_levels\"}, \"hostedby\": {\"key\": \"10|re3data_____::c4b2081b224be6b3e79d0e5e5556f631\", \"value\": \"European Union Open Data Portal\"}, \"url\": [\"http://data.europa.eu/88u/dataset/united-kingdom-fish-landings-by-ices-rectangle-by-vessel-length-2011-web-mapping-service-wms\"], \"pid\": [], \"instanceTypeMapping\": [{\"originalType\": \"http://purl.org/coar/resource_type/c_ddb1\", \"typeLabel\": \"dataset\", \"vocabularyName\": \"openaire::coar_resource_types_3_1\", \"typeCode\": \"http://purl.org/coar/resource_type/c_ddb1\"}], \"alternateIdentifier\": [{\"qualifier\": {\"classid\": \"data.europa.eu\", \"classname\": \"EU Persistent URL\", \"schemeid\": \"dnet:pid_types\", \"schemename\": \"dnet:pid_types\"}, \"value\": \"http://data.europa.eu/88u/dataset/united-kingdom-fish-landings-by-ices-rectangle-by-vessel-length-2011-web-mapping-service-wms\"}], \"dateofacceptance\": {\"dataInfo\": {\"invisible\": false, \"provenanceaction\": {\"classid\": \"sysimport:crosswalk:repository\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"trust\": \"0.9\", \"inferred\": false, \"deletedbyinference\": false}, \"value\": \"2021-10-12\"}, \"collectedfrom\": {\"key\": \"10|re3data_____::c4b2081b224be6b3e79d0e5e5556f631\", \"value\": \"European Union Open Data Portal\"}, \"accessright\": {\"classid\": \"OPEN\", \"classname\": \"Open Access\", \"schemeid\": \"dnet:access_modes\", \"schemename\": \"dnet:access_modes\"}, \"instancetype\": {\"classid\": \"0021\", \"classname\": \"Dataset\", \"schemeid\": \"dnet:publication_resource\", \"schemename\": \"dnet:publication_resource\"}}], \"storagedate\": {\"dataInfo\": {\"invisible\": false, \"provenanceaction\": {\"classid\": \"sysimport:crosswalk:repository\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"trust\": \"0.9\", \"inferred\": false, \"deletedbyinference\": false}, \"value\": \"2021-10-12T06:02:51Z\"}, \"dateofcollection\": \"2025-07-19T15:09:43.986\", \"metaResourceType\": {\"classid\": \"Research Data\", \"classname\": \"Research Data\", \"schemeid\": \"openaire::meta_resource_types\", \"schemename\": \"openaire::meta_resource_types\"}, \"dateoftransformation\": \"2025-05-30T00:02:53.756Z\", \"description\": [{\"dataInfo\": {\"invisible\": false, \"provenanceaction\": {\"classid\": \"sysimport:crosswalk:repository\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"trust\": \"0.9\", \"inferred\": false, \"deletedbyinference\": false}, \"value\": \"Staitistic\\\\u00ed bliant\\\\u00fala maidir le gn\\\\u00edomha\\\\u00edocht iascaireachta tr\\\\u00e1cht\\\\u00e1la ag soith\\\\u00ed iascaireachta at\\\\u00e1 cl\\\\u00e1raithe sa R\\\\u00edocht Aontaithe le haghaidh na bliana a l\\\\u00e9ir\\\\u00edtear.D\\\\u00e9antar soith\\\\u00ed a ghr\\\\u00fap\\\\u00e1il de r\\\\u00e9ir a bhfad i m\\\\u00e9adair.  Cuirtear staidreamh ar f\\\\u00e1il maidir leis an Atlantach Thoir Thuaidh (M\\\\u00f3rlimist\\\\u00e9ar FAO 27) i nd\\\\u00e1il le leibh\\\\u00e9al dronuilleog staidrimh ICES de r\\\\u00e9ir trealamh iascaireachta agus fad an tsoithigh maidir leis na nithe seo a leanas:   iarracht iascaireachta (cileavata/laethanta) gabh\\\\u00e1lacha de r\\\\u00e9ir beomhe\\\\u00e1chain (tona\\\\u00ed), gabh\\\\u00e1lacha de r\\\\u00e9ir luacha (punt steirling), an l\\\\u00edon soith\\\\u00ed (f\\\\u00e9adfar an fhaisn\\\\u00e9is sin a fh\\\\u00e1g\\\\u00e1il ar l\\\\u00e1r \\\\u00f3 leaganacha foilsithe).  Taispe\\\\u00e1ntar cine\\\\u00e1lacha trealaimh ar leibh\\\\u00e9al an Aicmithe Idirn\\\\u00e1isi\\\\u00fanta Chaighde\\\\u00e1naigh Staidrimh ar Threalamh Iascaireachta (ISSCFG) agus d\\\\u00e9antar iad a chomhioml\\\\u00e1n\\\\u00fa i ngr\\\\u00fapa\\\\u00ed \\\\u00e9ags\\\\u00fala freisin chun gur f\\\\u00e9idir anail\\\\u00eds a dh\\\\u00e9anamh ar leibh\\\\u00e9il \\\\u00e9ags\\\\u00fala.  D\\\\u00e9antar faid na soith\\\\u00ed a chomhioml\\\\u00e1n\\\\u00fa mar a leanas:  10m agus faoina bhun, n\\\\u00edos m\\\\u00f3 n\\\\u00e1 10-12m, n\\\\u00edos m\\\\u00f3 n\\\\u00e1 12 faoi bhun 15m, 15m agus os a chionn. Staitistic\\\\u00ed bliant\\\\u00fala maidir le gn\\\\u00edomha\\\\u00edocht iascaireachta tr\\\\u00e1cht\\\\u00e1la ag soith\\\\u00ed iascaireachta at\\\\u00e1 cl\\\\u00e1raithe sa R\\\\u00edocht Aontaithe le haghaidh na bliana a l\\\\u00e9ir\\\\u00edtear. D\\\\u00e9antar soith\\\\u00ed a ghr\\\\u00fap\\\\u00e1il de r\\\\u00e9ir a bhfad i m\\\\u00e9adair.    Cuirtear staidreamh ar f\\\\u00e1il maidir leis an Atlantach Thoir Thuaidh (M\\\\u00f3rlimist\\\\u00e9ar FAO 27) i nd\\\\u00e1il le leibh\\\\u00e9al dronuilleog staidrimh ICES de r\\\\u00e9ir trealamh iascaireachta agus fad an tsoithigh maidir leis na nithe seo a leanas:    iarracht iascaireachta (cileavata/laethanta)  gabh\\\\u00e1lacha de r\\\\u00e9ir beomhe\\\\u00e1chain (tona\\\\u00ed),  gabh\\\\u00e1lacha de r\\\\u00e9ir luacha (punt steirling),  an l\\\\u00edon soith\\\\u00ed (f\\\\u00e9adfar an fhaisn\\\\u00e9is sin a fh\\\\u00e1g\\\\u00e1il ar l\\\\u00e1r \\\\u00f3 leaganacha foilsithe).    Taispe\\\\u00e1ntar cine\\\\u00e1lacha trealaimh ar leibh\\\\u00e9al an Aicmithe Idirn\\\\u00e1isi\\\\u00fanta Chaighde\\\\u00e1naigh Staidrimh ar Threalamh Iascaireachta (ISSCFG) agus d\\\\u00e9antar iad a chomhioml\\\\u00e1n\\\\u00fa i ngr\\\\u00fapa\\\\u00ed \\\\u00e9ags\\\\u00fala freisin chun gur f\\\\u00e9idir anail\\\\u00eds a dh\\\\u00e9anamh ar leibh\\\\u00e9il \\\\u00e9ags\\\\u00fala.    D\\\\u00e9antar faid na soith\\\\u00ed a chomhioml\\\\u00e1n\\\\u00fa mar a leanas:    10m agus faoina bhun,  n\\\\u00edos m\\\\u00f3 n\\\\u00e1 10-12m,  n\\\\u00edos m\\\\u00f3 n\\\\u00e1 12 faoi bhun 15m,  15m agus os a chionn.\"}, {\"dataInfo\": {\"invisible\": false, \"provenanceaction\": {\"classid\": \"sysimport:crosswalk:repository\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"trust\": \"0.9\", \"inferred\": false, \"deletedbyinference\": false}, \"value\": \"Jaarlijkse statistieken van de commerci\\\\u00eble visserijactiviteiten van in het Verenigd Koninkrijk geregistreerde vissersvaartuigen voor het aangegeven jaar.De vaartuigen worden gegroepeerd op basis van hun lengte in meters.   Er worden statistieken verstrekt voor het noordoostelijke deel van de Atlantische Oceaan (FAO-groot gebied 27) tot op het statistische rechthoekniveau van de ICES, uitgesplitst naar vistuig en vaartuiglengte, over:  visserijinspanning (kilowatt/dagen) aanlandingen in levend gewicht (ton), aanlandingen naar waarde (ponden sterling), aantal vaartuigen (deze informatie mag in gepubliceerde versies worden weggelaten).  Soorten vistuig worden weergegeven op het niveau van de International Standard Statistical Classification of Fishing Gear (ISSCFG) en ook geaggregeerd in verschillende groepen om analyse op verschillende niveaus mogelijk te maken.  De lengten van het vaartuig worden als volgt geaggregeerd:  10m en onder, meer dan 10-12 m, meer dan 12-onder 15m, 15m en meer.Jaarlijkse statistieken van de commerci\\\\u00eble visserijactiviteiten van in het Verenigd Koninkrijk geregistreerde vissersvaartuigen voor het aangegeven jaar. De vaartuigen worden gegroepeerd op basis van hun lengte in meters.  Er worden statistieken verstrekt voor het noordoostelijke deel van de Atlantische Oceaan (FAO-groot gebied 27) tot op het statistische rechthoekniveau van de ICES, uitgesplitst naar vistuig en vaartuiglengte, over:  visserijinspanning (kilowatt/dagen) aanlandingen in levend gewicht (ton), aanlandingen naar waarde (ponden sterling), aantal vaartuigen (deze informatie mag in gepubliceerde versies worden weggelaten).  Soorten vistuig worden weergegeven op het niveau van de International Standard Statistical Classification of Fishing Gear (ISSCFG) en ook geaggregeerd in verschillende groepen om analyse op verschillende niveaus mogelijk te maken.  De lengten van het vaartuig worden als volgt geaggregeerd:  10m en onder,  meer dan 10-12 m,  meer dan 12-onder 15m,  15m en meer.\"}, {\"dataInfo\": {\"invisible\": false, \"provenanceaction\": {\"classid\": \"sysimport:crosswalk:repository\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"trust\": \"0.9\", \"inferred\": false, \"deletedbyinference\": false}, \"value\": \"Az Egyes\\\\u00fclt Kir\\\\u00e1lys\\\\u00e1gban lajstromozott hal\\\\u00e1szhaj\\\\u00f3k kereskedelmi hal\\\\u00e1szati tev\\\\u00e9kenys\\\\u00e9g\\\\u00e9re vonatkoz\\\\u00f3 \\\\u00e9ves statisztika a megjel\\\\u00f6lt \\\\u00e9vre vonatkoz\\\\u00f3an.A haj\\\\u00f3kat m\\\\u00e9terben kifejezett hosszuk szerint csoportos\\\\u00edtj\\\\u00e1k.  Statisztikai adatok \\\\u00e1llnak rendelkez\\\\u00e9sre az Atlanti-\\\\u00f3ce\\\\u00e1n \\\\u00e9szakkeleti r\\\\u00e9sz\\\\u00e9re (a FAO 27. f\\\\u0151 ter\\\\u00fclete) \\\\u00e9s az ICES statisztikai n\\\\u00e9gysz\\\\u00f6g szintj\\\\u00e9re vonatkoz\\\\u00f3an hal\\\\u00e1szeszk\\\\u00f6z\\\\u00f6k \\\\u00e9s haj\\\\u00f3hossz szerint a k\\\\u00f6vetkez\\\\u0151kr\\\\u0151l:   hal\\\\u00e1szati er\\\\u0151kifejt\\\\u00e9s (kilowatt/nap) kirakod\\\\u00e1sok \\\\u00e9l\\\\u0151t\\\\u00f6meg szerint (tonna), kirakod\\\\u00e1sok \\\\u00e9rt\\\\u00e9k szerint (font sterling), a haj\\\\u00f3k sz\\\\u00e1ma (ez az inform\\\\u00e1ci\\\\u00f3 elhagyhat\\\\u00f3 a k\\\\u00f6zz\\\\u00e9tett v\\\\u00e1ltozatokb\\\\u00f3l).  A hal\\\\u00e1szeszk\\\\u00f6z-t\\\\u00edpusokat a hal\\\\u00e1szeszk\\\\u00f6z\\\\u00f6k egys\\\\u00e9ges nemzetk\\\\u00f6zi statisztikai oszt\\\\u00e1lyoz\\\\u00e1si rendszere (ISSCFG) szintj\\\\u00e9n t\\\\u00fcntetik fel, \\\\u00e9s k\\\\u00fcl\\\\u00f6nb\\\\u00f6z\\\\u0151 csoportokba is csoportos\\\\u00edtj\\\\u00e1k, hogy lehet\\\\u0151v\\\\u00e9 tegy\\\\u00e9k a k\\\\u00fcl\\\\u00f6nb\\\\u00f6z\\\\u0151 szinteken t\\\\u00f6rt\\\\u00e9n\\\\u0151 elemz\\\\u00e9st.  A haj\\\\u00f3hosszakat a k\\\\u00f6vetkez\\\\u0151k\\\\u00e9ppen kell \\\\u00f6sszes\\\\u00edteni:  10 m\\\\u00e9ter \\\\u00e9s alatta, 10\\\\u201312 m\\\\u00e9ter felett, 12 \\\\u00e9s 15 m\\\\u00e9ter k\\\\u00f6z\\\\u00f6tt, 15 m\\\\u00e9ter \\\\u00e9s felette. Az Egyes\\\\u00fclt Kir\\\\u00e1lys\\\\u00e1gban lajstromozott hal\\\\u00e1szhaj\\\\u00f3k kereskedelmi hal\\\\u00e1szati tev\\\\u00e9kenys\\\\u00e9g\\\\u00e9re vonatkoz\\\\u00f3 \\\\u00e9ves statisztika a megjel\\\\u00f6lt \\\\u00e9vre vonatkoz\\\\u00f3an. A haj\\\\u00f3kat m\\\\u00e9terben kifejezett hosszuk szerint csoportos\\\\u00edtj\\\\u00e1k.    Statisztikai adatok \\\\u00e1llnak rendelkez\\\\u00e9sre az Atlanti-\\\\u00f3ce\\\\u00e1n \\\\u00e9szakkeleti r\\\\u00e9sz\\\\u00e9re (a FAO 27. f\\\\u0151 ter\\\\u00fclete) \\\\u00e9s az ICES statisztikai n\\\\u00e9gysz\\\\u00f6g szintj\\\\u00e9re vonatkoz\\\\u00f3an hal\\\\u00e1szeszk\\\\u00f6z\\\\u00f6k \\\\u00e9s haj\\\\u00f3hossz szerint a k\\\\u00f6vetkez\\\\u0151kr\\\\u0151l:    hal\\\\u00e1szati er\\\\u0151kifejt\\\\u00e9s (kilowatt/nap)  kirakod\\\\u00e1sok \\\\u00e9l\\\\u0151t\\\\u00f6meg szerint (tonna),  kirakod\\\\u00e1sok \\\\u00e9rt\\\\u00e9k szerint (font sterling),  a haj\\\\u00f3k sz\\\\u00e1ma (ez az inform\\\\u00e1ci\\\\u00f3 elhagyhat\\\\u00f3 a k\\\\u00f6zz\\\\u00e9tett v\\\\u00e1ltozatokb\\\\u00f3l).    A hal\\\\u00e1szeszk\\\\u00f6z-t\\\\u00edpusokat a hal\\\\u00e1szeszk\\\\u00f6z\\\\u00f6k egys\\\\u00e9ges nemzetk\\\\u00f6zi statisztikai oszt\\\\u00e1lyoz\\\\u00e1si rendszere (ISSCFG) szintj\\\\u00e9n t\\\\u00fcntetik fel, \\\\u00e9s k\\\\u00fcl\\\\u00f6nb\\\\u00f6z\\\\u0151 csoportokba is csoportos\\\\u00edtj\\\\u00e1k, hogy lehet\\\\u0151v\\\\u00e9 tegy\\\\u00e9k a k\\\\u00fcl\\\\u00f6nb\\\\u00f6z\\\\u0151 szinteken t\\\\u00f6rt\\\\u00e9n\\\\u0151 elemz\\\\u00e9st.    A haj\\\\u00f3hosszakat a k\\\\u00f6vetkez\\\\u0151k\\\\u00e9ppen kell \\\\u00f6sszes\\\\u00edteni:    10 m\\\\u00e9ter \\\\u00e9s alatta,  10\\\\u201312 m\\\\u00e9ter felett,  12 \\\\u00e9s 15 m\\\\u00e9ter k\\\\u00f6z\\\\u00f6tt,  15 m\\\\u00e9ter \\\\u00e9s felette.\"}, {\"dataInfo\": {\"invisible\": false, \"provenanceaction\": {\"classid\": \"sysimport:crosswalk:repository\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"trust\": \"0.9\", \"inferred\": false, \"deletedbyinference\": false}, \"value\": \"J\\\\u00e4hrliche Statistiken \\\\u00fcber die gewerbliche Fischereit\\\\u00e4tigkeit der im Vereinigten K\\\\u00f6nigreich registrierten Fischereifahrzeuge f\\\\u00fcr das angegebene Jahr.Die Schiffe werden nach ihrer L\\\\u00e4nge in Metern gruppiert.   Es werden Statistiken f\\\\u00fcr den Nordostatlantik (FAO-Hauptgebiet 27) bis zur statistischen Rechteckebene des ICES nach Fangger\\\\u00e4t und Schiffsl\\\\u00e4nge bereitgestellt \\\\u00fcber:  Fischereiaufwand (Kilowatt/Tag) Anlandungen nach Lebendgewicht (in Tonnen), Anlandungen nach Wert (Pfund Sterling), Anzahl der Schiffe (diese Angaben k\\\\u00f6nnen in den ver\\\\u00f6ffentlichten Fassungen weggelassen werden).  Die Fangger\\\\u00e4tetypen werden auf der Ebene der Internationalen statistischen Standardklassifikation f\\\\u00fcr Fangger\\\\u00e4te (ISSCFG) angezeigt und auch in verschiedene Gruppen aggregiert, um eine Analyse auf verschiedenen Ebenen zu erm\\\\u00f6glichen.  Die Schiffsl\\\\u00e4ngen werden wie folgt aggregiert:  10m und darunter, \\\\u00fcber 10-12m, \\\\u00fcber 12-unter 15m, 15m und mehr.J\\\\u00e4hrliche Statistiken \\\\u00fcber die gewerbliche Fischereit\\\\u00e4tigkeit der im Vereinigten K\\\\u00f6nigreich registrierten Fischereifahrzeuge f\\\\u00fcr das angegebene Jahr. Die Schiffe werden nach ihrer L\\\\u00e4nge in Metern gruppiert.  Es werden Statistiken f\\\\u00fcr den Nordostatlantik (FAO-Hauptgebiet 27) bis zur statistischen Rechteckebene des ICES nach Fangger\\\\u00e4t und Schiffsl\\\\u00e4nge bereitgestellt \\\\u00fcber:  Fischereiaufwand (Kilowatt/Tag) Anlandungen nach Lebendgewicht (in Tonnen), Anlandungen nach Wert (Pfund Sterling), Anzahl der Schiffe (diese Angaben k\\\\u00f6nnen in den ver\\\\u00f6ffentlichten Fassungen weggelassen werden).  Die Fangger\\\\u00e4tetypen werden auf der Ebene der Internationalen statistischen Standardklassifikation f\\\\u00fcr Fangger\\\\u00e4te (ISSCFG) angezeigt und auch in verschiedene Gruppen aggregiert, um eine Analyse auf verschiedenen Ebenen zu erm\\\\u00f6glichen.  Die Schiffsl\\\\u00e4ngen werden wie folgt aggregiert:  10m und darunter,  \\\\u00fcber 10-12m,  \\\\u00fcber 12-unter 15m,  15m und mehr.\"}, {\"dataInfo\": {\"invisible\": false, \"provenanceaction\": {\"classid\": \"sysimport:crosswalk:repository\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"trust\": \"0.9\", \"inferred\": false, \"deletedbyinference\": false}, \"value\": \"Statistiques annuelles de l\\'activit\\\\u00e9 de p\\\\u00eache commerciale des navires de p\\\\u00eache immatricul\\\\u00e9s au Royaume-Uni pour l\\'ann\\\\u00e9e indiqu\\\\u00e9e.Les navires sont regroup\\\\u00e9s en fonction de leur longueur en m\\\\u00e8tres.  Des statistiques sont fournies pour l\\'Atlantique du Nord-Est (zone principale 27 de la FAO) jusqu\\'au niveau du rectangle statistique CIEM par engin de p\\\\u00eache et longueur du navire sur:   effort de p\\\\u00eache (kilowatts/jours) d\\\\u00e9barquements en poids vif (tonnes), d\\\\u00e9barquements en valeur (livres sterling), nombre de navires (cette information peut \\\\u00eatre omise dans les versions publi\\\\u00e9es).  Les types d\\'engins sont pr\\\\u00e9sent\\\\u00e9s au niveau de la Classification statistique internationale type des engins de p\\\\u00eache (ISSCFG) et \\\\u00e9galement agr\\\\u00e9g\\\\u00e9s en diff\\\\u00e9rents groupes pour permettre une analyse \\\\u00e0 diff\\\\u00e9rents niveaux.  Les longueurs des navires sont agr\\\\u00e9g\\\\u00e9es comme suit:  10m et moins, plus de 10-12m, plus de 12-moins de 15m, 15m et plus. Statistiques annuelles de l\\'activit\\\\u00e9 de p\\\\u00eache commerciale des navires de p\\\\u00eache immatricul\\\\u00e9s au Royaume-Uni pour l\\'ann\\\\u00e9e indiqu\\\\u00e9e. Les navires sont regroup\\\\u00e9s en fonction de leur longueur en m\\\\u00e8tres.    Des statistiques sont fournies pour l\\'Atlantique du Nord-Est (zone principale 27 de la FAO) jusqu\\'au niveau du rectangle statistique CIEM par engin de p\\\\u00eache et longueur du navire sur:    effort de p\\\\u00eache (kilowatts/jours)  d\\\\u00e9barquements en poids vif (tonnes),  d\\\\u00e9barquements en valeur (livres sterling),  nombre de navires (cette information peut \\\\u00eatre omise dans les versions publi\\\\u00e9es).    Les types d\\'engins sont pr\\\\u00e9sent\\\\u00e9s au niveau de la Classification statistique internationale type des engins de p\\\\u00eache (ISSCFG) et \\\\u00e9galement agr\\\\u00e9g\\\\u00e9s en diff\\\\u00e9rents groupes pour permettre une analyse \\\\u00e0 diff\\\\u00e9rents niveaux.    Les longueurs des navires sont agr\\\\u00e9g\\\\u00e9es comme suit:    10m et moins,  plus de 10-12m,  plus de 12-moins de 15m,  15m et plus.\"}, {\"dataInfo\": {\"invisible\": false, \"provenanceaction\": {\"classid\": \"sysimport:crosswalk:repository\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"trust\": \"0.9\", \"inferred\": false, \"deletedbyinference\": false}, \"value\": \"Roczne statystyki dotycz\\\\u0105ce komercyjnej dzia\\\\u0142alno\\\\u015bci po\\\\u0142owowej statk\\\\u00f3w rybackich zarejestrowanych w Zjednoczonym Kr\\\\u00f3lestwie za wskazany rok.Statki s\\\\u0105 pogrupowane wed\\\\u0142ug ich d\\\\u0142ugo\\\\u015bci w metrach.  Przekazywane s\\\\u0105 statystyki dotycz\\\\u0105ce p\\\\u00f3\\\\u0142nocno-wschodniego Atlantyku (g\\\\u0142\\\\u00f3wny obszar FAO 27) do poziomu prostok\\\\u0105ta statystycznego ICES w podziale na narz\\\\u0119dzia po\\\\u0142owowe i d\\\\u0142ugo\\\\u015b\\\\u0107 statku w odniesieniu do:   nak\\\\u0142ad po\\\\u0142owowy (kilowatodni) wy\\\\u0142adunki wed\\\\u0142ug \\\\u017cywej wagi (w tonach), wy\\\\u0142adunki wed\\\\u0142ug warto\\\\u015bci (funty szterlingi), liczba statk\\\\u00f3w (informacje te mo\\\\u017cna pomin\\\\u0105\\\\u0107 w opublikowanych wersjach).  Rodzaje narz\\\\u0119dzi przedstawiono na poziomie Mi\\\\u0119dzynarodowej Standardowej Klasyfikacji Statystycznej Narz\\\\u0119dzi Po\\\\u0142owowych (ISSCFG), a tak\\\\u017ce po\\\\u0142\\\\u0105czono je w r\\\\u00f3\\\\u017cne grupy, aby umo\\\\u017cliwi\\\\u0107 analiz\\\\u0119 na r\\\\u00f3\\\\u017cnych poziomach.  D\\\\u0142ugo\\\\u015b\\\\u0107 statku agreguje si\\\\u0119 w nast\\\\u0119puj\\\\u0105cy spos\\\\u00f3b:  10m i poni\\\\u017cej, powy\\\\u017cej 10-12m, powy\\\\u017cej 12 poni\\\\u017cej 15m, 15m i wi\\\\u0119cej. Roczne statystyki dotycz\\\\u0105ce komercyjnej dzia\\\\u0142alno\\\\u015bci po\\\\u0142owowej statk\\\\u00f3w rybackich zarejestrowanych w Zjednoczonym Kr\\\\u00f3lestwie za wskazany rok. Statki s\\\\u0105 pogrupowane wed\\\\u0142ug ich d\\\\u0142ugo\\\\u015bci w metrach.    Przekazywane s\\\\u0105 statystyki dotycz\\\\u0105ce p\\\\u00f3\\\\u0142nocno-wschodniego Atlantyku (g\\\\u0142\\\\u00f3wny obszar FAO 27) do poziomu prostok\\\\u0105ta statystycznego ICES w podziale na narz\\\\u0119dzia po\\\\u0142owowe i d\\\\u0142ugo\\\\u015b\\\\u0107 statku w odniesieniu do:    nak\\\\u0142ad po\\\\u0142owowy (kilowatodni)  wy\\\\u0142adunki wed\\\\u0142ug \\\\u017cywej wagi (w tonach),  wy\\\\u0142adunki wed\\\\u0142ug warto\\\\u015bci (funty szterlingi),  liczba statk\\\\u00f3w (informacje te mo\\\\u017cna pomin\\\\u0105\\\\u0107 w opublikowanych wersjach).    Rodzaje narz\\\\u0119dzi przedstawiono na poziomie Mi\\\\u0119dzynarodowej Standardowej Klasyfikacji Statystycznej Narz\\\\u0119dzi Po\\\\u0142owowych (ISSCFG), a tak\\\\u017ce po\\\\u0142\\\\u0105czono je w r\\\\u00f3\\\\u017cne grupy, aby umo\\\\u017cliwi\\\\u0107 analiz\\\\u0119 na r\\\\u00f3\\\\u017cnych poziomach.    D\\\\u0142ugo\\\\u015b\\\\u0107 statku agreguje si\\\\u0119 w nast\\\\u0119puj\\\\u0105cy spos\\\\u00f3b:    10m i poni\\\\u017cej,  powy\\\\u017cej 10-12m,  powy\\\\u017cej 12 poni\\\\u017cej 15m,  15m i wi\\\\u0119cej.\"}, {\"dataInfo\": {\"invisible\": false, \"provenanceaction\": {\"classid\": \"sysimport:crosswalk:repository\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"trust\": \"0.9\", \"inferred\": false, \"deletedbyinference\": false}, \"value\": \"\\\\u0413\\\\u043e\\\\u0434\\\\u0438\\\\u0448\\\\u043d\\\\u0430 \\\\u0441\\\\u0442\\\\u0430\\\\u0442\\\\u0438\\\\u0441\\\\u0442\\\\u0438\\\\u043a\\\\u0430 \\\\u0437\\\\u0430 \\\\u0442\\\\u044a\\\\u0440\\\\u0433\\\\u043e\\\\u0432\\\\u0441\\\\u043a\\\\u0430\\\\u0442\\\\u0430 \\\\u0440\\\\u0438\\\\u0431\\\\u043e\\\\u043b\\\\u043e\\\\u0432\\\\u043d\\\\u0430 \\\\u0434\\\\u0435\\\\u0439\\\\u043d\\\\u043e\\\\u0441\\\\u0442 \\\\u043d\\\\u0430 \\\\u0440\\\\u0435\\\\u0433\\\\u0438\\\\u0441\\\\u0442\\\\u0440\\\\u0438\\\\u0440\\\\u0430\\\\u043d\\\\u0438\\\\u0442\\\\u0435 \\\\u0432 \\\\u041e\\\\u0431\\\\u0435\\\\u0434\\\\u0438\\\\u043d\\\\u0435\\\\u043d\\\\u043e\\\\u0442\\\\u043e \\\\u043a\\\\u0440\\\\u0430\\\\u043b\\\\u0441\\\\u0442\\\\u0432\\\\u043e \\\\u0440\\\\u0438\\\\u0431\\\\u043e\\\\u043b\\\\u043e\\\\u0432\\\\u043d\\\\u0438 \\\\u043a\\\\u043e\\\\u0440\\\\u0430\\\\u0431\\\\u0438 \\\\u0437\\\\u0430 \\\\u043f\\\\u043e\\\\u0441\\\\u043e\\\\u0447\\\\u0435\\\\u043d\\\\u0430\\\\u0442\\\\u0430 \\\\u0433\\\\u043e\\\\u0434\\\\u0438\\\\u043d\\\\u0430.\\\\u041a\\\\u043e\\\\u0440\\\\u0430\\\\u0431\\\\u0438\\\\u0442\\\\u0435 \\\\u0441\\\\u0435 \\\\u0433\\\\u0440\\\\u0443\\\\u043f\\\\u0438\\\\u0440\\\\u0430\\\\u0442 \\\\u0441\\\\u043f\\\\u043e\\\\u0440\\\\u0435\\\\u0434 \\\\u0434\\\\u044a\\\\u043b\\\\u0436\\\\u0438\\\\u043d\\\\u0430\\\\u0442\\\\u0430 \\\\u0438\\\\u043c \\\\u0432 \\\\u043c\\\\u0435\\\\u0442\\\\u0440\\\\u0438.   \\\\u041f\\\\u0440\\\\u0435\\\\u0434\\\\u043e\\\\u0441\\\\u0442\\\\u0430\\\\u0432\\\\u044f\\\\u0442 \\\\u0441\\\\u0435 \\\\u0441\\\\u0442\\\\u0430\\\\u0442\\\\u0438\\\\u0441\\\\u0442\\\\u0438\\\\u0447\\\\u0435\\\\u0441\\\\u043a\\\\u0438 \\\\u0434\\\\u0430\\\\u043d\\\\u043d\\\\u0438 \\\\u0437\\\\u0430 \\\\u0441\\\\u0435\\\\u0432\\\\u0435\\\\u0440\\\\u043e\\\\u0438\\\\u0437\\\\u0442\\\\u043e\\\\u0447\\\\u043d\\\\u0430\\\\u0442\\\\u0430 \\\\u0447\\\\u0430\\\\u0441\\\\u0442 \\\\u043d\\\\u0430 \\\\u0410\\\\u0442\\\\u043b\\\\u0430\\\\u043d\\\\u0442\\\\u0438\\\\u0447\\\\u0435\\\\u0441\\\\u043a\\\\u0438\\\\u044f \\\\u043e\\\\u043a\\\\u0435\\\\u0430\\\\u043d (\\\\u043c\\\\u0430\\\\u0439\\\\u043e\\\\u0440\\\\u043d\\\\u0430 \\\\u0437\\\\u043e\\\\u043d\\\\u0430 27 \\\\u043d\\\\u0430 \\\\u0424\\\\u0410\\\\u041e) \\\\u0434\\\\u043e \\\\u043d\\\\u0438\\\\u0432\\\\u043e\\\\u0442\\\\u043e \\\\u043d\\\\u0430 \\\\u0441\\\\u0442\\\\u0430\\\\u0442\\\\u0438\\\\u0441\\\\u0442\\\\u0438\\\\u0447\\\\u0435\\\\u0441\\\\u043a\\\\u0438\\\\u044f \\\\u043f\\\\u0440\\\\u0430\\\\u0432\\\\u043e\\\\u044a\\\\u0433\\\\u044a\\\\u043b\\\\u043d\\\\u0438\\\\u043a \\\\u043d\\\\u0430 ICES \\\\u043f\\\\u043e \\\\u0440\\\\u0438\\\\u0431\\\\u043e\\\\u043b\\\\u043e\\\\u0432\\\\u043d\\\\u0438 \\\\u0443\\\\u0440\\\\u0435\\\\u0434\\\\u0438 \\\\u0438 \\\\u0434\\\\u044a\\\\u043b\\\\u0436\\\\u0438\\\\u043d\\\\u0430 \\\\u043d\\\\u0430 \\\\u043a\\\\u043e\\\\u0440\\\\u0430\\\\u0431\\\\u0430 \\\\u043e\\\\u0442\\\\u043d\\\\u043e\\\\u0441\\\\u043d\\\\u043e:  \\\\u0440\\\\u0438\\\\u0431\\\\u043e\\\\u043b\\\\u043e\\\\u0432\\\\u043d\\\\u043e \\\\u0443\\\\u0441\\\\u0438\\\\u043b\\\\u0438\\\\u0435 (\\\\u043a\\\\u0438\\\\u043b\\\\u043e\\\\u0432\\\\u0430\\\\u0442/\\\\u0434\\\\u043d\\\\u0438) \\\\u0440\\\\u0430\\\\u0437\\\\u0442\\\\u043e\\\\u0432\\\\u0430\\\\u0440\\\\u0432\\\\u0430\\\\u043d\\\\u0438\\\\u044f \\\\u043f\\\\u043e \\\\u0436\\\\u0438\\\\u0432\\\\u043e \\\\u0442\\\\u0435\\\\u0433\\\\u043b\\\\u043e (\\\\u0432 \\\\u0442\\\\u043e\\\\u043d\\\\u043e\\\\u0432\\\\u0435), \\\\u0440\\\\u0430\\\\u0437\\\\u0442\\\\u043e\\\\u0432\\\\u0430\\\\u0440\\\\u0432\\\\u0430\\\\u043d\\\\u0438\\\\u044f \\\\u043f\\\\u043e \\\\u0441\\\\u0442\\\\u043e\\\\u0439\\\\u043d\\\\u043e\\\\u0441\\\\u0442 (\\\\u0441\\\\u0442\\\\u0435\\\\u0440\\\\u043b\\\\u0438\\\\u043d\\\\u0433\\\\u0438), \\\\u0431\\\\u0440\\\\u043e\\\\u0439 \\\\u043a\\\\u043e\\\\u0440\\\\u0430\\\\u0431\\\\u0438 (\\\\u0442\\\\u0430\\\\u0437\\\\u0438 \\\\u0438\\\\u043d\\\\u0444\\\\u043e\\\\u0440\\\\u043c\\\\u0430\\\\u0446\\\\u0438\\\\u044f \\\\u043c\\\\u043e\\\\u0436\\\\u0435 \\\\u0434\\\\u0430 \\\\u0431\\\\u044a\\\\u0434\\\\u0435 \\\\u043f\\\\u0440\\\\u043e\\\\u043f\\\\u0443\\\\u0441\\\\u043d\\\\u0430\\\\u0442\\\\u0430 \\\\u043e\\\\u0442 \\\\u043f\\\\u0443\\\\u0431\\\\u043b\\\\u0438\\\\u043a\\\\u0443\\\\u0432\\\\u0430\\\\u043d\\\\u0438\\\\u0442\\\\u0435 \\\\u0432\\\\u0435\\\\u0440\\\\u0441\\\\u0438\\\\u0438).  \\\\u0412\\\\u0438\\\\u0434\\\\u043e\\\\u0432\\\\u0435\\\\u0442\\\\u0435 \\\\u0443\\\\u0440\\\\u0435\\\\u0434\\\\u0438 \\\\u0441\\\\u0430 \\\\u043f\\\\u043e\\\\u043a\\\\u0430\\\\u0437\\\\u0430\\\\u043d\\\\u0438 \\\\u043d\\\\u0430 \\\\u0440\\\\u0430\\\\u0432\\\\u043d\\\\u0438\\\\u0449\\\\u0435\\\\u0442\\\\u043e \\\\u043d\\\\u0430 \\\\u041c\\\\u0435\\\\u0436\\\\u0434\\\\u0443\\\\u043d\\\\u0430\\\\u0440\\\\u043e\\\\u0434\\\\u043d\\\\u0430\\\\u0442\\\\u0430 \\\\u0441\\\\u0442\\\\u0430\\\\u043d\\\\u0434\\\\u0430\\\\u0440\\\\u0442\\\\u043d\\\\u0430 \\\\u0441\\\\u0442\\\\u0430\\\\u0442\\\\u0438\\\\u0441\\\\u0442\\\\u0438\\\\u0447\\\\u0435\\\\u0441\\\\u043a\\\\u0430 \\\\u043a\\\\u043b\\\\u0430\\\\u0441\\\\u0438\\\\u0444\\\\u0438\\\\u043a\\\\u0430\\\\u0446\\\\u0438\\\\u044f \\\\u043d\\\\u0430 \\\\u0440\\\\u0438\\\\u0431\\\\u043e\\\\u043b\\\\u043e\\\\u0432\\\\u043d\\\\u0438\\\\u0442\\\\u0435 \\\\u0441\\\\u044a\\\\u043e\\\\u0440\\\\u044a\\\\u0436\\\\u0435\\\\u043d\\\\u0438\\\\u044f (ISSCFG) \\\\u0438 \\\\u0441\\\\u0430 \\\\u043e\\\\u0431\\\\u043e\\\\u0431\\\\u0449\\\\u0435\\\\u043d\\\\u0438 \\\\u0432 \\\\u0440\\\\u0430\\\\u0437\\\\u043b\\\\u0438\\\\u0447\\\\u043d\\\\u0438 \\\\u0433\\\\u0440\\\\u0443\\\\u043f\\\\u0438, \\\\u0437\\\\u0430 \\\\u0434\\\\u0430 \\\\u0441\\\\u0435 \\\\u0434\\\\u0430\\\\u0434\\\\u0435 \\\\u0432\\\\u044a\\\\u0437\\\\u043c\\\\u043e\\\\u0436\\\\u043d\\\\u043e\\\\u0441\\\\u0442 \\\\u0437\\\\u0430 \\\\u0430\\\\u043d\\\\u0430\\\\u043b\\\\u0438\\\\u0437 \\\\u043d\\\\u0430 \\\\u0440\\\\u0430\\\\u0437\\\\u043b\\\\u0438\\\\u0447\\\\u043d\\\\u0438 \\\\u0440\\\\u0430\\\\u0432\\\\u043d\\\\u0438\\\\u0449\\\\u0430.  \\\\u0414\\\\u044a\\\\u043b\\\\u0436\\\\u0438\\\\u043d\\\\u0438\\\\u0442\\\\u0435 \\\\u043d\\\\u0430 \\\\u043a\\\\u043e\\\\u0440\\\\u0430\\\\u0431\\\\u0430 \\\\u0441\\\\u0435 \\\\u0441\\\\u0443\\\\u043c\\\\u0438\\\\u0440\\\\u0430\\\\u0442, \\\\u043a\\\\u0430\\\\u043a\\\\u0442\\\\u043e \\\\u0441\\\\u043b\\\\u0435\\\\u0434\\\\u0432\\\\u0430:  10\\\\u00a0m \\\\u0438 \\\\u043f\\\\u043e-\\\\u0434\\\\u043e\\\\u043b\\\\u0443, \\\\u043d\\\\u0430\\\\u0434 10\\\\u201412 m, \\\\u043d\\\\u0430\\\\u0434 12-\\\\u043f\\\\u043e\\\\u0434 15\\\\u00a0m, 15 \\\\u043c \\\\u0438 \\\\u043f\\\\u043e\\\\u0432\\\\u0435\\\\u0447\\\\u0435.\\\\u0413\\\\u043e\\\\u0434\\\\u0438\\\\u0448\\\\u043d\\\\u0430 \\\\u0441\\\\u0442\\\\u0430\\\\u0442\\\\u0438\\\\u0441\\\\u0442\\\\u0438\\\\u043a\\\\u0430 \\\\u0437\\\\u0430 \\\\u0442\\\\u044a\\\\u0440\\\\u0433\\\\u043e\\\\u0432\\\\u0441\\\\u043a\\\\u0430\\\\u0442\\\\u0430 \\\\u0440\\\\u0438\\\\u0431\\\\u043e\\\\u043b\\\\u043e\\\\u0432\\\\u043d\\\\u0430 \\\\u0434\\\\u0435\\\\u0439\\\\u043d\\\\u043e\\\\u0441\\\\u0442 \\\\u043d\\\\u0430 \\\\u0440\\\\u0435\\\\u0433\\\\u0438\\\\u0441\\\\u0442\\\\u0440\\\\u0438\\\\u0440\\\\u0430\\\\u043d\\\\u0438\\\\u0442\\\\u0435 \\\\u0432 \\\\u041e\\\\u0431\\\\u0435\\\\u0434\\\\u0438\\\\u043d\\\\u0435\\\\u043d\\\\u043e\\\\u0442\\\\u043e \\\\u043a\\\\u0440\\\\u0430\\\\u043b\\\\u0441\\\\u0442\\\\u0432\\\\u043e \\\\u0440\\\\u0438\\\\u0431\\\\u043e\\\\u043b\\\\u043e\\\\u0432\\\\u043d\\\\u0438 \\\\u043a\\\\u043e\\\\u0440\\\\u0430\\\\u0431\\\\u0438 \\\\u0437\\\\u0430 \\\\u043f\\\\u043e\\\\u0441\\\\u043e\\\\u0447\\\\u0435\\\\u043d\\\\u0430\\\\u0442\\\\u0430 \\\\u0433\\\\u043e\\\\u0434\\\\u0438\\\\u043d\\\\u0430. \\\\u041a\\\\u043e\\\\u0440\\\\u0430\\\\u0431\\\\u0438\\\\u0442\\\\u0435 \\\\u0441\\\\u0435 \\\\u0433\\\\u0440\\\\u0443\\\\u043f\\\\u0438\\\\u0440\\\\u0430\\\\u0442 \\\\u0441\\\\u043f\\\\u043e\\\\u0440\\\\u0435\\\\u0434 \\\\u0434\\\\u044a\\\\u043b\\\\u0436\\\\u0438\\\\u043d\\\\u0430\\\\u0442\\\\u0430 \\\\u0438\\\\u043c \\\\u0432 \\\\u043c\\\\u0435\\\\u0442\\\\u0440\\\\u0438.  \\\\u041f\\\\u0440\\\\u0435\\\\u0434\\\\u043e\\\\u0441\\\\u0442\\\\u0430\\\\u0432\\\\u044f\\\\u0442 \\\\u0441\\\\u0435 \\\\u0441\\\\u0442\\\\u0430\\\\u0442\\\\u0438\\\\u0441\\\\u0442\\\\u0438\\\\u0447\\\\u0435\\\\u0441\\\\u043a\\\\u0438 \\\\u0434\\\\u0430\\\\u043d\\\\u043d\\\\u0438 \\\\u0437\\\\u0430 \\\\u0441\\\\u0435\\\\u0432\\\\u0435\\\\u0440\\\\u043e\\\\u0438\\\\u0437\\\\u0442\\\\u043e\\\\u0447\\\\u043d\\\\u0430\\\\u0442\\\\u0430 \\\\u0447\\\\u0430\\\\u0441\\\\u0442 \\\\u043d\\\\u0430 \\\\u0410\\\\u0442\\\\u043b\\\\u0430\\\\u043d\\\\u0442\\\\u0438\\\\u0447\\\\u0435\\\\u0441\\\\u043a\\\\u0438\\\\u044f \\\\u043e\\\\u043a\\\\u0435\\\\u0430\\\\u043d (\\\\u043c\\\\u0430\\\\u0439\\\\u043e\\\\u0440\\\\u043d\\\\u0430 \\\\u0437\\\\u043e\\\\u043d\\\\u0430 27 \\\\u043d\\\\u0430 \\\\u0424\\\\u0410\\\\u041e) \\\\u0434\\\\u043e \\\\u043d\\\\u0438\\\\u0432\\\\u043e\\\\u0442\\\\u043e \\\\u043d\\\\u0430 \\\\u0441\\\\u0442\\\\u0430\\\\u0442\\\\u0438\\\\u0441\\\\u0442\\\\u0438\\\\u0447\\\\u0435\\\\u0441\\\\u043a\\\\u0438\\\\u044f \\\\u043f\\\\u0440\\\\u0430\\\\u0432\\\\u043e\\\\u044a\\\\u0433\\\\u044a\\\\u043b\\\\u043d\\\\u0438\\\\u043a \\\\u043d\\\\u0430 ICES \\\\u043f\\\\u043e \\\\u0440\\\\u0438\\\\u0431\\\\u043e\\\\u043b\\\\u043e\\\\u0432\\\\u043d\\\\u0438 \\\\u0443\\\\u0440\\\\u0435\\\\u0434\\\\u0438 \\\\u0438 \\\\u0434\\\\u044a\\\\u043b\\\\u0436\\\\u0438\\\\u043d\\\\u0430 \\\\u043d\\\\u0430 \\\\u043a\\\\u043e\\\\u0440\\\\u0430\\\\u0431\\\\u0430 \\\\u043e\\\\u0442\\\\u043d\\\\u043e\\\\u0441\\\\u043d\\\\u043e:  \\\\u0440\\\\u0438\\\\u0431\\\\u043e\\\\u043b\\\\u043e\\\\u0432\\\\u043d\\\\u043e \\\\u0443\\\\u0441\\\\u0438\\\\u043b\\\\u0438\\\\u0435 (\\\\u043a\\\\u0438\\\\u043b\\\\u043e\\\\u0432\\\\u0430\\\\u0442/\\\\u0434\\\\u043d\\\\u0438) \\\\u0440\\\\u0430\\\\u0437\\\\u0442\\\\u043e\\\\u0432\\\\u0430\\\\u0440\\\\u0432\\\\u0430\\\\u043d\\\\u0438\\\\u044f \\\\u043f\\\\u043e \\\\u0436\\\\u0438\\\\u0432\\\\u043e \\\\u0442\\\\u0435\\\\u0433\\\\u043b\\\\u043e (\\\\u0432 \\\\u0442\\\\u043e\\\\u043d\\\\u043e\\\\u0432\\\\u0435), \\\\u0440\\\\u0430\\\\u0437\\\\u0442\\\\u043e\\\\u0432\\\\u0430\\\\u0440\\\\u0432\\\\u0430\\\\u043d\\\\u0438\\\\u044f \\\\u043f\\\\u043e \\\\u0441\\\\u0442\\\\u043e\\\\u0439\\\\u043d\\\\u043e\\\\u0441\\\\u0442 (\\\\u0441\\\\u0442\\\\u0435\\\\u0440\\\\u043b\\\\u0438\\\\u043d\\\\u0433\\\\u0438), \\\\u0431\\\\u0440\\\\u043e\\\\u0439 \\\\u043a\\\\u043e\\\\u0440\\\\u0430\\\\u0431\\\\u0438 (\\\\u0442\\\\u0430\\\\u0437\\\\u0438 \\\\u0438\\\\u043d\\\\u0444\\\\u043e\\\\u0440\\\\u043c\\\\u0430\\\\u0446\\\\u0438\\\\u044f \\\\u043c\\\\u043e\\\\u0436\\\\u0435 \\\\u0434\\\\u0430 \\\\u0431\\\\u044a\\\\u0434\\\\u0435 \\\\u043f\\\\u0440\\\\u043e\\\\u043f\\\\u0443\\\\u0441\\\\u043d\\\\u0430\\\\u0442\\\\u0430 \\\\u043e\\\\u0442 \\\\u043f\\\\u0443\\\\u0431\\\\u043b\\\\u0438\\\\u043a\\\\u0443\\\\u0432\\\\u0430\\\\u043d\\\\u0438\\\\u0442\\\\u0435 \\\\u0432\\\\u0435\\\\u0440\\\\u0441\\\\u0438\\\\u0438).  \\\\u0412\\\\u0438\\\\u0434\\\\u043e\\\\u0432\\\\u0435\\\\u0442\\\\u0435 \\\\u0443\\\\u0440\\\\u0435\\\\u0434\\\\u0438 \\\\u0441\\\\u0430 \\\\u043f\\\\u043e\\\\u043a\\\\u0430\\\\u0437\\\\u0430\\\\u043d\\\\u0438 \\\\u043d\\\\u0430 \\\\u0440\\\\u0430\\\\u0432\\\\u043d\\\\u0438\\\\u0449\\\\u0435\\\\u0442\\\\u043e \\\\u043d\\\\u0430 \\\\u041c\\\\u0435\\\\u0436\\\\u0434\\\\u0443\\\\u043d\\\\u0430\\\\u0440\\\\u043e\\\\u0434\\\\u043d\\\\u0430\\\\u0442\\\\u0430 \\\\u0441\\\\u0442\\\\u0430\\\\u043d\\\\u0434\\\\u0430\\\\u0440\\\\u0442\\\\u043d\\\\u0430 \\\\u0441\\\\u0442\\\\u0430\\\\u0442\\\\u0438\\\\u0441\\\\u0442\\\\u0438\\\\u0447\\\\u0435\\\\u0441\\\\u043a\\\\u0430 \\\\u043a\\\\u043b\\\\u0430\\\\u0441\\\\u0438\\\\u0444\\\\u0438\\\\u043a\\\\u0430\\\\u0446\\\\u0438\\\\u044f \\\\u043d\\\\u0430 \\\\u0440\\\\u0438\\\\u0431\\\\u043e\\\\u043b\\\\u043e\\\\u0432\\\\u043d\\\\u0438\\\\u0442\\\\u0435 \\\\u0441\\\\u044a\\\\u043e\\\\u0440\\\\u044a\\\\u0436\\\\u0435\\\\u043d\\\\u0438\\\\u044f (ISSCFG) \\\\u0438 \\\\u0441\\\\u0430 \\\\u043e\\\\u0431\\\\u043e\\\\u0431\\\\u0449\\\\u0435\\\\u043d\\\\u0438 \\\\u0432 \\\\u0440\\\\u0430\\\\u0437\\\\u043b\\\\u0438\\\\u0447\\\\u043d\\\\u0438 \\\\u0433\\\\u0440\\\\u0443\\\\u043f\\\\u0438, \\\\u0437\\\\u0430 \\\\u0434\\\\u0430 \\\\u0441\\\\u0435 \\\\u0434\\\\u0430\\\\u0434\\\\u0435 \\\\u0432\\\\u044a\\\\u0437\\\\u043c\\\\u043e\\\\u0436\\\\u043d\\\\u043e\\\\u0441\\\\u0442 \\\\u0437\\\\u0430 \\\\u0430\\\\u043d\\\\u0430\\\\u043b\\\\u0438\\\\u0437 \\\\u043d\\\\u0430 \\\\u0440\\\\u0430\\\\u0437\\\\u043b\\\\u0438\\\\u0447\\\\u043d\\\\u0438 \\\\u0440\\\\u0430\\\\u0432\\\\u043d\\\\u0438\\\\u0449\\\\u0430.  \\\\u0414\\\\u044a\\\\u043b\\\\u0436\\\\u0438\\\\u043d\\\\u0438\\\\u0442\\\\u0435 \\\\u043d\\\\u0430 \\\\u043a\\\\u043e\\\\u0440\\\\u0430\\\\u0431\\\\u0430 \\\\u0441\\\\u0435 \\\\u0441\\\\u0443\\\\u043c\\\\u0438\\\\u0440\\\\u0430\\\\u0442, \\\\u043a\\\\u0430\\\\u043a\\\\u0442\\\\u043e \\\\u0441\\\\u043b\\\\u0435\\\\u0434\\\\u0432\\\\u0430:  10\\\\u00a0m \\\\u0438 \\\\u043f\\\\u043e-\\\\u0434\\\\u043e\\\\u043b\\\\u0443,  \\\\u043d\\\\u0430\\\\u0434 10\\\\u201412 m,  \\\\u043d\\\\u0430\\\\u0434 12-\\\\u043f\\\\u043e\\\\u0434 15\\\\u00a0m,  15 \\\\u043c \\\\u0438 \\\\u043f\\\\u043e\\\\u0432\\\\u0435\\\\u0447\\\\u0435.\"}, {\"dataInfo\": {\"invisible\": false, \"provenanceaction\": {\"classid\": \"sysimport:crosswalk:repository\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"trust\": \"0.9\", \"inferred\": false, \"deletedbyinference\": false}, \"value\": \"Estad\\\\u00edsticas anuales de la actividad pesquera comercial de los buques pesqueros registrados en el Reino Unido para el a\\\\u00f1o indicado.Los buques se agrupan en metros en funci\\\\u00f3n de su eslora.  Se proporcionan estad\\\\u00edsticas para el Atl\\\\u00e1ntico Nororiental (zona principal 27 de la FAO) hasta el nivel del rect\\\\u00e1ngulo estad\\\\u00edstico del CIEM por arte de pesca y eslora del buque en:   esfuerzo pesquero (kilovatios/d\\\\u00edas) desembarques en peso vivo (toneladas), desembarques por valor (libras esterlinas), n\\\\u00famero de buques (esta informaci\\\\u00f3n puede omitirse en las versiones publicadas).  Los tipos de artes se muestran a nivel de la Clasificaci\\\\u00f3n Estad\\\\u00edstica Internacional Uniforme de Artes de Pesca (ISSCFG) y tambi\\\\u00e9n se agregan en diferentes grupos para permitir el an\\\\u00e1lisis a diferentes niveles.  Las esloras de los buques se agregan como sigue:  10m y menos, m\\\\u00e1s de 10-12m, m\\\\u00e1s de 12-bajo 15m, 15m y m\\\\u00e1s. Estad\\\\u00edsticas anuales de la actividad pesquera comercial de los buques pesqueros registrados en el Reino Unido para el a\\\\u00f1o indicado. Los buques se agrupan en metros en funci\\\\u00f3n de su eslora.    Se proporcionan estad\\\\u00edsticas para el Atl\\\\u00e1ntico Nororiental (zona principal 27 de la FAO) hasta el nivel del rect\\\\u00e1ngulo estad\\\\u00edstico del CIEM por arte de pesca y eslora del buque en:    esfuerzo pesquero (kilovatios/d\\\\u00edas)  desembarques en peso vivo (toneladas),  desembarques por valor (libras esterlinas),  n\\\\u00famero de buques (esta informaci\\\\u00f3n puede omitirse en las versiones publicadas).    Los tipos de artes se muestran a nivel de la Clasificaci\\\\u00f3n Estad\\\\u00edstica Internacional Uniforme de Artes de Pesca (ISSCFG) y tambi\\\\u00e9n se agregan en diferentes grupos para permitir el an\\\\u00e1lisis a diferentes niveles.    Las esloras de los buques se agregan como sigue:    10m y menos,  m\\\\u00e1s de 10-12m,  m\\\\u00e1s de 12-bajo 15m,  15m y m\\\\u00e1s.\"}, {\"dataInfo\": {\"invisible\": false, \"provenanceaction\": {\"classid\": \"sysimport:crosswalk:repository\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"trust\": \"0.9\", \"inferred\": false, \"deletedbyinference\": false}, \"value\": \"JK registruot\\\\u0173 \\\\u017evejybos laiv\\\\u0173 nurodyt\\\\u0173 met\\\\u0173 verslin\\\\u0117s \\\\u017evejybos veiklos metiniai statistiniai duomenys.Laivai grupuojami pagal j\\\\u0173 ilg\\\\u012f metrais.   Statistiniai duomenys apie \\\\u0161iaur\\\\u0117s ryt\\\\u0173 Atlant\\\\u0105 (FAO 27 pagrindinis rajonas) teikiami ICES statistinio sta\\\\u010diakampio lygmeniu pagal \\\\u017evejybos \\\\u012frankius ir laivo ilg\\\\u012f:  \\\\u017evejybos pastangos (kilovatais per dien\\\\u0105) i\\\\u0161krautas kiekis pagal gyv\\\\u0105j\\\\u012f svor\\\\u012f (tonomis), i\\\\u0161krautas kiekis pagal vert\\\\u0119 (svarais sterling\\\\u0173), laiv\\\\u0173 skai\\\\u010dius (\\\\u0161i informacija gali b\\\\u016bti nepateikiama paskelbtose versijose).  \\\\u017dvejybos \\\\u012franki\\\\u0173 tipai nurodyti Tarptautin\\\\u0117s standartin\\\\u0117s statistin\\\\u0117s \\\\u017evejybos \\\\u012franki\\\\u0173 klasifikacijos (ISSCFG) lygmeniu ir taip pat sugrupuoti \\\\u012f skirtingas grupes, kad b\\\\u016bt\\\\u0173 galima atlikti analiz\\\\u0119 \\\\u012fvairiais lygmenimis.  Laivo ilgis susumuojamas taip:  10 m ir ma\\\\u017eiau, daugiau kaip 10\\\\u201312 m, daugiau kaip 12 m, bet ma\\\\u017eiau kaip 15 m, 15m ir daugiau.JK registruot\\\\u0173 \\\\u017evejybos laiv\\\\u0173 nurodyt\\\\u0173 met\\\\u0173 verslin\\\\u0117s \\\\u017evejybos veiklos metiniai statistiniai duomenys. Laivai grupuojami pagal j\\\\u0173 ilg\\\\u012f metrais.  Statistiniai duomenys apie \\\\u0161iaur\\\\u0117s ryt\\\\u0173 Atlant\\\\u0105 (FAO 27 pagrindinis rajonas) teikiami ICES statistinio sta\\\\u010diakampio lygmeniu pagal \\\\u017evejybos \\\\u012frankius ir laivo ilg\\\\u012f:  \\\\u017evejybos pastangos (kilovatais per dien\\\\u0105) i\\\\u0161krautas kiekis pagal gyv\\\\u0105j\\\\u012f svor\\\\u012f (tonomis), i\\\\u0161krautas kiekis pagal vert\\\\u0119 (svarais sterling\\\\u0173), laiv\\\\u0173 skai\\\\u010dius (\\\\u0161i informacija gali b\\\\u016bti nepateikiama paskelbtose versijose).  \\\\u017dvejybos \\\\u012franki\\\\u0173 tipai nurodyti Tarptautin\\\\u0117s standartin\\\\u0117s statistin\\\\u0117s \\\\u017evejybos \\\\u012franki\\\\u0173 klasifikacijos (ISSCFG) lygmeniu ir taip pat sugrupuoti \\\\u012f skirtingas grupes, kad b\\\\u016bt\\\\u0173 galima atlikti analiz\\\\u0119 \\\\u012fvairiais lygmenimis.  Laivo ilgis susumuojamas taip:  10 m ir ma\\\\u017eiau,  daugiau kaip 10\\\\u201312 m,  daugiau kaip 12 m, bet ma\\\\u017eiau kaip 15 m,  15m ir daugiau.\"}, {\"dataInfo\": {\"invisible\": false, \"provenanceaction\": {\"classid\": \"sysimport:crosswalk:repository\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"trust\": \"0.9\", \"inferred\": false, \"deletedbyinference\": false}, \"value\": \"Godi\\\\u0161nja statistika aktivnosti gospodarskog ribolova ribarskih plovila registriranih u Ujedinjenoj Kraljevini za navedenu godinu.Plovila su grupirana prema duljini u metrima.  Statisti\\\\u010dki podaci dostavljaju se za sjeveroisto\\\\u010dni Atlantik (glavno podru\\\\u010dje FAO-a 27) do razine statisti\\\\u010dkog pravokutnika ICES-a prema ribolovnom alatu i duljini plovila na:   ribolovni napor (kilovat/dani) iskrcavanja \\\\u017eive vage (u tonama), iskrcaji prema vrijednosti (pounds sterling), broj plovila (te se informacije mogu izostaviti iz objavljenih verzija).  Vrste ribolovnih alata prikazane su na razini Me\\\\u0111unarodne standardne statisti\\\\u010dke klasifikacije ribolovnog alata (ISSCFG) i objedinjene u razli\\\\u010dite skupine kako bi se omogu\\\\u0107ila analiza na razli\\\\u010ditim razinama.  Duljine plovila agregiraju se kako slijedi:  10 m i manje, preko 10-12m, iznad 12-ispod 15m, 15m i vi\\\\u0161e. Godi\\\\u0161nja statistika aktivnosti gospodarskog ribolova ribarskih plovila registriranih u Ujedinjenoj Kraljevini za navedenu godinu. Plovila su grupirana prema duljini u metrima.    Statisti\\\\u010dki podaci dostavljaju se za sjeveroisto\\\\u010dni Atlantik (glavno podru\\\\u010dje FAO-a 27) do razine statisti\\\\u010dkog pravokutnika ICES-a prema ribolovnom alatu i duljini plovila na:    ribolovni napor (kilovat/dani)  iskrcavanja \\\\u017eive vage (u tonama),  iskrcaji prema vrijednosti (pounds sterling),  broj plovila (te se informacije mogu izostaviti iz objavljenih verzija).    Vrste ribolovnih alata prikazane su na razini Me\\\\u0111unarodne standardne statisti\\\\u010dke klasifikacije ribolovnog alata (ISSCFG) i objedinjene u razli\\\\u010dite skupine kako bi se omogu\\\\u0107ila analiza na razli\\\\u010ditim razinama.    Duljine plovila agregiraju se kako slijedi:    10 m i manje,  preko 10-12m,  iznad 12-ispod 15m,  15m i vi\\\\u0161e.\"}], \"format\": [{\"dataInfo\": {\"invisible\": false, \"provenanceaction\": {\"classid\": \"sysimport:crosswalk:repository\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"trust\": \"0.9\", \"inferred\": false, \"deletedbyinference\": false}, \"value\": \"http://publications.europa.eu/resource/authority/file-type/WMS_SRVC\"}], \"measures\": [{\"id\": \"influence\", \"unit\": [{\"dataInfo\": {\"provenanceaction\": {\"classid\": \"measure:bip\", \"classname\": \"measure:bip\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"deletedbyinference\": false, \"inferred\": true, \"inferenceprovenance\": \"update\", \"invisible\": false, \"trust\": \"\"}, \"key\": \"score\", \"value\": \"2.6613876E-9\"}, {\"dataInfo\": {\"provenanceaction\": {\"classid\": \"measure:bip\", \"classname\": \"measure:bip\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"deletedbyinference\": false, \"inferred\": true, \"inferenceprovenance\": \"update\", \"invisible\": false, \"trust\": \"\"}, \"key\": \"class\", \"value\": \"C5\"}]}, {\"id\": \"popularity\", \"unit\": [{\"dataInfo\": {\"provenanceaction\": {\"classid\": \"measure:bip\", \"classname\": \"measure:bip\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"deletedbyinference\": false, \"inferred\": true, \"inferenceprovenance\": \"update\", \"invisible\": false, \"trust\": \"\"}, \"key\": \"score\", \"value\": \"1.9561959E-9\"}, {\"dataInfo\": {\"provenanceaction\": {\"classid\": \"measure:bip\", \"classname\": \"measure:bip\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"deletedbyinference\": false, \"inferred\": true, \"inferenceprovenance\": \"update\", \"invisible\": false, \"trust\": \"\"}, \"key\": \"class\", \"value\": \"C5\"}]}, {\"id\": \"influence_alt\", \"unit\": [{\"dataInfo\": {\"provenanceaction\": {\"classid\": \"measure:bip\", \"classname\": \"measure:bip\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"deletedbyinference\": false, \"inferred\": true, \"inferenceprovenance\": \"update\", \"invisible\": false, \"trust\": \"\"}, \"key\": \"score\", \"value\": \"0\"}, {\"dataInfo\": {\"provenanceaction\": {\"classid\": \"measure:bip\", \"classname\": \"measure:bip\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"deletedbyinference\": false, \"inferred\": true, \"inferenceprovenance\": \"update\", \"invisible\": false, \"trust\": \"\"}, \"key\": \"class\", \"value\": \"C5\"}]}, {\"id\": \"popularity_alt\", \"unit\": [{\"dataInfo\": {\"provenanceaction\": {\"classid\": \"measure:bip\", \"classname\": \"measure:bip\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"deletedbyinference\": false, \"inferred\": true, \"inferenceprovenance\": \"update\", \"invisible\": false, \"trust\": \"\"}, \"key\": \"score\", \"value\": \"0.0\"}, {\"dataInfo\": {\"provenanceaction\": {\"classid\": \"measure:bip\", \"classname\": \"measure:bip\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"deletedbyinference\": false, \"inferred\": true, \"inferenceprovenance\": \"update\", \"invisible\": false, \"trust\": \"\"}, \"key\": \"class\", \"value\": \"C5\"}]}, {\"id\": \"impulse\", \"unit\": [{\"dataInfo\": {\"provenanceaction\": {\"classid\": \"measure:bip\", \"classname\": \"measure:bip\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"deletedbyinference\": false, \"inferred\": true, \"inferenceprovenance\": \"update\", \"invisible\": false, \"trust\": \"\"}, \"key\": \"score\", \"value\": \"0\"}, {\"dataInfo\": {\"provenanceaction\": {\"classid\": \"measure:bip\", \"classname\": \"measure:bip\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"deletedbyinference\": false, \"inferred\": true, \"inferenceprovenance\": \"update\", \"invisible\": false, \"trust\": \"\"}, \"key\": \"class\", \"value\": \"C5\"}]}], \"coverage\": [], \"externalReference\": [], \"publisher\": {\"dataInfo\": {\"invisible\": false, \"provenanceaction\": {\"classid\": \"sysimport:crosswalk:repository\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"trust\": \"0.9\", \"inferred\": false, \"deletedbyinference\": false}, \"value\": \"Marine Management Organisation\"}, \"context\": [{\"dataInfo\": [{\"provenanceaction\": {\"classid\": \"community:advconstraint\", \"classname\": \"community:advconstraint\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"deletedbyinference\": false, \"inferred\": true, \"inferenceprovenance\": \"bulktagging\", \"invisible\": false, \"trust\": \"0.8\"}], \"id\": \"dh-ch::subcommunity::3\"}], \"eoscifguidelines\": [], \"language\": {\"classid\": \"und\", \"classname\": \"Undetermined\", \"schemeid\": \"dnet:languages\", \"schemename\": \"dnet:languages\"}, \"resulttype\": {\"classid\": \"dataset\", \"classname\": \"dataset\", \"schemeid\": \"dnet:result_typologies\", \"schemename\": \"dnet:result_typologies\"}, \"country\": [{\"classid\": \"EU\", \"classname\": \"European Union\", \"dataInfo\": {\"invisible\": false, \"provenanceaction\": {\"classid\": \"sysimport:crosswalk:repository\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"trust\": \"0.9\", \"inferred\": false, \"deletedbyinference\": false}, \"schemeid\": \"dnet:countries\", \"schemename\": \"dnet:countries\"}], \"extraInfo\": [], \"originalId\": [\"\", \"50|r3c4b2081b22::21f4b7af9e5ee8e19006ee644e89dd0e\"], \"source\": [], \"dateofacceptance\": {\"dataInfo\": {\"invisible\": false, \"provenanceaction\": {\"classid\": \"sysimport:crosswalk:repository\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"trust\": \"0.9\", \"inferred\": false, \"deletedbyinference\": false}, \"value\": \"2021-10-12\"}, \"title\": [{\"dataInfo\": {\"invisible\": false, \"provenanceaction\": {\"classid\": \"sysimport:crosswalk:repository\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"trust\": \"0.9\", \"inferred\": false, \"deletedbyinference\": false}, \"qualifier\": {\"classid\": \"main title\", \"classname\": \"main title\", \"schemeid\": \"dnet:dataCite_title\", \"schemename\": \"dnet:dataCite_title\"}, \"value\": \"Vereinigtes K\\\\u00f6nigreich Fischanlandungen nach ICES-Rechteck (nach Schiffsl\\\\u00e4nge) 2011 Web Mapping Service (WMS)\"}, {\"dataInfo\": {\"invisible\": false, \"provenanceaction\": {\"classid\": \"sysimport:crosswalk:repository\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"trust\": \"0.9\", \"inferred\": false, \"deletedbyinference\": false}, \"qualifier\": {\"classid\": \"main title\", \"classname\": \"main title\", \"schemeid\": \"dnet:dataCite_title\", \"schemename\": \"dnet:dataCite_title\"}, \"value\": \"Yhdistyneen kuningaskunnan puretut kalasaaliit ICES-ruuduittain (aluksen pituuden mukaan) 2011 Web Mapping Service (WMS)\"}, {\"dataInfo\": {\"invisible\": false, \"provenanceaction\": {\"classid\": \"sysimport:crosswalk:repository\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"trust\": \"0.9\", \"inferred\": false, \"deletedbyinference\": false}, \"qualifier\": {\"classid\": \"main title\", \"classname\": \"main title\", \"schemeid\": \"dnet:dataCite_title\", \"schemename\": \"dnet:dataCite_title\"}, \"value\": \"Desembarques de peixes no Reino Unido por rect\\\\u00e2ngulo CIEM (por comprimento de navio) 2011 Web Mapping Service (WMS)\"}, {\"dataInfo\": {\"invisible\": false, \"provenanceaction\": {\"classid\": \"sysimport:crosswalk:repository\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"trust\": \"0.9\", \"inferred\": false, \"deletedbyinference\": false}, \"qualifier\": {\"classid\": \"main title\", \"classname\": \"main title\", \"schemeid\": \"dnet:dataCite_title\", \"schemename\": \"dnet:dataCite_title\"}, \"value\": \"\\\\u041e\\\\u0431\\\\u0435\\\\u0434\\\\u0438\\\\u043d\\\\u0435\\\\u043d\\\\u043e \\\\u043a\\\\u0440\\\\u0430\\\\u043b\\\\u0441\\\\u0442\\\\u0432\\\\u043e \\\\u0420\\\\u0438\\\\u0431\\\\u043e\\\\u043b\\\\u043e\\\\u0432\\\\u043d\\\\u0438 \\\\u0440\\\\u0430\\\\u0437\\\\u0442\\\\u043e\\\\u0432\\\\u0430\\\\u0440\\\\u0432\\\\u0430\\\\u043d\\\\u0438\\\\u044f \\\\u043f\\\\u043e \\\\u043f\\\\u0440\\\\u0430\\\\u0432\\\\u043e\\\\u044a\\\\u0433\\\\u044a\\\\u043b\\\\u043d\\\\u0438\\\\u043a \\\\u043d\\\\u0430 ICES (\\\\u043f\\\\u043e \\\\u0434\\\\u044a\\\\u043b\\\\u0436\\\\u0438\\\\u043d\\\\u0430 \\\\u043d\\\\u0430 \\\\u043a\\\\u043e\\\\u0440\\\\u0430\\\\u0431\\\\u0430) 2011 Web Mapping Service (WMS)\"}, {\"dataInfo\": {\"invisible\": false, \"provenanceaction\": {\"classid\": \"sysimport:crosswalk:repository\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"trust\": \"0.9\", \"inferred\": false, \"deletedbyinference\": false}, \"qualifier\": {\"classid\": \"main title\", \"classname\": \"main title\", \"schemeid\": \"dnet:dataCite_title\", \"schemename\": \"dnet:dataCite_title\"}, \"value\": \"Jungtin\\\\u0117s Karalyst\\\\u0117s i\\\\u0161kraut\\\\u0173 \\\\u017euv\\\\u0173 kiekis pagal ICES sta\\\\u010diakamp\\\\u012f (pagal laivo ilg\\\\u012f) 2011 m. saityno kartografavimo paslauga (WMS)\"}, {\"dataInfo\": {\"invisible\": false, \"provenanceaction\": {\"classid\": \"sysimport:crosswalk:repository\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"trust\": \"0.9\", \"inferred\": false, \"deletedbyinference\": false}, \"qualifier\": {\"classid\": \"main title\", \"classname\": \"main title\", \"schemeid\": \"dnet:dataCite_title\", \"schemename\": \"dnet:dataCite_title\"}, \"value\": \"Apvienot\\\\u0101s Karalistes zivju izkr\\\\u0101vumi pa ICES taisnst\\\\u016briem (p\\\\u0113c ku\\\\u0123a garuma), 2011. gads, t\\\\u012bmek\\\\u013ca kart\\\\u0113\\\\u0161anas pakalpojums (WMS)\"}, {\"dataInfo\": {\"invisible\": false, \"provenanceaction\": {\"classid\": \"sysimport:crosswalk:repository\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"trust\": \"0.9\", \"inferred\": false, \"deletedbyinference\": false}, \"qualifier\": {\"classid\": \"main title\", \"classname\": \"main title\", \"schemeid\": \"dnet:dataCite_title\", \"schemename\": \"dnet:dataCite_title\"}, \"value\": \"Iskrcavanje ribe u Ujedinjenoj Kraljevini prema ICES-ovu pravokutniku (prema duljini plovila) 2011. Web Mapping Service (WMS)\"}, {\"dataInfo\": {\"invisible\": false, \"provenanceaction\": {\"classid\": \"sysimport:crosswalk:repository\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"trust\": \"0.9\", \"inferred\": false, \"deletedbyinference\": false}, \"qualifier\": {\"classid\": \"main title\", \"classname\": \"main title\", \"schemeid\": \"dnet:dataCite_title\", \"schemename\": \"dnet:dataCite_title\"}, \"value\": \"D\\\\u00e9barquements de poissons du Royaume-Uni par rectangle CIEM (par longueur de navire) 2011 Web Mapping Service (WMS)\"}, {\"dataInfo\": {\"invisible\": false, \"provenanceaction\": {\"classid\": \"sysimport:crosswalk:repository\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"trust\": \"0.9\", \"inferred\": false, \"deletedbyinference\": false}, \"qualifier\": {\"classid\": \"main title\", \"classname\": \"main title\", \"schemeid\": \"dnet:dataCite_title\", \"schemename\": \"dnet:dataCite_title\"}, \"value\": \"Az Egyes\\\\u00fclt Kir\\\\u00e1lys\\\\u00e1g \\\\u00e1ltal kirakodott halak ICES-n\\\\u00e9gysz\\\\u00f6g szerinti bont\\\\u00e1sban (haj\\\\u00f3hossz szerint) 2011 Web Mapping Service (WMS)\"}, {\"dataInfo\": {\"invisible\": false, \"provenanceaction\": {\"classid\": \"sysimport:crosswalk:repository\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"trust\": \"0.9\", \"inferred\": false, \"deletedbyinference\": false}, \"qualifier\": {\"classid\": \"main title\", \"classname\": \"main title\", \"schemeid\": \"dnet:dataCite_title\", \"schemename\": \"dnet:dataCite_title\"}, \"value\": \"\\\\u0412\\\\u0435\\\\u043b\\\\u0438\\\\u043a\\\\u0430 \\\\u0411\\\\u0440\\\\u0438\\\\u0442\\\\u0430\\\\u043d\\\\u0456\\\\u044f Fish Landings by ICES \\\\u043f\\\\u0440\\\\u044f\\\\u043c\\\\u043e\\\\u043a\\\\u0443\\\\u0442\\\\u043d\\\\u0438\\\\u043a (\\\\u0437\\\\u0430 \\\\u0434\\\\u043e\\\\u0432\\\\u0436\\\\u0438\\\\u043d\\\\u043e\\\\u044e \\\\u0441\\\\u0443\\\\u0434\\\\u043d\\\\u0430) 2011 Web Mapping Service (WMS)\"}, {\"dataInfo\": {\"invisible\": false, \"provenanceaction\": {\"classid\": \"sysimport:crosswalk:repository\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"trust\": \"0.9\", \"inferred\": false, \"deletedbyinference\": false}, \"qualifier\": {\"classid\": \"main title\", \"classname\": \"main title\", \"schemeid\": \"dnet:dataCite_title\", \"schemename\": \"dnet:dataCite_title\"}, \"value\": \"Vylodenia r\\\\u00fdb v Spojenom kr\\\\u00e1\\\\u013eovstve pod\\\\u013ea obd\\\\u013a\\\\u017enika ICES (pod\\\\u013ea d\\\\u013a\\\\u017eky plavidla) Webov\\\\u00e1 mapovacia slu\\\\u017eba (WMS) 2011\"}, {\"dataInfo\": {\"invisible\": false, \"provenanceaction\": {\"classid\": \"sysimport:crosswalk:repository\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"trust\": \"0.9\", \"inferred\": false, \"deletedbyinference\": false}, \"qualifier\": {\"classid\": \"main title\", \"classname\": \"main title\", \"schemeid\": \"dnet:dataCite_title\", \"schemename\": \"dnet:dataCite_title\"}, \"value\": \"Iztovarjanje rib iz Zdru\\\\u017eenega kraljestva glede na pravokotnik ICES (po dol\\\\u017eini plovila) \\\\u2013 storitev spletnega kartiranja za leto 2011 (WMS)\"}, {\"dataInfo\": {\"invisible\": false, \"provenanceaction\": {\"classid\": \"sysimport:crosswalk:repository\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"trust\": \"0.9\", \"inferred\": false, \"deletedbyinference\": false}, \"qualifier\": {\"classid\": \"main title\", \"classname\": \"main title\", \"schemeid\": \"dnet:dataCite_title\", \"schemename\": \"dnet:dataCite_title\"}, \"value\": \"Cur i dt\\\\u00edr \\\\u00e9isc na R\\\\u00edochta Aontaithe de r\\\\u00e9ir dhronuilleog ICES (de r\\\\u00e9ir fhad an tsoithigh) 2011 Seirbh\\\\u00eds L\\\\u00e9arsc\\\\u00e1ilithe Gr\\\\u00e9as\\\\u00e1in (WMS)\"}, {\"dataInfo\": {\"invisible\": false, \"provenanceaction\": {\"classid\": \"sysimport:crosswalk:repository\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"trust\": \"0.9\", \"inferred\": false, \"deletedbyinference\": false}, \"qualifier\": {\"classid\": \"main title\", \"classname\": \"main title\", \"schemeid\": \"dnet:dataCite_title\", \"schemename\": \"dnet:dataCite_title\"}, \"value\": \"F\\\\u00f6renade kungarikets fisklandningar per Ices-rektangel (per fartygsl\\\\u00e4ngd) 2011 Web Mapping Service (WMS)\"}, {\"dataInfo\": {\"invisible\": false, \"provenanceaction\": {\"classid\": \"sysimport:crosswalk:repository\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"trust\": \"0.9\", \"inferred\": false, \"deletedbyinference\": false}, \"qualifier\": {\"classid\": \"main title\", \"classname\": \"main title\", \"schemeid\": \"dnet:dataCite_title\", \"schemename\": \"dnet:dataCite_title\"}, \"value\": \"\\\\u0126att l-art tal-\\\\u0126ut tar-Renju Unit skont ir-rettangolu tal-ICES (skont it-tul tal-bastiment) 2011 Web Mapping Service (WMS)\"}, {\"dataInfo\": {\"invisible\": false, \"provenanceaction\": {\"classid\": \"sysimport:crosswalk:repository\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"trust\": \"0.9\", \"inferred\": false, \"deletedbyinference\": false}, \"qualifier\": {\"classid\": \"main title\", \"classname\": \"main title\", \"schemeid\": \"dnet:dataCite_title\", \"schemename\": \"dnet:dataCite_title\"}, \"value\": \"\\\\u0395\\\\u03ba\\\\u03c6\\\\u03bf\\\\u03c1\\\\u03c4\\\\u03ce\\\\u03c3\\\\u03b5\\\\u03b9\\\\u03c2 \\\\u03b9\\\\u03c7\\\\u03b8\\\\u03cd\\\\u03c9\\\\u03bd \\\\u03c4\\\\u03bf\\\\u03c5 \\\\u0397\\\\u03bd\\\\u03c9\\\\u03bc\\\\u03ad\\\\u03bd\\\\u03bf\\\\u03c5 \\\\u0392\\\\u03b1\\\\u03c3\\\\u03b9\\\\u03bb\\\\u03b5\\\\u03af\\\\u03bf\\\\u03c5 \\\\u03b1\\\\u03bd\\\\u03ac \\\\u03bf\\\\u03c1\\\\u03b8\\\\u03bf\\\\u03b3\\\\u03ce\\\\u03bd\\\\u03b9\\\\u03bf ICES (\\\\u03b1\\\\u03bd\\\\u03ac \\\\u03bc\\\\u03ae\\\\u03ba\\\\u03bf\\\\u03c2 \\\\u03c3\\\\u03ba\\\\u03ac\\\\u03c6\\\\u03bf\\\\u03c5\\\\u03c2) 2011 Web Mapping Service (WMS)\"}, {\"dataInfo\": {\"invisible\": false, \"provenanceaction\": {\"classid\": \"sysimport:crosswalk:repository\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"trust\": \"0.9\", \"inferred\": false, \"deletedbyinference\": false}, \"qualifier\": {\"classid\": \"main title\", \"classname\": \"main title\", \"schemeid\": \"dnet:dataCite_title\", \"schemename\": \"dnet:dataCite_title\"}, \"value\": \"United Kingdom Fish Landings by ICES rectangle (by vessel length) 2011 Web Mapping Service (WMS)\"}, {\"dataInfo\": {\"invisible\": false, \"provenanceaction\": {\"classid\": \"sysimport:crosswalk:repository\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"trust\": \"0.9\", \"inferred\": false, \"deletedbyinference\": false}, \"qualifier\": {\"classid\": \"main title\", \"classname\": \"main title\", \"schemeid\": \"dnet:dataCite_title\", \"schemename\": \"dnet:dataCite_title\"}, \"value\": \"Regno Unito Sbarchi di pesci per rettangolo CIEM (per lunghezza della nave) 2011 Web Mapping Service (WMS)\"}, {\"dataInfo\": {\"invisible\": false, \"provenanceaction\": {\"classid\": \"sysimport:crosswalk:repository\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"trust\": \"0.9\", \"inferred\": false, \"deletedbyinference\": false}, \"qualifier\": {\"classid\": \"main title\", \"classname\": \"main title\", \"schemeid\": \"dnet:dataCite_title\", \"schemename\": \"dnet:dataCite_title\"}, \"value\": \"Desembarques de pescado en el Reino Unido por rect\\\\u00e1ngulo del CIEM (por eslora del buque) 2011 Web Mapping Service (WMS)\"}, {\"dataInfo\": {\"invisible\": false, \"provenanceaction\": {\"classid\": \"sysimport:crosswalk:repository\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"trust\": \"0.9\", \"inferred\": false, \"deletedbyinference\": false}, \"qualifier\": {\"classid\": \"main title\", \"classname\": \"main title\", \"schemeid\": \"dnet:dataCite_title\", \"schemename\": \"dnet:dataCite_title\"}, \"value\": \"\\\\u00dchendkuningriigi lossitud kalakogused ICESi ruutude kaupa (laeva pikkuse j\\\\u00e4rgi) 2011 Web Mapping Service (WMS)\"}, {\"dataInfo\": {\"invisible\": false, \"provenanceaction\": {\"classid\": \"sysimport:crosswalk:repository\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"trust\": \"0.9\", \"inferred\": false, \"deletedbyinference\": false}, \"qualifier\": {\"classid\": \"main title\", \"classname\": \"main title\", \"schemeid\": \"dnet:dataCite_title\", \"schemename\": \"dnet:dataCite_title\"}, \"value\": \"Vykl\\\\u00e1dky ryb ve Spojen\\\\u00e9m kr\\\\u00e1lovstv\\\\u00ed podle obd\\\\u00e9ln\\\\u00edku ICES (podle d\\\\u00e9lky plavidla) 2011 Web Mapping Service (WMS)\"}, {\"dataInfo\": {\"invisible\": false, \"provenanceaction\": {\"classid\": \"sysimport:crosswalk:repository\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"trust\": \"0.9\", \"inferred\": false, \"deletedbyinference\": false}, \"qualifier\": {\"classid\": \"main title\", \"classname\": \"main title\", \"schemeid\": \"dnet:dataCite_title\", \"schemename\": \"dnet:dataCite_title\"}, \"value\": \"Wy\\\\u0142adunki ryb w Zjednoczonym Kr\\\\u00f3lestwie wed\\\\u0142ug prostok\\\\u0105ta ICES (wed\\\\u0142ug d\\\\u0142ugo\\\\u015bci statku) 2011 Web Mapping Service (WMS)\"}, {\"dataInfo\": {\"invisible\": false, \"provenanceaction\": {\"classid\": \"sysimport:crosswalk:repository\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"trust\": \"0.9\", \"inferred\": false, \"deletedbyinference\": false}, \"qualifier\": {\"classid\": \"main title\", \"classname\": \"main title\", \"schemeid\": \"dnet:dataCite_title\", \"schemename\": \"dnet:dataCite_title\"}, \"value\": \"Debarc\\\\u0103rile de pe\\\\u0219te din Regatul Unit \\\\u00een func\\\\u021bie de dreptunghiul ICES (pe lungime de nav\\\\u0103) 2011 Web Mapping Service (WMS)\"}, {\"dataInfo\": {\"invisible\": false, \"provenanceaction\": {\"classid\": \"sysimport:crosswalk:repository\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"trust\": \"0.9\", \"inferred\": false, \"deletedbyinference\": false}, \"qualifier\": {\"classid\": \"main title\", \"classname\": \"main title\", \"schemeid\": \"dnet:dataCite_title\", \"schemename\": \"dnet:dataCite_title\"}, \"value\": \"Det Forenede Kongeriges fiskelandinger efter ICES-rektangel (efter fart\\\\u00f8jsl\\\\u00e6ngde) 2011 Web Mapping Service (WMS)\"}, {\"dataInfo\": {\"invisible\": false, \"provenanceaction\": {\"classid\": \"sysimport:crosswalk:repository\", \"classname\": \"Harvested\", \"schemeid\": \"dnet:provenanceActions\", \"schemename\": \"dnet:provenanceActions\"}, \"trust\": \"0.9\", \"inferred\": false, \"deletedbyinference\": false}, \"qualifier\": {\"classid\": \"main title\", \"classname\": \"main title\", \"schemeid\": \"dnet:dataCite_title\", \"schemename\": \"dnet:dataCite_title\"}, \"value\": \"Aanvoer van vis uit het Verenigd Koninkrijk per ICES-rechthoek (per vaartuiglengte) 2011 Web Mapping Service (WMS)\"}]}";
		ProtoMap protoMap = new Gson().fromJson(pathMap, ProtoMap.class);
		Dataset res = resultTagger.enrichContextCriteria(new Gson().fromJson(value, Dataset.class),
				Utils.getCommunityConfiguration(baseURL), protoMap);
		System.out.println(new ObjectMapper().writeValueAsString(res));

	}

	@Test
	public void testApi() throws IOException {
		String baseURL = "https://services.openaire.eu/openaire/community/";
		List<SubCommunityModel> subcommunities = Utils.getSubcommunities("clarin", baseURL);

		CommunityConfiguration tmp = Utils.getCommunityConfiguration(baseURL);
//		tmp.getCommunities().keySet().forEach(c -> {
//			try {
//				System.out.println(new ObjectMapper().writeValueAsString(tmp.getCommunities().get(c)));
//			} catch (JsonProcessingException e) {
//				throw new RuntimeException(e);
//			}
//		});

		System.out.println(new ObjectMapper().writeValueAsString(Utils.getOrganizationCommunityMap(baseURL)));
		System.out.println(new ObjectMapper().writeValueAsString(Utils.getDatasourceCommunities(baseURL)));
	}

	@Test
	public void getConfigurationApi() throws IOException {
		String baseURL = "https://services.openaire.eu/openaire/community/";
		List<SubCommunityModel> subcommunities = Utils.getSubcommunities("clarin", baseURL);

		CommunityConfiguration cc = Utils.getCommunityConfiguration(baseURL);

		cc.getCommunities().keySet().forEach(c -> {
			try {
				System.out.println(new ObjectMapper().writeValueAsString(cc.getCommunities().get(c)));
			} catch (JsonProcessingException e) {
				throw new RuntimeException(e);
			}
		});

//		System.out.println(new ObjectMapper().writeValueAsString(Utils.getOrganizationCommunityMap(baseURL)));
//		System.out.println(new ObjectMapper().writeValueAsString(Utils.getDatasourceCommunities(baseURL)));
	}

}
