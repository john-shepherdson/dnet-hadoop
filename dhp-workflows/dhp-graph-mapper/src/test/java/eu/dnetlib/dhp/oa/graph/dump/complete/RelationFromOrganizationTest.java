
package eu.dnetlib.dhp.oa.graph.dump.complete;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.schema.dump.oaf.graph.Relation;

public class RelationFromOrganizationTest {
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SparkSession spark;

	private static Path workingDir;

	private static final Logger log = LoggerFactory
		.getLogger(RelationFromOrganizationTest.class);

	private static final HashMap<String, String> map = new HashMap<>();

	String organizationCommunityMap = "{\"20|grid________::afaa39865943381c51f76c08725ffa75\":[\"mes\",\"euromarine\"], \"20|corda__h2020::e8dbe14cca9bf6fce09d468872f813f8\":[\"mes\",\"euromarine\"], \"20|snsf________::9b253f265e3bef5cae6d881fdf61aceb\":[\"mes\",\"euromarine\"],\"20|rcuk________::e054eea0a47665af8c3656b5785ccf76\":[\"mes\",\"euromarine\"],\"20|corda__h2020::edc18d67c9b11fb616ca9f6e1db1b151\":[\"mes\",\"euromarine\"],\"20|rcuk________::d5736d9da90521ddcdc7828a05a85e9a\":[\"mes\",\"euromarine\"],\"20|corda__h2020::f5d418d3aa1cf817ddefcc3fdc039f27\":[\"mes\",\"euromarine\"],\"20|snsf________::8fa091f8f25a846779acb4ea97b50aef\":[\"mes\",\"euromarine\"],\"20|corda__h2020::81e020977211c2c40fae2e1a50bffd71\":[\"mes\",\"euromarine\"],\"20|corda_______::81e020977211c2c40fae2e1a50bffd71\":[\"mes\",\"euromarine\"],\"20|snsf________::31d0a100e54e3cdb3c6f52d91e638c78\":[\"mes\",\"euromarine\"],\"20|corda__h2020::ea379ef91b8cc86f9ac5edc4169292db\":[\"mes\",\"euromarine\"],\"20|corda__h2020::f75ee2ee48e5cb0ec8c8d30aaa8fef70\":[\"mes\",\"euromarine\"],\"20|rcuk________::e16010089551a1a9182a94604fc0ea59\":[\"mes\",\"euromarine\"],\"20|corda__h2020::38531a2cce7c5c347ffc439b07c1f43b\":[\"mes\",\"euromarine\"],\"20|corda_______::38531a2cce7c5c347ffc439b07c1f43b\":[\"mes\",\"euromarine\"],\"20|grid________::b2cbbf5eadbbf87d534b022bad3191d7\":[\"mes\",\"euromarine\"],\"20|snsf________::74730ef1439d7f7636a8be58a6b471b8\":[\"mes\",\"euromarine\"],\"20|nsf_________::ad72e19043a5a467e35f9b444d11563e\":[\"mes\",\"euromarine\"],\"20|rcuk________::0fc3e92500290902a2d38ec2445e74c3\":[\"mes\",\"euromarine\"],\"20|grid________::ad2c29905da0eb3c06b3fa80cacd89ea\":[\"mes\",\"euromarine\"],\"20|corda__h2020::30b53e4d63d3724f00acb9cbaca40860\":[\"mes\",\"euromarine\"],\"20|corda__h2020::f60f84bee14ad93f0db0e49af1d5c317\":[\"mes\",\"euromarine\"],  \"20|corda__h2020::7bf251ac3765b5e89d82270a1763d09f\":[\"mes\",\"euromarine\"],  \"20|corda__h2020::65531bd11be9935948c7f2f4db1c1832\":[\"mes\",\"euromarine\"],  \"20|corda__h2020::e0e98f86bbc76638bbb72a8fe2302946\":[\"mes\",\"euromarine\"],  \"20|snsf________::3eb43582ac27601459a8d8b3e195724b\":[\"mes\",\"euromarine\"],  \"20|corda__h2020::af2481dab65d06c8ea0ae02b5517b9b6\":[\"mes\",\"euromarine\"],  \"20|corda__h2020::c19d05cfde69a50d3ebc89bd0ee49929\":[\"mes\",\"euromarine\"],  \"20|corda__h2020::af0bfd9fc09f80d9488f56d71a9832f0\":[\"mes\",\"euromarine\"],  \"20|rcuk________::f33c02afb0dc66c49d0ed97ca5dd5cb0\":[\"beopen\"], "
		+
		"\"20|grid________::a867f78acdc5041b34acfe4f9a349157\":[\"beopen\"],   \"20|grid________::7bb116a1a9f95ab812bf9d2dea2be1ff\":[\"beopen\"],  \"20|corda__h2020::6ab0e0739dbe625b99a2ae45842164ad\":[\"beopen\"],  \"20|corda__h2020::8ba50792bc5f4d51d79fca47d860c602\":[\"beopen\"],  \"20|corda_______::8ba50792bc5f4d51d79fca47d860c602\":[\"beopen\"],  \"20|corda__h2020::e70e9114979e963eef24666657b807c3\":[\"beopen\"],  \"20|corda_______::e70e9114979e963eef24666657b807c3\":[\"beopen\"],  \"20|corda_______::15911e01e9744d57205825d77c218737\":[\"beopen\"],  \"20|opendoar____::056a41e24e2a9a67215e87bbee6a80ab\":[\"beopen\"],  \"20|opendoar____::7f67f2e6c6fbb0628f8160fcd3d92ae3\":[\"beopen\"],  \"20|grid________::a8ecfd7c084e561168bcbe6bf0daf3e3\":[\"beopen\"],  \"20|corda_______::7bbe6cc5d8ec1864739a04b0d020c9e9\":[\"beopen\"],  \"20|corda_______::3ff558e30c2e434d688539548300b050\":[\"beopen\"],  \"20|corda__h2020::5ffee5b3b83b33a8cf0e046877bd3a39\":[\"beopen\"],  \"20|corda__h2020::5187217e2e806a6df3579c46f82401bc\":[\"beopen\"],  \"20|grid________::5fa7e2709bcd945e26bfa18689adeec1\":[\"beopen\"],  \"20|corda_______::d8696683c53027438031a96ad27c3c07\":[\"beopen\"],  \"20|corda__h2020::d8696683c53027438031a96ad27c3c07\":[\"beopen\"],  \"20|rcuk________::23a79ebdfa59790864e4a485881568c1\":[\"beopen\"],  \"20|corda__h2020::b76cf8fe49590a966953c37e18608af9\":[\"beopen\"],  \"20|grid________::d2f0204126ee709244a488a4cd3b91c2\":[\"beopen\"],  \"20|corda__h2020::05aba9d2ed17533d15221e5655ac11e6\":[\"beopen\"],  \"20|grid________::802401579481dc32062bdee69f5e6a34\":[\"beopen\"],  \"20|corda__h2020::3f6d9d54cac975a517ba6b252c81582d\":[\"beopen\"]}";

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files
			.createTempDirectory(RelationFromOrganizationTest.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(RelationFromOrganizationTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(RelationFromOrganizationTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	@Test
	public void test1() throws Exception {

		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/relation")
			.getPath();

		final String communityMapPath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/communityMapPath/communitymapservices.json")
			.getPath();

		SparkOrganizationRelation.main(new String[] {
			"-isSparkSessionManaged", Boolean.FALSE.toString(),
			"-outputPath", workingDir.toString() + "/relation",
			"-sourcePath", sourcePath,
			"-organizationCommunityMap", organizationCommunityMap,
			"-communityMapPath", communityMapPath
		});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Relation> tmp = sc
			.textFile(workingDir.toString() + "/relation")
			.map(item -> OBJECT_MAPPER.readValue(item, Relation.class));

		org.apache.spark.sql.Dataset<Relation> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(Relation.class));

		verificationDataset.createOrReplaceTempView("table");

		// Assertions.assertEquals(170, verificationDataset.count());
		Assertions.assertEquals(0, verificationDataset.count());

//		Dataset<Row> checkDs = spark
//			.sql(
//				"Select source.id, source.type " +
//					"from table ");
//
//		Assertions.assertEquals(2, checkDs.filter("substr(id, 4, 5) = 'dedup' ").count());
//
//		Assertions.assertEquals(0, checkDs.filter("id = '20|grid________::afaa39865943381c51f76c08725ffa75'").count());
//
//		Assertions.assertEquals(25, checkDs.filter("id = '00|context_____::" + DHPUtils.md5("beopen") + "'").count());
//
//		Assertions
//			.assertEquals(30, checkDs.filter("id = '00|context_____::" + DHPUtils.md5("euromarine") + "'").count());
//
//		Assertions.assertEquals(30, checkDs.filter("id = '00|context_____::" + DHPUtils.md5("mes") + "'").count());
	}

}
