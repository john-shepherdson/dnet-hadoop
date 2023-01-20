
package eu.dnetlib.dhp.bulktag;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.schema.oaf.Dataset;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

//"50|475c1990cbb2::0fecfb874d9395aa69d2f4d7cd1acbea" has instance hostedby eosc
//"50|475c1990cbb2::3185cd5d8a2b0a06bb9b23ef11748eb1" has instance hostedby eosc
//"50|475c1990cbb2::449f28eefccf9f70c04ad70d61e041c7" has two instance one hostedby eosc
//"50|475c1990cbb2::3894c94123e96df8a21249957cf160cb" has EoscTag

public class EOSCContextTaggingTest {

	private static final Logger log = LoggerFactory.getLogger(EOSCContextTaggingTest.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SparkSession spark;

	private static Path workingDir;

	public static final String MOCK_IS_LOOK_UP_URL = "BASEURL:8280/is/services/isLookUp";

	public static final String pathMap = "{ \"author\" : \"$['author'][*]['fullname']\","
			+ "  \"title\" : \"$['title'][*]['value']\","
			+ "  \"orcid\" : \"$['author'][*]['pid'][*][?(@['key']=='ORCID')]['value']\","
			+ "  \"contributor\" : \"$['contributor'][*]['value']\","
			+ "  \"description\" : \"$['description'][*]['value']\", "
			+ " \"subject\" :\"$['subject'][*]['value']\" , " +

			"\"fos\" : \"$['subject'][?(@['qualifier']['classid']=='subject:fos')].value\"} ";

	private static String taggingConf = "";

	static {
		try {
			taggingConf = IOUtils
					.toString(
							BulkTagJobTest.class
									.getResourceAsStream(
											"/eu/dnetlib/dhp/bulktag/communityconfiguration/tagging_conf.xml"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files.createTempDirectory(EOSCContextTaggingTest.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(EOSCContextTaggingTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(EOSCTagJobTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	@Test
	void EoscContextTagTest() throws Exception {

		spark
			.read()
			.textFile(getClass().getResource("/eu/dnetlib/dhp/bulktag/eosc/dataset/dataset_10.json").getPath())
			.map(
				(MapFunction<String, Dataset>) value -> OBJECT_MAPPER.readValue(value, Dataset.class),
				Encoders.bean(Dataset.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(workingDir.toString() + "/input/dataset");


		SparkBulkTagJob
			.main(
				new String[] {
					"-isTest", Boolean.TRUE.toString(),
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath", workingDir.toString() + "/input/dataset",
					"-taggingConf", taggingConf,
					"-resultTableName", "eu.dnetlib.dhp.schema.oaf.Dataset",
					"-outputPath", workingDir.toString() + "/dataset",
					"-isLookUpUrl", MOCK_IS_LOOK_UP_URL,
					"-pathMap", pathMap,
					"-datasourceMapPath",
					getClass()
						.getResource("/eu/dnetlib/dhp/bulktag/eosc/datasourceMasterAssociation/datasourceMaster")
						.getPath(),
				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Dataset> tmp = sc
			.textFile(workingDir.toString() + "/input/dataset")
			.map(item -> OBJECT_MAPPER.readValue(item, Dataset.class));

		Assertions.assertEquals(10, tmp.count());

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
						d -> d.getId().equals("50|475c1990cbb2::3894c94123e96df8a21249957cf160cb")
							&&
							d.getContext().stream().anyMatch(c -> c.getId().equals("eosc")))
					.count());
	}

}
