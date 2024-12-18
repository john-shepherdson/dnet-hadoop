
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
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory;
import eu.dnetlib.dhp.schema.oaf.utils.PidCleaner;
import eu.dnetlib.dhp.schema.oaf.utils.PidType;

/**
 * @author miriam.baglioni
 * @Date 22/04/24
 */
public class RemoveFromASTest {
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SparkSession spark;

	private static Path workingDir;
	private static final Logger log = LoggerFactory
		.getLogger(RemoveFromASTest.class);

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files
			.createTempDirectory(RemoveFromASTest.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(RemoveFromASTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(RemoveFromASTest.class.getSimpleName())
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
				"/eu/dnetlib/dhp/actionmanager/webcrawl/actionSet/")
			.getPath();
		String blackListPath = getClass()
			.getResource(
				"/eu/dnetlib/dhp/actionmanager/webcrawl/blackListRemove/")
			.getPath();

		RemoveRelationFromActionSet
			.main(
				new String[] {
					"-isSparkSessionManaged",
					Boolean.FALSE.toString(),
					"-sourcePath",
					inputPath,
					"-outputPath",
					workingDir.toString() + "/actionSet1",
					"-blackListPath", blackListPath
				});

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaRDD<Relation> tmp = sc
			.sequenceFile(workingDir.toString() + "/actionSet1", Text.class, Text.class)
			.map(value -> OBJECT_MAPPER.readValue(value._2().toString(), AtomicAction.class))
			.map(aa -> ((Relation) aa.getPayload()));

		Assertions.assertEquals(22, tmp.count());

	}

}
