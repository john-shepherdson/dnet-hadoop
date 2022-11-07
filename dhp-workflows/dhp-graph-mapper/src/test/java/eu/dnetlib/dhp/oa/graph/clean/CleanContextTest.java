
package eu.dnetlib.dhp.oa.graph.clean;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;

public class CleanContextTest {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SparkSession spark;

	private static Path workingDir;

	private static final Logger log = LoggerFactory.getLogger(CleanContextTest.class);

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files.createTempDirectory(CleanContextTest.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(CleanContextTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(CleanContextTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	@Test
	public void testResultClean() throws Exception {
		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/clean/publication_clean_context.json")
			.getPath();
		final String prefix = "gcube ";

		spark
			.read()
			.textFile(sourcePath)
			.map(
				(MapFunction<String, Publication>) r -> OBJECT_MAPPER.readValue(r, Publication.class),
				Encoders.bean(Publication.class))
			.write()
			.json(workingDir.toString() + "/publication");

		CleanContextSparkJob.main(new String[] {
			"--isSparkSessionManaged", Boolean.FALSE.toString(),
			"--inputPath", workingDir.toString() + "/publication",
			"-graphTableClassName", Publication.class.getCanonicalName(),
			"-workingPath", workingDir.toString() + "/working",
			"-contextId", "sobigdata",
			"-verifyParam", "gCube "
		});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
		JavaRDD<Publication> tmp = sc
			.textFile(workingDir.toString() + "/publication")
			.map(item -> OBJECT_MAPPER.readValue(item, Publication.class));

		Assertions.assertEquals(7, tmp.count());

		// original result with sobigdata context and gcube as starting string in the main title for the publication
		Assertions
			.assertEquals(
				0,
				tmp
					.filter(p -> p.getId().equals("50|DansKnawCris::0224aae28af558f21768dbc6439c7a95"))
					.collect()
					.get(0)
					.getContext()
					.size());

		// original result with sobigdata context without gcube as starting string in the main title for the publication
		Assertions
			.assertEquals(
				1,
				tmp
					.filter(p -> p.getId().equals("50|DansKnawCris::20c414a3b1c742d5dd3851f1b67df2d9"))
					.collect()
					.get(0)
					.getContext()
					.size());
		Assertions
			.assertEquals(
				"sobigdata::projects::2",
				tmp
					.filter(p -> p.getId().equals("50|DansKnawCris::20c414a3b1c742d5dd3851f1b67df2d9"))
					.collect()
					.get(0)
					.getContext()
					.get(0)
					.getId());

		// original result with sobigdata context with gcube as starting string in the subtitle
		Assertions
			.assertEquals(
				1,
				tmp
					.filter(p -> p.getId().equals("50|DansKnawCris::3c81248c335f0aa07e06817ece6fa6af"))
					.collect()
					.get(0)
					.getContext()
					.size());
		Assertions
			.assertEquals(
				"sobigdata::projects::2",
				tmp
					.filter(p -> p.getId().equals("50|DansKnawCris::3c81248c335f0aa07e06817ece6fa6af"))
					.collect()
					.get(0)
					.getContext()
					.get(0)
					.getId());
		List<StructuredProperty> titles = tmp
			.filter(p -> p.getId().equals("50|DansKnawCris::3c81248c335f0aa07e06817ece6fa6af"))
			.collect()
			.get(0)
			.getTitle();
		Assertions.assertEquals(1, titles.size());
		Assertions.assertTrue(titles.get(0).getValue().toLowerCase().startsWith(prefix));
		Assertions.assertEquals("subtitle", titles.get(0).getQualifier().getClassid());

		// original result with sobigdata context with gcube not as starting string in the main title
		Assertions
			.assertEquals(
				1,
				tmp
					.filter(p -> p.getId().equals("50|DansKnawCris::3c9f068ddc930360bec6925488a9a97f"))
					.collect()
					.get(0)
					.getContext()
					.size());
		Assertions
			.assertEquals(
				"sobigdata::projects::1",
				tmp
					.filter(p -> p.getId().equals("50|DansKnawCris::3c9f068ddc930360bec6925488a9a97f"))
					.collect()
					.get(0)
					.getContext()
					.get(0)
					.getId());
		titles = tmp
			.filter(p -> p.getId().equals("50|DansKnawCris::3c9f068ddc930360bec6925488a9a97f"))
			.collect()
			.get(0)
			.getTitle();
		Assertions.assertEquals(1, titles.size());
		Assertions.assertFalse(titles.get(0).getValue().toLowerCase().startsWith(prefix));
		Assertions.assertTrue(titles.get(0).getValue().toLowerCase().contains(prefix.trim()));
		Assertions.assertEquals("main title", titles.get(0).getQualifier().getClassid());

		// original result with sobigdata in context and also other contexts with gcube as starting string for the main
		// title
		Assertions
			.assertEquals(
				1,
				tmp
					.filter(p -> p.getId().equals("50|DansKnawCris::4669a378a73661417182c208e6fdab53"))
					.collect()
					.get(0)
					.getContext()
					.size());
		Assertions
			.assertEquals(
				"dh-ch",
				tmp
					.filter(p -> p.getId().equals("50|DansKnawCris::4669a378a73661417182c208e6fdab53"))
					.collect()
					.get(0)
					.getContext()
					.get(0)
					.getId());
		titles = tmp
			.filter(p -> p.getId().equals("50|DansKnawCris::4669a378a73661417182c208e6fdab53"))
			.collect()
			.get(0)
			.getTitle();
		Assertions.assertEquals(1, titles.size());
		Assertions.assertTrue(titles.get(0).getValue().toLowerCase().startsWith(prefix));
		Assertions.assertEquals("main title", titles.get(0).getQualifier().getClassid());

		// original result with multiple main title one of which whith gcube as starting string and with 2 contextes
		Assertions
			.assertEquals(
				1,
				tmp
					.filter(p -> p.getId().equals("50|DansKnawCris::4a9152e80f860eab99072e921d74a0ff"))
					.collect()
					.get(0)
					.getContext()
					.size());
		Assertions
			.assertEquals(
				"dh-ch",
				tmp
					.filter(p -> p.getId().equals("50|DansKnawCris::4a9152e80f860eab99072e921d74a0ff"))
					.collect()
					.get(0)
					.getContext()
					.get(0)
					.getId());
		titles = tmp
			.filter(p -> p.getId().equals("50|DansKnawCris::4a9152e80f860eab99072e921d74a0ff"))
			.collect()
			.get(0)
			.getTitle();
		Assertions.assertEquals(2, titles.size());
		Assertions
			.assertTrue(
				titles
					.stream()
					.anyMatch(
						t -> t.getQualifier().getClassid().equals("main title")
							&& t.getValue().toLowerCase().startsWith(prefix)));

		// original result without sobigdata in context with gcube as starting string for the main title
		Assertions
			.assertEquals(
				1,
				tmp
					.filter(p -> p.getId().equals("50|dedup_wf_001::01e6a28565ca01376b7548e530c6f6e8"))
					.collect()
					.get(0)
					.getContext()
					.size());
		Assertions
			.assertEquals(
				"dh-ch",
				tmp
					.filter(p -> p.getId().equals("50|dedup_wf_001::01e6a28565ca01376b7548e530c6f6e8"))
					.collect()
					.get(0)
					.getContext()
					.get(0)
					.getId());
		titles = tmp
			.filter(p -> p.getId().equals("50|dedup_wf_001::01e6a28565ca01376b7548e530c6f6e8"))
			.collect()
			.get(0)
			.getTitle();
		Assertions.assertEquals(2, titles.size());

		Assertions
			.assertTrue(
				titles
					.stream()
					.anyMatch(
						t -> t.getQualifier().getClassid().equals("main title")
							&& t.getValue().toLowerCase().startsWith(prefix)));

	}
}
