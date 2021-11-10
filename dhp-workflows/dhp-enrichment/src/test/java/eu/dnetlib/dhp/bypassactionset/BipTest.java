
package eu.dnetlib.dhp.bypassactionset;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import eu.dnetlib.dhp.schema.oaf.Author;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.neethi.Assertion;
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

import eu.dnetlib.dhp.bypassactionset.bipfinder.PrepareBipFinder;
import eu.dnetlib.dhp.bypassactionset.bipfinder.SparkUpdateBip;
import eu.dnetlib.dhp.bypassactionset.model.BipScore;
import eu.dnetlib.dhp.countrypropagation.CountryPropagationJobTest;
import eu.dnetlib.dhp.schema.oaf.Measure;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.utils.CleaningFunctions;
import eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory;

public class BipTest {

	private static final Logger log = LoggerFactory.getLogger(FOSTest.class);

	private static Path workingDir;
	private static SparkSession spark;
	private static LocalFileSystem fs;
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
	private static final String ID_PREFIX = "50|doi_________";

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files.createTempDirectory(BipTest.class.getSimpleName());

		fs = FileSystem.getLocal(new Configuration());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(FOSTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(CountryPropagationJobTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	@Test
	void prepareBipTest() throws Exception {
		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/bypassactionset/bip/bip.json")
			.getPath();

		PrepareBipFinder
			.main(
				new String[] {
					"--isSparkSessionManaged", Boolean.FALSE.toString(),
					"--inputPath", sourcePath,
					"--outputPath", workingDir.toString() + "/remapDoi"

				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<BipScore> tmp = sc
			.textFile(workingDir.toString() + "/remapDoi")
			.map(item -> OBJECT_MAPPER.readValue(item, BipScore.class));

		Assertions.assertEquals(86, tmp.count());
//        tmp.foreach(v -> System.out.println(OBJECT_MAPPER.writeValueAsString(v)));

		String doi1 = ID_PREFIX +
			IdentifierFactory.md5(CleaningFunctions.normalizePidValue("doi", "10.0000/096020199389707"));

		Assertions.assertEquals(1, tmp.filter(r -> r.getId().equals(doi1)).count());
		Assertions.assertEquals(3, tmp.filter(r -> r.getId().equals(doi1)).collect().get(0).getScoreList().size());
		Assertions
			.assertEquals(
				"6.34596412687e-09", tmp
					.filter(r -> r.getId().equals(doi1))
					.collect()
					.get(0)
					.getScoreList()
					.stream()
					.filter(sl -> sl.getId().equals("influence"))
					.collect(Collectors.toList())
					.get(0)
					.getUnit()
					.get(0)
					.getValue());
		Assertions
			.assertEquals(
				"0.641151896994", tmp
					.filter(r -> r.getId().equals(doi1))
					.collect()
					.get(0)
					.getScoreList()
					.stream()
					.filter(sl -> sl.getId().equals("popularity_alt"))
					.collect(Collectors.toList())
					.get(0)
					.getUnit()
					.get(0)
					.getValue());
		Assertions
			.assertEquals(
				"2.33375102921e-09", tmp
					.filter(r -> r.getId().equals(doi1))
					.collect()
					.get(0)
					.getScoreList()
					.stream()
					.filter(sl -> sl.getId().equals("popularity"))
					.collect(Collectors.toList())
					.get(0)
					.getUnit()
					.get(0)
					.getValue());

	}

	@Test
	void updateResult() throws Exception {
		final String bipScorePath = getClass()
			.getResource("/eu/dnetlib/dhp/bypassactionset/bip/preparedbip.json")
			.getPath();

		final String inputPath = getClass()
			.getResource("/eu/dnetlib/dhp/bypassactionset/bip/publicationnomatch.json")
			.getPath();

		SparkUpdateBip
			.main(
				new String[] {
					"--isSparkSessionManaged", Boolean.FALSE.toString(),
					"--bipScorePath", bipScorePath,
					"--inputPath", inputPath,
					"--outputPath", workingDir.toString() + "/publication",
					"--resultTableName", "eu.dnetlib.dhp.schema.oaf.Publication"

				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Publication> tmp = sc
			.textFile(workingDir.toString() + "/publication/bip")
			.map(item -> OBJECT_MAPPER.readValue(item, Publication.class));

		Assertions.assertEquals(6, tmp.count());
		Assertions.assertEquals(0, tmp.filter(r -> r.getMeasures() != null).count());
		tmp.foreach(r -> Assertions.assertEquals("publication", r.getResulttype().getClassid()));

	}

	@Test
	void updateResultMatchCheckMeasures() throws Exception {
		final String bipScorePath = getClass()
			.getResource("/eu/dnetlib/dhp/bypassactionset/bip/preparedbip.json")
			.getPath();

		final String inputPath = getClass()
			.getResource("/eu/dnetlib/dhp/bypassactionset/bip/publicationmatch.json")
			.getPath();

		SparkUpdateBip
			.main(
				new String[] {
					"--isSparkSessionManaged", Boolean.FALSE.toString(),
					"--bipScorePath", bipScorePath,
					"--inputPath", inputPath,
					"--outputPath", workingDir.toString() + "/publication",
					"--resultTableName", "eu.dnetlib.dhp.schema.oaf.Publication"

				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Publication> tmp = sc
			.textFile(workingDir.toString() + "/publication/bip")
			.map(item -> OBJECT_MAPPER.readValue(item, Publication.class));

		Assertions.assertEquals(6, tmp.count());
		Assertions.assertEquals(1, tmp.filter(r -> r.getMeasures() != null).count());
		Assertions
			.assertEquals(
				1, tmp.filter(r -> r.getId().equals("50|doi_________b24ab3e127aa67e2a1017292988d571f")).count());
		Assertions
			.assertEquals(
				1,
				tmp
					.filter(
						r -> r.getId().equals("50|doi_________b24ab3e127aa67e2a1017292988d571f")
							&& r.getMeasures() != null)
					.count());
		Assertions.assertEquals(3, tmp
			.filter(r -> r.getId().equals("50|doi_________b24ab3e127aa67e2a1017292988d571f"))
			.collect()
			.get(0)
			.getMeasures().size());

        Assertions.assertEquals("5.91019644836e-09",
                tmp.filter(r -> r.getId().equals("50|doi_________b24ab3e127aa67e2a1017292988d571f"))
                        .collect()
                .get(0).getMeasures().stream().filter(m -> m.getId().equals("influence")).collect(Collectors.toList()).get(0).getUnit().get(0).getValue());
		Assertions.assertEquals("0.0",
				tmp.filter(r -> r.getId().equals("50|doi_________b24ab3e127aa67e2a1017292988d571f"))
						.collect()
						.get(0).getMeasures().stream().filter(m -> m.getId().equals("popularity_alt")).collect(Collectors.toList()).get(0).getUnit().get(0).getValue());
		Assertions.assertEquals("9.88840807598e-09",
				tmp.filter(r -> r.getId().equals("50|doi_________b24ab3e127aa67e2a1017292988d571f"))
						.collect()
						.get(0).getMeasures().stream().filter(m -> m.getId().equals("popularity")).collect(Collectors.toList()).get(0).getUnit().get(0).getValue());

		tmp.foreach(r -> System.out.println(OBJECT_MAPPER.writeValueAsString(r)));

	}



}
