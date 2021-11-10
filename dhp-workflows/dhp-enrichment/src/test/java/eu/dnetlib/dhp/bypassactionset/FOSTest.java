
package eu.dnetlib.dhp.bypassactionset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import eu.dnetlib.dhp.PropagationConstant;
import eu.dnetlib.dhp.bypassactionset.fos.SparkUpdateFOS;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;
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

import eu.dnetlib.dhp.bypassactionset.fos.GetFOSData;
import eu.dnetlib.dhp.bypassactionset.fos.PrepareFOSSparkJob;
import eu.dnetlib.dhp.bypassactionset.model.FOSDataModel;
import eu.dnetlib.dhp.common.collection.CollectorException;
import eu.dnetlib.dhp.countrypropagation.CountryPropagationJobTest;
import eu.dnetlib.dhp.schema.oaf.utils.CleaningFunctions;
import eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory;

public class FOSTest {
	private static final Logger log = LoggerFactory.getLogger(FOSTest.class);

	private static Path workingDir;
	private static SparkSession spark;
	private static LocalFileSystem fs;
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
	private static final String ID_PREFIX = "50|doi_________";

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files.createTempDirectory(CountryPropagationJobTest.class.getSimpleName());

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
	void getFOSFileTest() throws CollectorException, IOException, ClassNotFoundException {

		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/bypassactionset/fos/h2020_fos_sbs.csv")
			.getPath();
		final String outputPath = workingDir.toString() + "/fos.json";

		new GetFOSData()
			.doRewrite(sourcePath, outputPath, "eu.dnetlib.dhp.bypassactionset.FOSDataModel", '\t', fs);

		BufferedReader in = new BufferedReader(
			new InputStreamReader(fs.open(new org.apache.hadoop.fs.Path(outputPath))));

		String line;
		int count = 0;
		while ((line = in.readLine()) != null) {
			FOSDataModel fos = new ObjectMapper().readValue(line, FOSDataModel.class);

			System.out.println(new ObjectMapper().writeValueAsString(fos));
			count += 1;
		}

		assertEquals(38, count);

	}

	@Test
	void distributeDoiTest() throws Exception {
		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/bypassactionset/fos/fos.json")
			.getPath();

		PrepareFOSSparkJob
			.main(
				new String[] {
					"--isSparkSessionManaged", Boolean.FALSE.toString(),
					"--sourcePath", sourcePath,

					"-outputPath", workingDir.toString() + "/distribute"

				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<FOSDataModel> tmp = sc
			.textFile(workingDir.toString() + "/distribute")
			.map(item -> OBJECT_MAPPER.readValue(item, FOSDataModel.class));

		String doi1 = ID_PREFIX +
			IdentifierFactory.md5(CleaningFunctions.normalizePidValue("doi", "10.3390/s18072310"));

		assertEquals(50, tmp.count());
		assertEquals(1, tmp.filter(row -> row.getDoi().equals(doi1)).count());
		assertEquals(
			"engineering and technology", tmp.filter(r -> r.getDoi().equals(doi1)).collect().get(0).getLevel1());
		assertEquals("nano-technology", tmp.filter(r -> r.getDoi().equals(doi1)).collect().get(0).getLevel2());
		assertEquals(
			"nanoscience & nanotechnology", tmp.filter(r -> r.getDoi().equals(doi1)).collect().get(0).getLevel3());

		String doi = ID_PREFIX +
			IdentifierFactory.md5(CleaningFunctions.normalizePidValue("doi", "10.1111/1365-2656.12831"));
		assertEquals(1, tmp.filter(row -> row.getDoi().equals(doi)).count());
		assertEquals("social sciences", tmp.filter(r -> r.getDoi().equals(doi)).collect().get(0).getLevel1());
		assertEquals(
			"psychology and cognitive sciences", tmp.filter(r -> r.getDoi().equals(doi)).collect().get(0).getLevel2());
		assertEquals("NULL", tmp.filter(r -> r.getDoi().equals(doi)).collect().get(0).getLevel3());


	}

	@Test
	void updateResult() throws Exception{

			final String fosPath = getClass()
					.getResource("/eu/dnetlib/dhp/bypassactionset/fos/fos_prepared.json")
					.getPath();

			final String inputPath = getClass()
					.getResource("/eu/dnetlib/dhp/bypassactionset/bip/publicationnomatch.json")
					.getPath();

			SparkUpdateFOS
					.main(
							new String[] {
									"--isSparkSessionManaged", Boolean.FALSE.toString(),
									"--fosPath", fosPath,
									"--inputPath", inputPath,
									"--outputPath", workingDir.toString() + "/publication",
									"--resultTableName", "eu.dnetlib.dhp.schema.oaf.Publication"

							});

			final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

			JavaRDD<Publication> tmp = sc
					.textFile(workingDir.toString() + "/publication")
					.map(item -> OBJECT_MAPPER.readValue(item, Publication.class));

			Assertions.assertEquals(6, tmp.count());

		tmp.filter(r -> r.getSubject() != null).map(p -> p.getSubject())
					.foreach(s -> s.stream().forEach(sbj -> Assertions.assertFalse("FOS".equals(sbj.getQualifier().getClassid()))));


	}

	@Test
	void updateResultMatch() throws Exception{
		final String fosPath = getClass()
				.getResource("/eu/dnetlib/dhp/bypassactionset/fos/fos_prepared.json")
				.getPath();

		final String inputPath = getClass()
				.getResource("/eu/dnetlib/dhp/bypassactionset/fos/publicationmatch.json")
				.getPath();

		SparkUpdateFOS
				.main(
						new String[] {
								"--isSparkSessionManaged", Boolean.FALSE.toString(),
								"--fosPath", fosPath,
								"--inputPath", inputPath,
								"--outputPath", workingDir.toString() + "/publication",
								"--resultTableName", "eu.dnetlib.dhp.schema.oaf.Publication"

						});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Publication> tmp = sc
				.textFile(workingDir.toString() + "/publication")
				.map(item -> OBJECT_MAPPER.readValue(item, Publication.class));

		Assertions.assertEquals(6, tmp.count());

		Assertions.assertEquals(3, tmp.filter(r -> r.getSubject() != null).map(p -> p.getSubject()).flatMap(v -> v.iterator())
				.filter(sbj -> sbj.getQualifier().getClassid().equals("FOS")).collect().size());


		List<StructuredProperty> sbjs = tmp.filter(r -> r.getId().equals("50|doi_________b24ab3e127aa67e2a1017292988d571f"))
				.map(p -> p.getSubject()).collect().get(0);

		Assertions.assertEquals(12, sbjs.size());

		Stream<StructuredProperty> fosSubjs = sbjs.stream().filter(sbj -> sbj.getQualifier().getClassid().equals("FOS"));

		Assertions.assertTrue(fosSubjs
				.map(sbj -> sbj.getValue()).collect(Collectors.toList()).contains("engineering and technology"));
		Assertions.assertTrue(fosSubjs
				.map(sbj -> sbj.getValue()).collect(Collectors.toList()).contains("nano-technology"));
		Assertions.assertTrue(fosSubjs
				.map(sbj -> sbj.getValue()).collect(Collectors.toList()).contains("nanoscience & nanotechnology"));

		fosSubjs.forEach(sbj -> Assertions.assertEquals("update", sbj.getDataInfo().getInferenceprovenance()) );
		fosSubjs.forEach(sbj -> Assertions.assertEquals("subject:fos", sbj.getDataInfo().getProvenanceaction().getClassid()) );
		fosSubjs.forEach(sbj -> Assertions.assertEquals("Inferred by OpenAIRE", sbj.getDataInfo().getProvenanceaction().getClassname() ));
		fosSubjs.forEach(sbj -> Assertions.assertEquals("", sbj.getDataInfo().getTrust() ));
		fosSubjs.forEach(sbj -> Assertions.assertEquals(false, sbj.getDataInfo().getDeletedbyinference() ));
		fosSubjs.forEach(sbj -> Assertions.assertEquals(false, sbj.getDataInfo().getInvisible() ));
		fosSubjs.forEach(sbj -> Assertions.assertEquals(true, sbj.getDataInfo().getInferred() ));



	}



}
