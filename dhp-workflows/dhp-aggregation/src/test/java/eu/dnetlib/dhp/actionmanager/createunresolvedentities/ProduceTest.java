
package eu.dnetlib.dhp.actionmanager.createunresolvedentities;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
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

import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.*;

public class ProduceTest {
	private static final Logger log = LoggerFactory.getLogger(ProduceTest.class);

	private static Path workingDir;
	private static SparkSession spark;
	private static LocalFileSystem fs;
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
	private static final String ID_PREFIX = "50|doi_________";

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files.createTempDirectory(ProduceTest.class.getSimpleName());

		fs = FileSystem.getLocal(new Configuration());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(ProduceTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(ProduceTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	@Test
	void produceTest() throws Exception {

		final String bipPath = getClass()
			.getResource("/eu/dnetlib/dhp/actionmanager/createunresolvedentities/bip/bip.json")
			.getPath();

		PrepareBipFinder
			.main(
				new String[] {
					"--isSparkSessionManaged", Boolean.FALSE.toString(),
					"--sourcePath", bipPath,
					"--outputPath", workingDir.toString() + "/work"

				});
		final String fosPath = getClass()
			.getResource("/eu/dnetlib/dhp/actionmanager/createunresolvedentities/fos/fos.json")
			.getPath();

		PrepareFOSSparkJob
			.main(
				new String[] {
					"--isSparkSessionManaged", Boolean.FALSE.toString(),
					"--sourcePath", fosPath,
					"-outputPath", workingDir.toString() + "/work"
				});

		SparkSaveUnresolved.main(new String[] {
			"--isSparkSessionManaged", Boolean.FALSE.toString(),
			"--sourcePath", workingDir.toString() + "/work",

			"-outputPath", workingDir.toString() + "/unresolved"

		});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Result> tmp = sc
			.textFile(workingDir.toString() + "/unresolved")
			.map(item -> OBJECT_MAPPER.readValue(item, Result.class));

		Assertions.assertEquals(135, tmp.count());

		Assertions.assertEquals(1, tmp.filter(row -> row.getId().equals("unresolved::10.3390/s18072310::doi")).count());

		Assertions
			.assertEquals(
				3, tmp
					.filter(row -> row.getId().equals("unresolved::10.3390/s18072310::doi"))
					.collect()
					.get(0)
					.getSubject()
					.size());

		Assertions
			.assertEquals(
				3, tmp
					.filter(row -> row.getId().equals("unresolved::10.3390/s18072310::doi"))
					.collect()
					.get(0)
							.getInstance()
							.get(0)
					.getMeasures()
					.size());

		List<StructuredProperty> sbjs = tmp
			.filter(row -> row.getId().equals("unresolved::10.3390/s18072310::doi"))
			.flatMap(row -> row.getSubject().iterator())
			.collect();

		sbjs.forEach(sbj -> Assertions.assertEquals("FOS", sbj.getQualifier().getClassid()));
		sbjs
			.forEach(
				sbj -> Assertions
					.assertEquals(
						"Fields of Science and Technology classification", sbj.getQualifier().getClassname()));
		sbjs
			.forEach(
				sbj -> Assertions
					.assertEquals(ModelConstants.DNET_SUBJECT_TYPOLOGIES, sbj.getQualifier().getSchemeid()));
		sbjs
			.forEach(
				sbj -> Assertions
					.assertEquals(ModelConstants.DNET_SUBJECT_TYPOLOGIES, sbj.getQualifier().getSchemename()));

		sbjs.forEach(sbj -> Assertions.assertEquals(false, sbj.getDataInfo().getDeletedbyinference()));
		sbjs.forEach(sbj -> Assertions.assertEquals(true, sbj.getDataInfo().getInferred()));
		sbjs.forEach(sbj -> Assertions.assertEquals(false, sbj.getDataInfo().getInvisible()));
		sbjs.forEach(sbj -> Assertions.assertEquals("", sbj.getDataInfo().getTrust()));
		sbjs.forEach(sbj -> Assertions.assertEquals("update", sbj.getDataInfo().getInferenceprovenance()));
		sbjs
			.forEach(
				sbj -> Assertions.assertEquals("subject:fos", sbj.getDataInfo().getProvenanceaction().getClassid()));
		sbjs
			.forEach(
				sbj -> Assertions
					.assertEquals("Inferred by OpenAIRE", sbj.getDataInfo().getProvenanceaction().getClassname()));
		sbjs
			.forEach(
				sbj -> Assertions
					.assertEquals(
						ModelConstants.DNET_PROVENANCE_ACTIONS, sbj.getDataInfo().getProvenanceaction().getSchemeid()));
		sbjs
			.forEach(
				sbj -> Assertions
					.assertEquals(
						ModelConstants.DNET_PROVENANCE_ACTIONS,
						sbj.getDataInfo().getProvenanceaction().getSchemename()));

		sbjs.stream().anyMatch(sbj -> sbj.getValue().equals("engineering and technology"));
		sbjs.stream().anyMatch(sbj -> sbj.getValue().equals("nano-technology"));
		sbjs.stream().anyMatch(sbj -> sbj.getValue().equals("nanoscience & nanotechnology"));

		List<Measure> measures = tmp
			.filter(row -> row.getId().equals("unresolved::10.3390/s18072310::doi"))
			.flatMap(row -> row.getInstance().iterator())
				.flatMap(inst -> inst.getMeasures().iterator())
			.collect();
		Assertions
			.assertEquals(
				"7.5597134689e-09", measures
					.stream()
					.filter(mes -> mes.getId().equals("influence"))
					.collect(Collectors.toList())
					.get(0)
					.getUnit()
					.get(0)
					.getValue());

		Assertions
			.assertEquals(
				"4.903880192", measures
					.stream()
					.filter(mes -> mes.getId().equals("popularity_alt"))
					.collect(Collectors.toList())
					.get(0)
					.getUnit()
					.get(0)
					.getValue());

		Assertions
			.assertEquals(
				"1.17977512835e-08", measures
					.stream()
					.filter(mes -> mes.getId().equals("popularity"))
					.collect(Collectors.toList())
					.get(0)
					.getUnit()
					.get(0)
					.getValue());

		Assertions
			.assertEquals(
				49, tmp
					.filter(row -> !row.getId().equals("unresolved::10.3390/s18072310::doi"))
					.filter(row -> row.getSubject() != null)
					.count());

		Assertions
			.assertEquals(
				85,
				tmp
					.filter(row -> !row.getId().equals("unresolved::10.3390/s18072310::doi"))
					.filter(r -> r.getMeasures() != null)
					.count());

	}

}
